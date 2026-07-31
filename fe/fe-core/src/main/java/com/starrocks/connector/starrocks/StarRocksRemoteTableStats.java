// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.starrocks;

import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * Wire DTOs shared by the remote FE catalog statistics HTTP endpoints
 * ({@code StarRocksCatalogStatsAction}, shipped with the control-plane server
 * PR) and the local StarRocks catalog statistics client. Serialized with Gson
 * on both sides.
 *
 * <p>All partition ids are LOGICAL {@code Partition} ids: the remote
 * {@code _statistics_} rows are keyed by logical partition id and the collect
 * SQL already aggregates across physical sub-partitions, so physical
 * partitions never cross the wire.
 *
 * <p>Epochs are opaque tokens minted by the remote from persisted metadata;
 * the local side only compares them for equality, as the fast changed /
 * not-changed check of the timed conditional pull.
 */
public class StarRocksRemoteTableStats {

    public static final String PARTITION_TYPE_RANGE = "RANGE";
    public static final String PARTITION_TYPE_LIST = "LIST";
    public static final String PARTITION_TYPE_UNPARTITIONED = "UNPARTITIONED";
    // Partitioning the local side cannot prune (e.g. partition columns not
    // resolvable in the base schema). Row counts are still usable.
    public static final String PARTITION_TYPE_UNSUPPORTED = "UNSUPPORTED";

    public static final String ANALYZE_TYPE_NONE = "NONE";

    /**
     * Change-detection tokens for the epoch-gated conditional fetch. The remote mints
     * them from its persisted metadata under the table read lock; the local side
     * treats them as OPAQUE — it stores the last seen values and sends them back on
     * the next fetch, and the remote compares them for equality only, to decide
     * which fields to re-ship: a moved {@code list} or {@code data} epoch re-ships
     * the row-count-bearing fields ({@code partitions} + {@code tableRowCount}), a
     * moved {@code analyze} epoch re-ships the collected column stats. Unchanged
     * fields are omitted from the response and merged back from the local cache, so
     * the periodic background refresh costs almost nothing while the table is quiet.
     */
    public static class Epochs {
        /**
         * Partition-SET token: murmur3_128 over the sorted logical partition id
         * list. Moves exactly when partitions are added / dropped / replaced.
         */
        @SerializedName("list")
        public String list;
        /**
         * Data-version token: the sum of every physical partition's visible
         * version. Any load / compaction publish bumps a visible version, so the
         * sum moves on any data change; partition-set changes are already covered
         * by {@link #list}.
         */
        @SerializedName("data")
        public String data;
        /**
         * Collected-stats token: {@code "<updateTimeMillis>:<analyzeType>"} of the
         * table's BasicStatsMeta, or {@code "0:NONE"} when never analyzed. Moves
         * when a new ANALYZE lands or its type (FULL/SAMPLE) changes.
         */
        @SerializedName("analyze")
        public String analyze;

        public Epochs() {
        }

        public Epochs(String list, String data, String analyze) {
            this.list = list;
            this.data = data;
            this.analyze = analyze;
        }
    }

    /** One bound of a RANGE partition. */
    public static class RangeBound {
        @SerializedName("infinite_min")
        public boolean infiniteMin;
        @SerializedName("infinite_max")
        public boolean infiniteMax;
        // Per-partition-column literal strings; an element may be the literal
        // "MAXVALUE" sentinel for partially-unbounded multi-column ranges.
        @SerializedName("values")
        public List<String> values;
    }

    public static class PartitionMeta {
        @SerializedName("id")
        public long id;
        @SerializedName("name")
        public String name;
        // Physical row count of the logical partition (sum over sub-partitions,
        // TabletStatMgr-fed; ~tablet_stat_update_interval_second staleness).
        @SerializedName("row_count")
        public long rowCount;
        // LIST partitioning: value tuples, one inner list per IN-value, each
        // inner list has one element per partition column. A null element
        // denotes a NULL partition value.
        @SerializedName("list_values")
        public List<List<String>> listValues;
        @SerializedName("range_lower")
        public RangeBound rangeLower;
        @SerializedName("range_upper")
        public RangeBound rangeUpper;
    }

    /**
     * Raw collected statistics for one column, aggregated table-wide on the
     * remote (same shape as TStatisticData). min/max stay engine-native strings
     * and are converted locally with the same code path the native optimizer
     * uses, so the conversion semantics cannot drift.
     */
    public static class ColumnStats {
        @SerializedName("column")
        public String column;
        @SerializedName("row_count")
        public long rowCount;
        @SerializedName("data_size")
        public double dataSize;
        @SerializedName("ndv")
        public long ndv;
        @SerializedName("null_count")
        public long nullCount;
        @SerializedName("max")
        public String max;
        @SerializedName("min")
        public String min;
        @SerializedName("collection_size")
        public long collectionSize = -1;
        // Raw histogram JSON ({"buckets": ..., "mcv": ...}) passed through from
        // the remote _statistics_ histogram table; null when absent.
        @SerializedName("histogram")
        public String histogram;
    }

    /** Endpoint A response: epoch-gated conditional table stats snapshot. */
    public static class Snapshot {
        @SerializedName("status")
        public int status;
        @SerializedName("exception")
        public String exception;
        @SerializedName("epochs")
        public Epochs epochs;

        // Immutable partitioning shape: fixed at CREATE TABLE, always shipped.
        @SerializedName("partition_type")
        public String partitionType;
        @SerializedName("partition_columns")
        public List<String> partitionColumns;

        // Row-count-bearing physical state. partitions embed per-partition row
        // counts, so a moved list (partition set) OR data (row versions) epoch
        // re-ships both fields; the client detects that by re-comparing the
        // epochs (StarRocksMetadataCache.mergeSnapshot).
        @SerializedName("table_row_count")
        public long tableRowCount;
        @SerializedName("partitions")
        public List<PartitionMeta> partitions;

        // ANALYZE-collected column statistics: re-shipped when the analyze epoch
        // moved; detected the same way.
        @SerializedName("analyze_type")
        public String analyzeType;
        @SerializedName("column_stats")
        public List<ColumnStats> columnStats;
    }

    /** Endpoint B request body. */
    public static class PartitionStatsRequest {
        @SerializedName("partition_ids")
        public List<Long> partitionIds;
        @SerializedName("columns")
        public List<String> columns;
    }

    /** Per-(partition, column) collected statistics from the remote _statistics_. */
    public static class PartitionColumnStats {
        @SerializedName("partition_id")
        public long partitionId;
        @SerializedName("column")
        public String column;
        @SerializedName("ndv")
        public long ndv;
        @SerializedName("null_count")
        public long nullCount;
        @SerializedName("row_count")
        public long rowCount;
    }

    /** Endpoint B response. */
    public static class PartitionStatsResponse {
        @SerializedName("status")
        public int status;
        @SerializedName("exception")
        public String exception;
        @SerializedName("partition_stats")
        public List<PartitionColumnStats> partitionStats;
    }

    private StarRocksRemoteTableStats() {
    }
}
