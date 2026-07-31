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

import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.optimizer.statistics.ColumnBasicStatsCacheLoader;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.HistogramUtils;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

/**
 * Interpretation helpers over the statistics snapshot wire DTOs. Both the
 * partition pruner branch and the connector statistics path construct
 * PartitionKeys through this class from the same pinned snapshot, so key
 * equality round-trips between pruning and statistics derivation.
 */
public class StarRocksStatsUtils {
    private static final Logger LOG = LogManager.getLogger(StarRocksStatsUtils.class);
    private static final String MAX_VALUE_SENTINEL = "MAXVALUE";

    private StarRocksStatsUtils() {
    }

    public static boolean isPruneSupported(StarRocksRemoteTableStats.Snapshot snapshot) {
        return snapshot != null && snapshot.partitions != null
                && (StarRocksRemoteTableStats.PARTITION_TYPE_RANGE.equals(snapshot.partitionType)
                || StarRocksRemoteTableStats.PARTITION_TYPE_LIST.equals(snapshot.partitionType));
    }

    /** RANGE partitioning: logical partition id → [lower, upper) key range. */
    public static Map<Long, Range<PartitionKey>> buildRangeMap(StarRocksRemoteTableStats.Snapshot snapshot,
                                                               List<Column> partitionColumns) {
        Map<Long, Range<PartitionKey>> result = new HashMap<>();
        if (snapshot.partitions == null) {
            return result;
        }
        for (StarRocksRemoteTableStats.PartitionMeta partition : snapshot.partitions) {
            try {
                PartitionKey lower = buildBoundKey(partition.rangeLower, partitionColumns);
                PartitionKey upper = buildBoundKey(partition.rangeUpper, partitionColumns);
                if (lower == null || upper == null) {
                    continue;
                }
                result.put(partition.id, Range.closedOpen(lower, upper));
            } catch (Exception e) {
                LOG.warn("failed to rebuild range bound for partition {}", partition.name, e);
            }
        }
        return result;
    }

    private static PartitionKey buildBoundKey(StarRocksRemoteTableStats.RangeBound bound,
                                              List<Column> partitionColumns) throws Exception {
        if (bound == null) {
            return null;
        }
        if (bound.infiniteMin) {
            return PartitionKey.createInfinityPartitionKey(partitionColumns, false);
        }
        if (bound.infiniteMax) {
            return PartitionKey.createInfinityPartitionKey(partitionColumns, true);
        }
        if (bound.values == null) {
            return null;
        }
        List<PartitionValue> values = new ArrayList<>(bound.values.size());
        for (String value : bound.values) {
            values.add(MAX_VALUE_SENTINEL.equals(value) ? PartitionValue.MAX_VALUE : new PartitionValue(value));
        }
        return PartitionKey.createPartitionKey(values, partitionColumns);
    }

    /**
     * LIST partitioning: logical partition id → value tuples parsed to
     * literals. A null tuple element denotes a NULL partition value.
     */
    public static Map<Long, List<List<LiteralExpr>>> parseListTuples(StarRocksRemoteTableStats.Snapshot snapshot,
                                                                     List<Column> partitionColumns) {
        Map<Long, List<List<LiteralExpr>>> result = new HashMap<>();
        if (snapshot.partitions == null) {
            return result;
        }
        for (StarRocksRemoteTableStats.PartitionMeta partition : snapshot.partitions) {
            if (partition.listValues == null) {
                continue;
            }
            List<List<LiteralExpr>> tuples = new ArrayList<>();
            try {
                for (List<String> tuple : partition.listValues) {
                    List<LiteralExpr> literals = new ArrayList<>(tuple.size());
                    for (int i = 0; i < tuple.size(); i++) {
                        String value = tuple.get(i);
                        literals.add(value == null ? null :
                                LiteralExprFactory.create(value, partitionColumns.get(i).getType()));
                    }
                    tuples.add(literals);
                }
                result.put(partition.id, tuples);
            } catch (Exception e) {
                LOG.warn("failed to parse list partition values for partition {}", partition.name, e);
            }
        }
        return result;
    }

    /**
     * Canonical PartitionKey per partition, used to round-trip ids through
     * ScanOperatorPredicates (the pruner fills idToPartitionKey from this, the
     * statistics path maps the selected keys back to ids with the same
     * construction). RANGE uses the lower bound; LIST uses the first value
     * tuple with NULLs replaced by a deterministic placeholder.
     */
    public static Map<Long, PartitionKey> buildCanonicalKeys(StarRocksRemoteTableStats.Snapshot snapshot,
                                                             List<Column> partitionColumns) {
        Map<Long, PartitionKey> result = new HashMap<>();
        if (snapshot.partitions == null) {
            return result;
        }
        boolean isRange = StarRocksRemoteTableStats.PARTITION_TYPE_RANGE.equals(snapshot.partitionType);
        for (StarRocksRemoteTableStats.PartitionMeta partition : snapshot.partitions) {
            try {
                PartitionKey key;
                if (isRange) {
                    key = buildBoundKey(partition.rangeLower, partitionColumns);
                } else {
                    key = buildListCanonicalKey(partition, partitionColumns);
                }
                if (key != null) {
                    result.put(partition.id, key);
                }
            } catch (Exception e) {
                LOG.warn("failed to build canonical partition key for partition {}", partition.name, e);
            }
        }
        return result;
    }

    private static PartitionKey buildListCanonicalKey(StarRocksRemoteTableStats.PartitionMeta partition,
                                                      List<Column> partitionColumns) throws Exception {
        if (partition.listValues == null || partition.listValues.isEmpty()) {
            return null;
        }
        List<String> firstTuple = partition.listValues.get(0);
        // NULL partition values become NullLiterals — the engine's own list-partition
        // NULL representation. Both the pruner and the stats path build through this
        // method, so equality round-trips; a string placeholder would risk colliding
        // with a real value (e.g. "") and silently drop a partition id from the
        // canonical-key map, losing its rows from the estimate.
        PartitionKey key = new PartitionKey();
        for (int i = 0; i < firstTuple.size(); i++) {
            String value = firstTuple.get(i);
            Type columnType = partitionColumns.get(i).getType();
            LiteralExpr literal = value == null ? NullLiteral.create(columnType)
                    : LiteralExprFactory.create(value, columnType);
            key.pushColumn(literal, columnType.getPrimitiveType());
        }
        return key;
    }

    /**
     * Converts a wire column stats row to the optimizer's ColumnStatistic via
     * the exact code path native statistics use (TStatisticData →
     * buildColumnStatistics), so min/max/date conversion semantics cannot
     * drift, then attaches the histogram when present.
     */
    public static ColumnStatistic toColumnStatistic(StarRocksRemoteTableStats.ColumnStats stats,
                                                    String dbName, String tableName, Type columnType) {
        try {
            TStatisticData data = new TStatisticData();
            data.setColumnName(stats.column);
            data.setRowCount(stats.rowCount);
            data.setDataSize(stats.dataSize);
            data.setCountDistinct(stats.ndv);
            data.setNullCount(stats.nullCount);
            data.setMax(stats.max == null ? "" : stats.max);
            data.setMin(stats.min == null ? "" : stats.min);
            if (stats.collectionSize > 0) {
                data.setCollectionSize(stats.collectionSize);
            }
            ColumnStatistic columnStatistic = ColumnBasicStatsCacheLoader.buildColumnStatistics(
                    data, "default_catalog", dbName, tableName, stats.column, columnType);
            if (stats.histogram != null) {
                try {
                    Histogram histogram = new Histogram(HistogramUtils.convertBuckets(stats.histogram, columnType),
                            HistogramUtils.convertMCV(stats.histogram));
                    columnStatistic = ColumnStatistic.buildFrom(columnStatistic).setHistogram(histogram).build();
                } catch (Exception e) {
                    LOG.warn("failed to parse histogram for column {}", stats.column, e);
                }
            }
            return columnStatistic;
        } catch (Exception e) {
            LOG.warn("failed to convert column statistics for column {}", stats.column, e);
            return ColumnStatistic.unknown();
        }
    }

    /**
     * Statistic-domain double of a partition bound/value string for the given
     * column type, converted through the same native path as
     * {@link #toColumnStatistic} so the narrowed min/max live in the same
     * domain as the base column statistics. Empty when not convertible.
     */
    public static OptionalDouble statisticDomainValue(String value, Type columnType) {
        if (value == null || MAX_VALUE_SENTINEL.equals(value)) {
            return OptionalDouble.empty();
        }
        try {
            TStatisticData data = new TStatisticData();
            data.setColumnName("__bound__");
            data.setRowCount(1);
            data.setDataSize(0);
            data.setCountDistinct(1);
            data.setNullCount(0);
            data.setMax(value);
            data.setMin(value);
            ColumnStatistic converted = ColumnBasicStatsCacheLoader.buildColumnStatistics(
                    data, "default_catalog", "", "", "__bound__", columnType);
            if (Double.isInfinite(converted.getMinValue()) || Double.isNaN(converted.getMinValue())) {
                return OptionalDouble.empty();
            }
            return OptionalDouble.of(converted.getMinValue());
        } catch (Exception e) {
            return OptionalDouble.empty();
        }
    }
}
