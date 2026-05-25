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

package com.starrocks.alter;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TDropIndexInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Lake-only fast-path Job for {@code ALTER TABLE ... DROP INDEX}. Pure
 * metadata on BE: writes an OpDropIndex TxnLog tombstone into IDG;
 * physical {@code .idx} cleanup is deferred to compaction.
 *
 * <p>Catalog mutation at finish: remove each dropped {@link Index} from
 * {@code table.getIndexes()}, and (for NGRAMBF) clear {@code is_bf_column}
 * on columns that are no longer referenced by any bloom-family index.
 */
public class LakeTableDropIndexJob extends LakeTableIndexFastPathJobBase {

    /**
     * Catalog-side index ids to remove (for replay idempotency).
     *
     * <p>Important: for BITMAP / NGRAMBF / BLOOM_FILTER, {@code Index.indexId}
     * stays {@code -1} (only GIN/VECTOR get a real id via
     * {@code incAndGetMaxIndexId} — see
     * {@code SchemaChangeHandler.processAddIndex}). Matching purely by id
     * is therefore lossy when the table has multiple BITMAP / NGRAMBF
     * indexes: a single DROP would Set-contains-(-1) match all of them.
     * applyCatalogMutation matches by NAME first; this list is used only
     * when {@code IndexType.isCompatibleIndex} indexes (id &gt;= 0) are
     * involved.
     */
    @SerializedName(value = "dropIndexIds")
    private List<Long> dropIndexIds = new ArrayList<>();

    /**
     * Catalog-side index names to remove. Required because the indexId
     * shortcut is not unique across non-isCompatibleIndex types
     * (BITMAP / NGRAMBF / BLOOM_FILTER all share indexId=-1).
     */
    @SerializedName(value = "dropIndexNames")
    private List<String> dropIndexNames = new ArrayList<>();

    /** Thrift payload for BE: (index_id, col_unique_id, index_type) triples. */
    @SerializedName(value = "dropInfos")
    private List<TDropIndexInfo> dropInfos = new ArrayList<>();

    /**
     * Columns whose plain bloom filter (is_bf_column) should be cleared by
     * this fast-path job. Empty for the USING-clause path. Populated when
     * the alter originated from a {@code bloom_filter_columns} property
     * change that drops columns; applyCatalogMutation removes these from
     * the table's bf set and flips is_bf_column=false on each.
     */
    @SerializedName(value = "dropBfColumns")
    private List<String> dropBfColumns = new ArrayList<>();

    /** For deserialization / GSON. */
    public LakeTableDropIndexJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    public LakeTableDropIndexJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                                 List<Long> dropIndexIds, List<String> dropIndexNames,
                                 List<TDropIndexInfo> dropInfos) {
        this(jobId, dbId, tableId, tableName, timeoutMs, dropIndexIds, dropIndexNames, dropInfos, new ArrayList<>());
    }

    /**
     * Backward-compat overload used by existing unit tests that constructed the
     * job without a names list. Production callers must use the constructor
     * with {@code dropIndexNames} populated — otherwise BITMAP / NGRAMBF /
     * BLOOM_FILTER drops will not remove any catalog Index (their id is -1
     * and applyCatalogMutation falls back to name matching).
     */
    public LakeTableDropIndexJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                                 List<Long> dropIndexIds, List<TDropIndexInfo> dropInfos) {
        this(jobId, dbId, tableId, tableName, timeoutMs, dropIndexIds, new ArrayList<>(), dropInfos, new ArrayList<>());
    }

    /**
     * Backward-compat overload with bf columns but no names list.
     */
    public LakeTableDropIndexJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                                 List<Long> dropIndexIds, List<TDropIndexInfo> dropInfos,
                                 List<String> dropBfColumns) {
        this(jobId, dbId, tableId, tableName, timeoutMs, dropIndexIds, new ArrayList<>(), dropInfos, dropBfColumns);
    }

    public LakeTableDropIndexJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                                 List<Long> dropIndexIds, List<String> dropIndexNames,
                                 List<TDropIndexInfo> dropInfos, List<String> dropBfColumns) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
        this.dropIndexIds = new ArrayList<>(dropIndexIds);
        this.dropIndexNames = new ArrayList<>(dropIndexNames);
        this.dropInfos = new ArrayList<>(dropInfos);
        this.dropBfColumns = new ArrayList<>(dropBfColumns);
    }

    protected LakeTableDropIndexJob(LakeTableDropIndexJob other) {
        super(other);
        this.dropIndexIds = other.dropIndexIds == null ? null : new ArrayList<>(other.dropIndexIds);
        this.dropIndexNames = other.dropIndexNames == null ? null : new ArrayList<>(other.dropIndexNames);
        this.dropInfos = other.dropInfos == null ? null : new ArrayList<>(other.dropInfos);
        this.dropBfColumns = other.dropBfColumns == null ? null : new ArrayList<>(other.dropBfColumns);
    }

    @Override
    protected void populateAlterRequest(AlterReplicaTask task) {
        task.setOnlyDropIndex(dropInfos);
    }

    @Override
    protected void applyCatalogMutation(OlapTable table) {
        // Remove matching Index objects from the table's index list. BE
        // observes the index removal through the published schema and no
        // longer emits ngrambf blobs for the column. The table-level
        // bloom_filter_columns property stays out of band and is managed
        // separately via ALTER TABLE ... SET PROPERTIES.
        //
        // Match by NAME — not by indexId — because BITMAP / NGRAMBF /
        // BLOOM_FILTER all share indexId = -1 (only GIN/VECTOR get a real
        // id, see SchemaChangeHandler.processAddIndex). A previous
        // implementation matched by id, which silently removed every
        // non-isCompatibleIndex index from the table on a single DROP.
        // Fall back to id for GIN/VECTOR (id >= 0).
        List<Index> existing = table.getIndexes();
        Set<String> nameSet = new HashSet<>();
        for (String n : dropIndexNames) {
            if (n != null) {
                nameSet.add(n.toLowerCase());
            }
        }
        Set<Long> idSet = new HashSet<>();
        for (Long id : dropIndexIds) {
            if (id != null && id >= 0) {
                idSet.add(id);
            }
        }
        Iterator<Index> it = existing.iterator();
        while (it.hasNext()) {
            Index ix = it.next();
            String name = ix.getIndexName();
            boolean matchByName = name != null && nameSet.contains(name.toLowerCase());
            boolean matchById = ix.getIndexId() >= 0 && idSet.contains(ix.getIndexId());
            if (matchByName || matchById) {
                it.remove();
            }
        }
        // Plain bloom filter drop (BF IDG fast path): remove these columns
        // from the table's bf set. `Column.is_bf_column` is derived by
        // Column.setIndexFlag from OlapTable.bfColumns at schema publish,
        // so updating the table-level set is sufficient. BE IDG tombstone
        // is already written; this publishes catalog state for future
        // writers and readers. Idempotent on replay.
        if (dropBfColumns != null && !dropBfColumns.isEmpty()) {
            Set<ColumnId> remaining = new TreeSet<>(ColumnId.CASE_INSENSITIVE_ORDER);
            if (table.getBfColumnIds() != null) {
                remaining.addAll(table.getBfColumnIds());
            }
            for (String name : dropBfColumns) {
                Column col = table.getColumn(name);
                if (col == null) {
                    continue;
                }
                remaining.remove(ColumnId.create(col.getName()));
            }
            if (remaining.isEmpty()) {
                // Dropping the last BF column clears the table-level fpp too
                // so subsequent legacy-style property queries see a clean
                // "no bloom filter" state.
                table.setBloomFilterInfo(null, 0);
            } else {
                table.setBloomFilterInfo(remaining, table.getBfFpp());
            }
        }
    }

    @Override
    public AlterJobV2 copyForPersist() {
        LakeTableDropIndexJob copy = new LakeTableDropIndexJob();
        copyBaseFields(copy);
        copy.watershedTxnId = this.watershedTxnId;
        copy.partitionToTablets = this.partitionToTablets;
        copy.tabletToIndexMetaId = this.tabletToIndexMetaId;
        copy.commitVersionMap = this.commitVersionMap;
        copySubclassFields(copy);
        return copy;
    }

    @Override
    protected void copySubclassFields(LakeTableIndexFastPathJobBase copy) {
        LakeTableDropIndexJob c = (LakeTableDropIndexJob) copy;
        c.dropIndexIds = this.dropIndexIds == null ? null : new ArrayList<>(this.dropIndexIds);
        c.dropInfos = this.dropInfos == null ? null : new ArrayList<>(this.dropInfos);
        c.dropBfColumns = this.dropBfColumns == null ? null : new ArrayList<>(this.dropBfColumns);
    }

    // Accessors for tests / tooling.
    public List<Long> getDropIndexIds() {
        return dropIndexIds;
    }

    public List<TDropIndexInfo> getDropInfos() {
        return dropInfos;
    }

    public List<String> getDropBfColumns() {
        return dropBfColumns;
    }
}
