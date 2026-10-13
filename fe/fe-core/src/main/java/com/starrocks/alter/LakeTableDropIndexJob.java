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
import com.starrocks.thrift.TIndexType;

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
     * (BITMAP / NGRAMBF / BLOOM_FILTER all share indexId=-1). Also the
     * source the BE payload is rebuilt from (see {@link #buildDropInfos}).
     */
    @SerializedName(value = "dropIndexNames")
    private List<String> dropIndexNames = new ArrayList<>();

    /**
     * Thrift payload for BE: (index_id, col_unique_id, index_type) triples,
     * resolved at each dispatch by {@link #prepareAlterRequests}. Never
     * persisted: Gson drops every field of a Thrift struct, so a journaled
     * copy would come back empty. Older FEs wrote it under "dropInfos"; a new
     * FE ignores that key.
     */
    private transient List<TDropIndexInfo> dropInfos = new ArrayList<>();

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
                                 List<Long> dropIndexIds, List<String> dropIndexNames) {
        this(jobId, dbId, tableId, tableName, timeoutMs, dropIndexIds, dropIndexNames, new ArrayList<>());
    }

    public LakeTableDropIndexJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                                 List<Long> dropIndexIds, List<String> dropIndexNames, List<String> dropBfColumns) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
        this.dropIndexIds = new ArrayList<>(dropIndexIds);
        this.dropIndexNames = new ArrayList<>(dropIndexNames);
        this.dropBfColumns = new ArrayList<>(dropBfColumns);
    }

    protected LakeTableDropIndexJob(LakeTableDropIndexJob other) {
        super(other);
        this.dropIndexIds = other.dropIndexIds == null ? null : new ArrayList<>(other.dropIndexIds);
        this.dropIndexNames = other.dropIndexNames == null ? null : new ArrayList<>(other.dropIndexNames);
        this.dropBfColumns = other.dropBfColumns == null ? null : new ArrayList<>(other.dropBfColumns);
    }

    /**
     * The tombstones BE writes for this job, as the Thrift payload of its alter tasks: one (index_id, col_unique_id,
     * index_type) entry per column of each index named in {@code dropIndexNames} (multi-column indexes produce one
     * per column, so BE's IDG tombstone projection matches per (col_uid, type)), then one BLOOM_FILTER entry per
     * {@code dropBfColumns} column. Plain bloom filter has no Index object, so its index_id is the -1 sentinel; BE's
     * apply_drop_index keys on (col_unique_id, index_type) for BF flavors. Reads only persisted fields and the table,
     * so the FE that built the job and any FE that loaded it from the journal or an image derive the same payload: the
     * table stays in SCHEMA_CHANGE until the job is done, so neither its indexes nor its columns change in between.
     */
    List<TDropIndexInfo> buildDropInfos(OlapTable table) throws AlterCancelException {
        List<TDropIndexInfo> result = new ArrayList<>();
        if (dropIndexNames != null) {
            for (String name : dropIndexNames) {
                Index target = null;
                for (Index ix : table.getIndexes()) {
                    if (ix.getIndexName().equalsIgnoreCase(name)) {
                        target = ix;
                        break;
                    }
                }
                if (target == null) {
                    throw new AlterCancelException("index " + name + " does not exist in table " + table.getName());
                }
                if (target.getColumns() == null) {
                    continue;
                }
                for (ColumnId colId : target.getColumns()) {
                    Column col = table.getColumn(colId);
                    if (col == null) {
                        continue;
                    }
                    result.add(dropInfo(target.getIndexId(), col.getUniqueId(), toThriftIndexType(target.getIndexType())));
                }
            }
        }
        if (dropBfColumns != null) {
            for (String name : dropBfColumns) {
                Column col = table.getColumn(name);
                if (col == null) {
                    throw new AlterCancelException("bloom filter column " + name + " does not exist in table "
                            + table.getName());
                }
                result.add(dropInfo(-1, col.getUniqueId(), TIndexType.BLOOM_FILTER));
            }
        }
        return result;
    }

    private static TDropIndexInfo dropInfo(long indexId, int colUniqueId, TIndexType indexType) {
        TDropIndexInfo info = new TDropIndexInfo();
        info.setIndex_id(indexId);
        info.setCol_unique_id(colUniqueId);
        info.setIndex_type(indexType);
        return info;
    }

    @Override
    protected void prepareAlterRequests(OlapTable table) throws AlterCancelException {
        List<TDropIndexInfo> infos = buildDropInfos(table);
        // A job always drops something. Nothing to drop means the definitions this FE holds do not describe the job;
        // fail here rather than send BE requests it rejects.
        if (infos.isEmpty()) {
            throw new AlterCancelException("DROP INDEX job " + jobId + " found nothing to drop on table "
                    + table.getName());
        }
        dropInfos = infos;
    }

    @Override
    protected void populateAlterRequest(AlterReplicaTask task, long indexMetaId,
                                        com.starrocks.catalog.MaterializedIndexMeta indexMeta, OlapTable table) {
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
        // Must be journaled: replay() installs whatever the FINISHED entry carries, so a null here
        // would leave a recovered FE unable to resolve the schema this flip retired.
        copy.historySchema = this.historySchema;
        copySubclassFields(copy);
        return copy;
    }

    @Override
    protected void copySubclassFields(LakeTableIndexFastPathJobBase copy) {
        LakeTableDropIndexJob c = (LakeTableDropIndexJob) copy;
        c.dropIndexIds = this.dropIndexIds == null ? null : new ArrayList<>(this.dropIndexIds);
        // The names are what the BE payload is rebuilt from and the dropped Index objects are matched by, so every
        // journal entry carries them, not only the one that submitted the job.
        c.dropIndexNames = this.dropIndexNames == null ? null : new ArrayList<>(this.dropIndexNames);
        c.dropBfColumns = this.dropBfColumns == null ? null : new ArrayList<>(this.dropBfColumns);
    }

    // Accessors for tests / tooling.
    public List<Long> getDropIndexIds() {
        return dropIndexIds;
    }

    public List<String> getDropIndexNames() {
        return dropIndexNames;
    }

    public List<String> getDropBfColumns() {
        return dropBfColumns;
    }
}
