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
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TDropIndexInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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

    /** Catalog-side index ids to remove (for replay idempotency). */
    @SerializedName(value = "dropIndexIds")
    private List<Long> dropIndexIds = new ArrayList<>();

    /** Thrift payload for BE: (index_id, col_unique_id, index_type) triples. */
    @SerializedName(value = "dropInfos")
    private List<TDropIndexInfo> dropInfos = new ArrayList<>();

    /** For deserialization / GSON. */
    public LakeTableDropIndexJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    public LakeTableDropIndexJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                                 List<Long> dropIndexIds, List<TDropIndexInfo> dropInfos) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
        this.dropIndexIds = new ArrayList<>(dropIndexIds);
        this.dropInfos = new ArrayList<>(dropInfos);
    }

    protected LakeTableDropIndexJob(LakeTableDropIndexJob other) {
        super(other);
        this.dropIndexIds = other.dropIndexIds == null ? null : new ArrayList<>(other.dropIndexIds);
        this.dropInfos = other.dropInfos == null ? null : new ArrayList<>(other.dropInfos);
    }

    @Override
    protected void populateAlterRequest(AlterReplicaTask task) {
        task.setOnlyDropIndex(dropInfos);
    }

    @Override
    protected void applyCatalogMutation(OlapTable table) {
        // Remove matching Index objects from the table's index list.
        List<Index> existing = table.getIndexes();
        Set<Long> idSet = new HashSet<>(dropIndexIds);
        // Track NGRAMBF target column ids that we just dropped, so we can
        // decide whether to clear is_bf_column on those columns.
        Set<Integer> droppedNgramColUids = new HashSet<>();
        Iterator<Index> it = existing.iterator();
        while (it.hasNext()) {
            Index ix = it.next();
            if (idSet.contains(ix.getIndexId())) {
                if (ix.getIndexType() == IndexDef.IndexType.NGRAMBF && ix.getColumns() != null) {
                    for (com.starrocks.catalog.ColumnId colId : ix.getColumns()) {
                        Column col = table.getColumnByUniqueId(colId);
                        if (col != null) {
                            droppedNgramColUids.add(col.getUniqueId());
                        }
                    }
                }
                it.remove();
            }
        }
        if (droppedNgramColUids.isEmpty()) {
            return;
        }
        // Clear is_bf_column on any column whose only bloom-family index was
        // just removed. A column may still have is_bf_column=true if it's
        // also in the table-level bloom_filter_columns property OR referenced
        // by another NGRAMBF index.
        Set<Integer> stillReferenced = new HashSet<>();
        for (Index ix : existing) {
            if (ix.getIndexType() == IndexDef.IndexType.NGRAMBF && ix.getColumns() != null) {
                for (com.starrocks.catalog.ColumnId colId : ix.getColumns()) {
                    Column col = table.getColumnByUniqueId(colId);
                    if (col != null) {
                        stillReferenced.add(col.getUniqueId());
                    }
                }
            }
        }
        // The "table-level bloom_filter_columns" property is reflected in
        // the is_bf_column flag that was set at CREATE TABLE time; we must
        // NOT clear it here. Only clear is_bf_column when the only reason
        // it was set was an NGRAMBF index we just dropped, not a table
        // property. Discriminating the two requires checking the table's
        // bloom filter column set explicitly.
        Set<String> tablePropertyBfCols = table.getBfColumnNames() == null ? new HashSet<>()
                : new HashSet<>(table.getBfColumnNames());
        for (Integer uid : droppedNgramColUids) {
            if (stillReferenced.contains(uid)) {
                continue;
            }
            Column col = table.getColumnByUniqueId(new com.starrocks.catalog.ColumnId("<by-uid>"));
            // Fallback: linear scan by uid (ColumnId lookup needs a name).
            for (Column c : table.getFullSchema()) {
                if (c.getUniqueId() == uid.intValue()) {
                    col = c;
                    break;
                }
            }
            if (col == null) {
                continue;
            }
            if (tablePropertyBfCols.stream().anyMatch(n -> n.equalsIgnoreCase(col.getName()))) {
                continue; // set by table property; leave alone
            }
            col.setIsBloomFilterColumn(false);
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
    }

    // Accessors for tests / tooling.
    public List<Long> getDropIndexIds() {
        return dropIndexIds;
    }

    public List<TDropIndexInfo> getDropInfos() {
        return dropInfos;
    }
}
