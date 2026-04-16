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
import com.starrocks.thrift.TOlapTableIndex;

import java.util.ArrayList;
import java.util.List;

/**
 * Lake-only fast-path Job for {@code ALTER TABLE ... ADD INDEX ... USING
 * {BITMAP|NGRAMBF|GIN}}. The BE side produces standalone {@code .idx}
 * payload files (Index Delta Group) per segment without rewriting data.
 *
 * <p>Catalog mutation at finish: append each {@link Index} to
 * {@code table.getIndexes()}, and for NGRAMBF additionally flip the target
 * column's {@code is_bf_column} flag so future writes (including subsequent
 * compaction) build the index inline into segment footers.
 */
public class LakeTableAddIndexJob extends LakeTableIndexFastPathJobBase {

    /**
     * Catalog-side {@link Index} metadata to add. Persisted so a replayed
     * job (FE cold start after crash) can reapply the catalog mutation
     * idempotently.
     */
    @SerializedName(value = "newIndexes")
    private List<Index> newIndexes = new ArrayList<>();

    /**
     * Thrift payload sent to BE. Carries column *names* (not unique ids);
     * BE resolves names via the new tablet schema at
     * {@code do_process_add_index_only}.
     */
    @SerializedName(value = "indexesToAdd")
    private List<TOlapTableIndex> indexesToAdd = new ArrayList<>();

    /** For deserialization / GSON. */
    public LakeTableAddIndexJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    public LakeTableAddIndexJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                                List<Index> newIndexes, List<TOlapTableIndex> indexesToAdd) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
        this.newIndexes = new ArrayList<>(newIndexes);
        this.indexesToAdd = new ArrayList<>(indexesToAdd);
    }

    protected LakeTableAddIndexJob(LakeTableAddIndexJob other) {
        super(other);
        this.newIndexes = other.newIndexes == null ? null : new ArrayList<>(other.newIndexes);
        this.indexesToAdd = other.indexesToAdd == null ? null : new ArrayList<>(other.indexesToAdd);
    }

    @Override
    protected void populateAlterRequest(AlterReplicaTask task) {
        task.setOnlyAddIndex(indexesToAdd);
    }

    @Override
    protected void applyCatalogMutation(OlapTable table) {
        List<Index> existing = table.getIndexes();
        for (Index ix : newIndexes) {
            // Idempotent append: skip if an index with the same id / name
            // already exists (replay path).
            boolean dup = false;
            for (Index e : existing) {
                if (sameIndex(e, ix)) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                existing.add(ix);
            }

            // NGRAMBF: flip is_bf_column on every target column so the
            // existing column-writer path (segment_writer + column_writer
            // branches keyed on `column.is_bf_column()` +
            // `tablet_index[NGRAMBF]`) builds the index inline into all
            // future segments (loads / compaction / etc.).
            if (ix.getIndexType() == IndexDef.IndexType.NGRAMBF) {
                for (String colName : table.getFullSchema().stream()
                        .map(Column::getName).toList()) {
                    if (ix.getColumns() != null && ix.getColumns().stream()
                            .anyMatch(id -> id.getId().equalsIgnoreCase(colName))) {
                        Column col = table.getColumn(colName);
                        if (col != null) {
                            col.setIsBloomFilterColumn(true);
                        }
                    }
                }
            }
        }
    }

    private static boolean sameIndex(Index a, Index b) {
        if (a.getIndexId() >= 0 && b.getIndexId() >= 0) {
            return a.getIndexId() == b.getIndexId();
        }
        return a.getIndexName() != null && a.getIndexName().equalsIgnoreCase(b.getIndexName());
    }

    @Override
    public AlterJobV2 copyForPersist() {
        LakeTableAddIndexJob copy = new LakeTableAddIndexJob();
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
        LakeTableAddIndexJob c = (LakeTableAddIndexJob) copy;
        c.newIndexes = this.newIndexes == null ? null : new ArrayList<>(this.newIndexes);
        c.indexesToAdd = this.indexesToAdd == null ? null : new ArrayList<>(this.indexesToAdd);
    }

    // Accessors for tests / tooling.
    public List<Index> getNewIndexes() {
        return newIndexes;
    }

    public List<TOlapTableIndex> getIndexesToAdd() {
        return indexesToAdd;
    }
}
