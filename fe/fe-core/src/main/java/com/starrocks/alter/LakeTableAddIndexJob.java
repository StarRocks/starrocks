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
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TOlapTableIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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

    /**
     * Columns newly enabled for plain bloom filter (is_bf_column=true) by
     * this fast-path job. Empty for the USING-clause path (BITMAP / NGRAMBF
     * / GIN). Populated when the alter originated from a
     * {@code bloom_filter_columns} property change; applyCatalogMutation
     * merges these into the table's bf set (and the corresponding synthetic
     * BLOOM_FILTER entries in {@code indexesToAdd} drive the BE-side build).
     */
    @SerializedName(value = "addBfColumns")
    private List<String> addBfColumns = new ArrayList<>();

    /**
     * Per-index-meta new schema id + version, allocated once at job build time
     * (one getNextId() per affected index meta; version = that meta's own
     * schemaVersion + 1). The fast path reuses each index meta's schema_id but
     * changes its content (adds the index); every lake by-id schema cache assumes
     * id==content, so without a fresh id per index, loads/compaction on that index
     * keep resolving the cached pre-index schema and build no index. Each index's
     * tablets are stamped with THAT index's new id (populateAlterRequest), and
     * each affected meta is bumped in applyCatalogMutation — the dispatch set, the
     * allocation set, and the mutation set are kept identical so FE and BE schema
     * ids never diverge (e.g. on a table with rollup / sync-MV indexes). Persisted
     * for idempotent replay. Key = index meta id; value = [schemaId, schemaVersion].
     */
    @SerializedName(value = "indexMetaIdToNewSchema")
    private Map<Long, long[]> indexMetaIdToNewSchema = new HashMap<>();

    /** For deserialization / GSON. */
    public LakeTableAddIndexJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    /** Record the FE-allocated new schema id/version for one affected index meta. */
    public void putNewSchema(long indexMetaId, long schemaId, long schemaVersion) {
        indexMetaIdToNewSchema.put(indexMetaId, new long[] {schemaId, schemaVersion});
    }

    public LakeTableAddIndexJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                                List<Index> newIndexes, List<TOlapTableIndex> indexesToAdd) {
        this(jobId, dbId, tableId, tableName, timeoutMs, newIndexes, indexesToAdd, new ArrayList<>());
    }

    public LakeTableAddIndexJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                                List<Index> newIndexes, List<TOlapTableIndex> indexesToAdd,
                                List<String> addBfColumns) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
        this.newIndexes = new ArrayList<>(newIndexes);
        this.indexesToAdd = new ArrayList<>(indexesToAdd);
        this.addBfColumns = new ArrayList<>(addBfColumns);
    }

    protected LakeTableAddIndexJob(LakeTableAddIndexJob other) {
        super(other);
        this.newIndexes = other.newIndexes == null ? null : new ArrayList<>(other.newIndexes);
        this.indexesToAdd = other.indexesToAdd == null ? null : new ArrayList<>(other.indexesToAdd);
        this.addBfColumns = other.addBfColumns == null ? null : new ArrayList<>(other.addBfColumns);
        this.indexMetaIdToNewSchema = other.indexMetaIdToNewSchema == null ? null
                : new HashMap<>(other.indexMetaIdToNewSchema);
    }

    /**
     * Returns the subset of {@code indexes} whose columns are ALL present in the
     * given materialized index meta's schema. A rollup / sync-MV that projects
     * away an indexed column must not receive (or build) that index — mirroring
     * the legacy path, which only builds indexes on the base index. A null meta
     * (defensive) applies every index.
     */
    static List<TOlapTableIndex> applicableIndexes(List<TOlapTableIndex> indexes, MaterializedIndexMeta meta) {
        if (indexes == null || indexes.isEmpty() || meta == null) {
            return indexes;
        }
        Set<String> metaColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : meta.getSchema()) {
            metaColumnNames.add(column.getName());
        }
        List<TOlapTableIndex> applicable = new ArrayList<>();
        for (TOlapTableIndex ix : indexes) {
            if (ix.getColumns() != null && metaColumnNames.containsAll(ix.getColumns())) {
                applicable.add(ix);
            }
        }
        return applicable;
    }

    @Override
    protected void populateAlterRequest(AlterReplicaTask task, long indexMetaId, MaterializedIndexMeta indexMeta) {
        // A materialized index (rollup / sync MV) whose schema projects away an
        // indexed column must not build that index: send it only the indexes
        // whose columns its schema carries. An empty list makes BE write a no-op
        // txn log so the reserved alter version still publishes on its tablets.
        task.setOnlyAddIndex(applicableIndexes(indexesToAdd, indexMeta));
        // Each affected index meta got its OWN new schema id/version; stamp this
        // tablet with its index's id so BE bumps that index's schema, and
        // applyCatalogMutation bumps the same meta. Keeps FE and BE schema ids
        // aligned per index. Metas with no applicable index were never allocated
        // an entry (see the build sites), so their tablets carry no stamp.
        long[] newSchema = indexMetaIdToNewSchema.get(indexMetaId);
        if (newSchema != null && newSchema[0] > 0) {
            task.setNewIndexSchema(newSchema[0], newSchema[1]);
        }
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
            // For NGRAMBF we rely purely on the presence of the Index object
            // in table.getIndexes(); BE reads tablet_index[NGRAMBF] directly
            // from the published schema without needing a per-column
            // is_bf_column flip. (The table-level bloom_filter_columns
            // property stays out of band and is set via ALTER TABLE ...
            // SET PROPERTIES.)
        }
        // Plain bloom filter add (BF IDG fast path): merge addBfColumns into
        // the table's bf set. `Column.is_bf_column` is not a first-class
        // Column attribute — TColumn.is_bloom_filter_column is derived by
        // Column.setIndexFlag from OlapTable.bfColumns at schema publish
        // time, so updating the table-level set is sufficient. BE already
        // holds the IDG .idx payloads; this call publishes the "column is
        // a bf column" fact to future writers and the query path.
        if (addBfColumns != null && !addBfColumns.isEmpty()) {
            Set<ColumnId> merged = new TreeSet<>(ColumnId.CASE_INSENSITIVE_ORDER);
            if (table.getBfColumnIds() != null) {
                merged.addAll(table.getBfColumnIds());
            }
            for (String name : addBfColumns) {
                Column col = table.getColumn(name);
                if (col == null) {
                    continue;
                }
                merged.add(ColumnId.create(col.getName()));
            }
            double fpp = table.getBfFpp();
            if (fpp <= 0) {
                fpp = FeConstants.DEFAULT_BLOOM_FILTER_FPP;
            }
            table.setBloomFilterInfo(merged, fpp);
        }

        // Bump EACH affected index meta's schema id/version so subsequent loads
        // and compaction resolve the now-indexed schema instead of the cached
        // pre-index one (the fast path reuses the schema_id but changed its
        // content; every lake by-id schema cache would otherwise return stale).
        // Per-index — the same set stamped onto BE tablets (populateAlterRequest)
        // — so a rollup / sync-MV index meta stays consistent with its tablets.
        // Idempotent on replay: the persisted ids/versions are set verbatim.
        // Mirrors LakeTableAsyncFastSchemaChangeJob's per-index meta update.
        if (indexMetaIdToNewSchema != null && !indexMetaIdToNewSchema.isEmpty()) {
            boolean bumped = false;
            for (Map.Entry<Long, long[]> entry : indexMetaIdToNewSchema.entrySet()) {
                MaterializedIndexMeta meta = table.getIndexMetaByMetaId(entry.getKey());
                if (meta == null) {
                    continue;
                }
                long[] newSchema = entry.getValue();
                MaterializedIndexMeta copy = meta.shallowCopy();
                copy.setSchemaId(newSchema[0]);
                copy.setSchemaVersion((int) newSchema[1]);
                table.getIndexMetaIdToMeta().put(entry.getKey(), copy);
                bumped = true;
            }
            if (bumped) {
                table.rebuildFullSchema();
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
        c.addBfColumns = this.addBfColumns == null ? null : new ArrayList<>(this.addBfColumns);
        c.indexMetaIdToNewSchema = this.indexMetaIdToNewSchema == null ? null
                : new HashMap<>(this.indexMetaIdToNewSchema);
    }

    // Accessors for tests / tooling.
    public List<Index> getNewIndexes() {
        return newIndexes;
    }

    public List<TOlapTableIndex> getIndexesToAdd() {
        return indexesToAdd;
    }

    public List<String> getAddBfColumns() {
        return addBfColumns;
    }
}
