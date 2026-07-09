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
import java.util.List;
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
     * New schema id/version allocated once at job build time (see
     * SchemaChangeHandler.tryBuildLakeAddIndexJob). The fast path reuses the
     * base index meta's schema_id but changes its content (adds the index), and
     * every lake by-id schema cache assumes id==content — so without a new id,
     * data loaded after the index and compaction output keep using the cached
     * pre-index schema and build no index. Bumping the id (persisted here so
     * replay is idempotent) forces every cache to miss and pick up the indexed
     * schema. 0 = not set (older job / not applicable).
     */
    @SerializedName(value = "newSchemaId")
    private long newSchemaId = 0;
    @SerializedName(value = "newSchemaVersion")
    private long newSchemaVersion = 0;

    /**
     * Base index meta id captured at build time. Only tablets belonging to the
     * base index may carry {@link #newSchemaId}: applyCatalogMutation bumps only
     * the base index meta, so stamping a rollup/MV-index tablet would push its BE
     * schema id ahead of its (unchanged) FE meta. -1 = not set.
     */
    @SerializedName(value = "baseIndexMetaId")
    private long baseIndexMetaId = -1;

    /** For deserialization / GSON. */
    public LakeTableAddIndexJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    /** Set the FE-allocated new schema id/version and the base index meta id. */
    public void setNewSchema(long schemaId, long schemaVersion, long baseIndexMetaId) {
        this.newSchemaId = schemaId;
        this.newSchemaVersion = schemaVersion;
        this.baseIndexMetaId = baseIndexMetaId;
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
        this.newSchemaId = other.newSchemaId;
        this.newSchemaVersion = other.newSchemaVersion;
        this.baseIndexMetaId = other.baseIndexMetaId;
    }

    @Override
    protected void populateAlterRequest(AlterReplicaTask task, long indexMetaId) {
        task.setOnlyAddIndex(indexesToAdd);
        // Only the base index meta has its schema id/version bumped
        // (applyCatalogMutation), so only base-index tablets may carry the new
        // schema id. Stamping a rollup/MV-index tablet would push its BE schema
        // id ahead of the unchanged FE meta; those tablets keep their existing
        // schema id (pre-fix behavior).
        if (newSchemaId > 0 && indexMetaId == baseIndexMetaId) {
            task.setNewIndexSchema(newSchemaId, newSchemaVersion);
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

        // Bump the base index meta's schema id/version so subsequent loads and
        // compaction resolve the now-indexed schema instead of the cached
        // pre-index one (the fast path reuses the same schema_id but changed its
        // content; every lake by-id schema cache would otherwise return stale).
        // Idempotent on replay: the persisted newSchemaId/version are set
        // verbatim. Mirrors LakeTableAsyncFastSchemaChangeJob's meta update.
        if (newSchemaId > 0) {
            long baseMetaId = table.getBaseIndexMetaId();
            MaterializedIndexMeta baseMeta = table.getIndexMetaByMetaId(baseMetaId);
            if (baseMeta != null) {
                MaterializedIndexMeta copy = baseMeta.shallowCopy();
                copy.setSchemaId(newSchemaId);
                copy.setSchemaVersion((int) newSchemaVersion);
                table.getIndexMetaIdToMeta().put(baseMetaId, copy);
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
        c.newSchemaId = this.newSchemaId;
        c.newSchemaVersion = this.newSchemaVersion;
        c.baseIndexMetaId = this.baseIndexMetaId;
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
