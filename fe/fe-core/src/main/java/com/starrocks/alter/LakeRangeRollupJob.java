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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.reshard.presplit.ReservoirSampler;
import com.starrocks.alter.reshard.presplit.Sampler;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

/**
 * Additive-index (rollup) job for shared-data (lake) range-distribution OLAP tables.
 *
 * <p>This is the additive sibling of {@link LakeRangeRewriteSchemaChangeJob} (which replaces the
 * base index). This job adds a new rollup index as a column subset of the base, building a K-tablet
 * shadow index presplit by the rollup sort key and materialising it via an internal INSERT.
 *
 * <p>The generic online-rewrite state machine, the watershed allocation, the resumable per-partition
 * rewrite, and the reserve/publish/flip chain all live in {@link LakeOnlineRewriteJobBase}. This
 * subclass supplies the rollup specifics: the rollup column subset and sort key, the additive flip
 * (promotes the shadow index to NORMAL without dropping the base), and the abstract hooks for
 * shadow-layout planning, index-meta registration, and catalog flip (implemented in Tasks 3-4).
 */
public class LakeRangeRollupJob extends LakeOnlineRewriteJobBase {
    private static final Logger LOG = LogManager.getLogger(LakeRangeRollupJob.class);

    // The rollup column subset and key configuration applied when the shadow is registered and flipped.
    @SerializedName(value = "rollupSchema")
    private List<Column> rollupSchema;
    @SerializedName(value = "rollupKeysType")
    private KeysType rollupKeysType;
    @SerializedName(value = "rollupSortKeyIdxes")
    private List<Integer> rollupSortKeyIdxes;
    @SerializedName(value = "rollupSortKeyUniqueIds")
    private List<Integer> rollupSortKeyUniqueIds;
    @SerializedName(value = "rollupSortKeyColumns")
    private List<Column> rollupSortKeyColumns;
    @SerializedName(value = "shadowShortKeyColumnCount")
    private short shadowShortKeyColumnCount;

    // Sampling seam. Production samples a partition's base by the rollup sort key; tests inject a stub.
    // Not serialized.
    private transient Sampler sampler;

    // for deserialization
    public LakeRangeRollupJob() {
        super(JobType.ROLLUP);
    }

    public LakeRangeRollupJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.ROLLUP, dbId, tableId, tableName, timeoutMs);
    }

    // Deep-copy constructor used by copyForPersist(): the WAL must record a snapshot, not a live
    // reference. Chains through super(other) for the base + watershed + shadow-identity + partitionStates
    // fields and deep-copies this job's own collections. The transient sampler is intentionally NOT copied.
    protected LakeRangeRollupJob(LakeRangeRollupJob other) {
        super(other);
        this.rollupSchema = other.rollupSchema == null ? null : new ArrayList<>(other.rollupSchema);
        this.rollupKeysType = other.rollupKeysType;
        this.rollupSortKeyIdxes = other.rollupSortKeyIdxes == null ? null : new ArrayList<>(other.rollupSortKeyIdxes);
        this.rollupSortKeyUniqueIds =
                other.rollupSortKeyUniqueIds == null ? null : new ArrayList<>(other.rollupSortKeyUniqueIds);
        this.rollupSortKeyColumns =
                other.rollupSortKeyColumns == null ? null : new ArrayList<>(other.rollupSortKeyColumns);
        this.shadowShortKeyColumnCount = other.shadowShortKeyColumnCount;
    }

    public void setRollupSchema(List<Column> rollupSchema) {
        this.rollupSchema = rollupSchema;
    }

    public void setRollupKeysType(KeysType rollupKeysType) {
        this.rollupKeysType = rollupKeysType;
    }

    public void setRollupSortKeyIdxes(List<Integer> v) {
        this.rollupSortKeyIdxes = v;
    }

    public void setRollupSortKeyUniqueIds(List<Integer> v) {
        this.rollupSortKeyUniqueIds = v;
    }

    public void setRollupSortKeyColumns(List<Column> v) {
        this.rollupSortKeyColumns = v;
    }

    // originIndexMetaId here is the BASE index the rollup derives its data from (NOT an index to drop).
    public void setShadowIndex(long shadowIndexMetaId, long baseIndexMetaId, String rollupIndexName,
                               short shadowShortKeyColumnCount) {
        this.shadowIndexMetaId = shadowIndexMetaId;
        this.originIndexMetaId = baseIndexMetaId;
        this.shadowIndexName = rollupIndexName;
        this.shadowShortKeyColumnCount = shadowShortKeyColumnCount;
    }

    /**
     * Inject the sampler used by {@link #planPartitionShadow}. Tests use this to bypass the real SQL
     * sample sub-query (which needs a backend); production leaves it null so {@link #getSampler}
     * lazily builds the real internal-partition sampler.
     */
    @VisibleForTesting
    public void setSampler(Sampler sampler) {
        this.sampler = sampler;
    }

    private Sampler getSampler() {
        if (sampler == null) {
            sampler = ReservoirSampler.forInternalPartition();
        }
        return sampler;
    }

    /** The rollup's user-facing name. Used by CANCEL ALTER TABLE ROLLUP and force-drop matching. */
    public String getRollupIndexName() {
        return shadowIndexName;
    }

    @Override
    protected OlapTable.OlapTableState jobTableState() {
        return OlapTable.OlapTableState.ROLLUP;
    }

    @Override
    protected void validateRewriteConfig() throws AlterCancelException {
        // Fail loudly before registering bad index metadata if construction/replay left the config
        // incomplete. The job depends on ALL of these.
        Preconditions.checkState(rollupSchema != null && !rollupSchema.isEmpty(), "rollup schema is not set");
        Preconditions.checkNotNull(rollupKeysType, "rollup keys type is not set");
        Preconditions.checkState(rollupSortKeyColumns != null && !rollupSortKeyColumns.isEmpty(),
                "rollup sort key columns are not set");
        Preconditions.checkState(rollupSortKeyIdxes != null && !rollupSortKeyIdxes.isEmpty(),
                "rollup sort key indexes are not set");
        Preconditions.checkState(rollupSortKeyUniqueIds != null
                        && rollupSortKeyUniqueIds.size() == rollupSortKeyIdxes.size(),
                "rollup sort key unique ids are not set / size mismatch");
        Preconditions.checkState(shadowShortKeyColumnCount > 0, "rollup short key column count is not set");
        Preconditions.checkState(shadowIndexMetaId != -1, "rollup shadow index meta id is not set");
        Preconditions.checkState(originIndexMetaId != -1, "rollup base index meta id is not set");
        Preconditions.checkNotNull(shadowIndexName, "rollup index name is not set");
    }

    /**
     * The INSERT...SELECT projection from the watershed-pinned base into the rollup: the rollup's
     * (non-generated) column subset, backquoted for SQL. The AGG value columns pass through by their
     * aggregate-state type; the sink's aggregate-on-write merges them by the rollup key.
     */
    @Override
    protected List<String> rewriteSelectColumnNames(@NotNull OlapTable table) {
        return rollupSchema.stream()
                .filter(column -> !column.isGeneratedColumn())
                .map(column -> ParseUtil.backquote(column.getName()))
                .collect(Collectors.toList());
    }

    /**
     * The rewrite writes a COLUMN SUBSET (the rollup schema), so the INSERT needs an explicit target
     * column list or InsertAnalyzer's source/target count check fails (defaults to full base schema).
     */
    @Override
    protected List<String> rewriteTargetColumnNames(@NotNull OlapTable table) {
        return rollupSchema.stream()
                .filter(column -> !column.isGeneratedColumn())
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    /**
     * Additive flip: nothing existing changes, so no dependent MV is invalidated.
     */
    @Override
    protected Set<String> affectedColumnsForMvInactivation(@NotNull OlapTable table) {
        return Sets.newHashSet();
    }

    /**
     * Additive flip: promotes the rollup index from SHADOW to NORMAL. If any physical partition still
     * holds it in SHADOW state, the flip has not yet been applied.
     */
    @Override
    protected boolean flipNotYetApplied(@NotNull OlapTable table) {
        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            MaterializedIndex idx = physicalPartition.getLatestIndex(shadowIndexMetaId);
            if (idx != null && idx.getState() == MaterializedIndex.IndexState.SHADOW) {
                return true;
            }
        }
        return false;
    }

    /**
     * Plan one partition's K-tablet shadow layout for the rollup index. Implemented in Task 3.
     */
    @Override
    protected void planPartitionShadow(PendingPartitionPlan plan, OlapTable table, String dbName)
            throws AlterCancelException {
        throw new UnsupportedOperationException("implemented in Task 3/4");
    }

    /**
     * Register the rollup shadow index's MaterializedIndexMeta on the table. Implemented in Task 3.
     */
    @Override
    protected void registerShadowIndexMeta(@NotNull OlapTable table) {
        throw new UnsupportedOperationException("implemented in Task 3/4");
    }

    /**
     * Promote the shadow rollup index to NORMAL as an additional (not replacing) index. Implemented in Task 4.
     */
    @Override
    protected void visualiseShadowIndex(@NotNull OlapTable table) {
        throw new UnsupportedOperationException("implemented in Task 3/4");
    }

    /**
     * Emit the rollup-shaped row for SHOW ALTER TABLE ROLLUP (12 columns per {@link
     * com.starrocks.common.proc.RollupProcDir#TITLE_NAMES}):
     * JobId, TableName, CreateTime, FinishedTime, BaseIndexName, RollupIndexName, RollupId,
     * TransactionId, State, Msg, Progress, Timeout.
     */
    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        // Resolve the base index name from the live catalog when available.
        String baseIndexName = "";
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db != null) {
            OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getId(), tableId);
            if (olapTable != null) {
                String resolved = olapTable.getIndexNameByMetaId(originIndexMetaId);
                if (resolved != null) {
                    baseIndexName = resolved;
                }
            }
        }
        // rollup index name: the new index being built.
        String rollupName = shadowIndexName != null ? shadowIndexName : "";

        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);
        info.add(tableName);
        info.add(TimeUtils.longToTimeString(createTimeMs));
        info.add(TimeUtils.longToTimeString(finishedTimeMs));
        info.add(baseIndexName);
        info.add(rollupName);
        info.add(shadowIndexMetaId);
        info.add(getTransactionId().orElse(-1L));
        info.add(jobState.name());
        info.add(errMsg);
        info.add(FeConstants.NULL_STRING);
        info.add(timeoutMs / 1000);
        infos.add(info);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        // Copy the subclass-specific journaled fields first so the base's catalog reconstruction hooks
        // (registerShadowIndexMeta / visualiseShadowIndex) see this job's rollup config.
        if (this != replayedJob) {
            LakeRangeRollupJob other = (LakeRangeRollupJob) replayedJob;
            this.rollupSchema = other.rollupSchema;
            this.rollupKeysType = other.rollupKeysType;
            this.rollupSortKeyIdxes = other.rollupSortKeyIdxes;
            this.rollupSortKeyUniqueIds = other.rollupSortKeyUniqueIds;
            this.rollupSortKeyColumns = other.rollupSortKeyColumns;
            this.shadowShortKeyColumnCount = other.shadowShortKeyColumnCount;
        }
        super.replay(replayedJob);
    }

    @Override
    public AlterJobV2 copyForPersist() {
        // The WAL records a deep snapshot (not a live reference) so journaling never mutates the
        // running job.
        return new LakeRangeRollupJob(this);
    }
}
