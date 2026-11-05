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

package com.starrocks.planner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.BucketProperty;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.DelegatingConnectorMetadata;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.RemoteFilesSampleStrategy;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergConnectorScanRangeSource;
import com.starrocks.connector.iceberg.IcebergGetRemoteFilesParams;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.IcebergRemoteSourceTrigger;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.connector.iceberg.IcebergVendedCredentials;
import com.starrocks.connector.iceberg.QueueIcebergRemoteFileInfoSource;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CredentialRefreshPolicy;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.metrics.ScanReportParser;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class IcebergScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    protected final IcebergTable icebergTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private ScalarOperator icebergJobPlanningPredicate = null;
    private volatile CloudConfiguration cloudConfiguration = null;
    // The table's own vended credentials, when it has them: they expire, so a running scan renews them through
    // refreshScanCredentials and every later serialization carries the newest. Renewed under vendedCredentialsLock.
    private volatile IcebergVendedCredentials vendedCredentials = null;
    private final ReentrantLock vendedCredentialsLock = new ReentrantLock();
    // After a failed renewal, the backends asking meanwhile get the failure without calling the catalog again.
    private long renewalRetryAfterMs = 0;
    private String lastRenewalFailure = null;
    private IcebergConnectorScanRangeSource scanRangeSource = null;
    private final IcebergTableMORParams tableFullMORParams;
    private final IcebergMORParams morParams;
    private volatile boolean reachLimit = false;
    private int selectedPartitionCount = -1;
    private Optional<List<BucketProperty>> bucketProperties = Optional.empty();
    private PartitionIdGenerator partitionIdGenerator = null;
    private IcebergMetricsReporter icebergScanMetricsReporter;
    private boolean usedForDelete = false;
    private Set<String> runtimeFilterPruningColumns = Set.of();
    // Columns probed by a join or top-n runtime filter. Their file bounds let the BE skip a whole file once the
    // filter arrives, so the scan asks for their statistics when the manifest cache keeps them.
    private Set<String> runtimeFilterProbeColumnNames = Set.of();
    // the slot types whose file bounds the BE compares with runtime filters (HdfsScanner::_pruned_by_file_stats);
    // DATETIME only for a timestamp without time zone, whose bounds are sent as they are
    private static final Set<PrimitiveType> FILE_STATS_PRUNING_TYPES = Set.of(PrimitiveType.TINYINT,
            PrimitiveType.SMALLINT, PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.DATE,
            PrimitiveType.DATETIME);
    private boolean enableGlobalLateMaterialization = false;
    private boolean enableIncrementalScanRanges = false;

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                           IcebergTableMORParams tableFullMORParams, IcebergMORParams morParams,
                           PartitionIdGenerator partitionIdGenerator) {
        super(id, desc, planNodeName);
        this.icebergTable = (IcebergTable) desc.getTable();
        this.tableFullMORParams = tableFullMORParams;
        this.morParams = morParams;
        this.partitionIdGenerator = partitionIdGenerator;
        this.icebergScanMetricsReporter = new IcebergMetricsReporter();
        this.icebergTable.setIcebergMetricsReporter(icebergScanMetricsReporter);
        setupCloudCredential();
    }

    @Override
    public boolean hasMoreScanRanges() {
        if (scanRangeSource == null) {
            return false;
        }

        return !reachLimit && scanRangeSource.hasMoreOutput();
    }

    @Override
    public void setReachLimit() {
        reachLimit = true;
    }

    @Override
    public void clear() {
        try {
            if (icebergTable != null) {
                icebergTable.clearMetadata();
            }
            if (scanRangeSource != null) {
                scanRangeSource.close();
            }
        } catch (Exception e) {
            LOG.warn("close Iceberg scanRangeSource failed", e);
        }
        scanRangeSource = null;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        if (tvrVersionRange.isEmpty() || scanRangeSource == null) {
            return List.of();
        }

        // Runtime filters are assigned after the source is set up, so read them when the ranges are built.
        scanRangeSource.setRuntimeFilterProbeSlotIds(runtimeFilterProbeSlotIds());
        if (maxScanRangeLength == 0) {
            return scanRangeSource.getAllOutputs();
        }

        return scanRangeSource.getOutputs((int) maxScanRangeLength);
    }

    private Set<Integer> runtimeFilterProbeSlotIds() {
        Set<Integer> slotIds = new HashSet<>();
        for (RuntimeFilterDescription rf : getProbeRuntimeFilters()) {
            Expr probeExpr = rf.getNodeIdToProbeExpr().get(getId().asInt());
            if (probeExpr instanceof SlotRef slotRef) {
                slotIds.add(slotRef.getSlotId().asInt());
            }
        }
        return slotIds;
    }

    public IcebergConnectorScanRangeSource getSourceRange() {
        return scanRangeSource;
    }

    public void rebuildScanRange(List<RemoteFileInfo> splits) throws StarRocksException {
        Preconditions.checkNotNull(tvrVersionRange, "snapshot id is null");
        if (tvrVersionRange.isEmpty()) {
            LOG.warn(String.format("Table %s has no snapshot!", icebergTable.getCatalogTableName()));
            return;
        }

        if (splits.isEmpty()) {
            LOG.warn("There is no scan tasks after splits",
                    icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), icebergJobPlanningPredicate);
            return;
        }
        if (getSourceRange() != null) {
            getSourceRange().clearScannedFiles();
        }
        RemoteFileInfoSource remoteFileInfoSource;
        remoteFileInfoSource = new RemoteFileInfoDefaultSource(splits);
        if (morParams != IcebergMORParams.EMPTY) {
            boolean needToCheckEqualityIds = tableFullMORParams.size() != 3;
            IcebergRemoteSourceTrigger trigger = new IcebergRemoteSourceTrigger(
                    remoteFileInfoSource, morParams, needToCheckEqualityIds);
            Deque<RemoteFileInfo> remoteFileInfoDeque = trigger.getQueue(morParams);
            remoteFileInfoSource = new QueueIcebergRemoteFileInfoSource(trigger, remoteFileInfoDeque);
        }

        scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                remoteFileInfoSource, morParams, desc, bucketProperties, partitionIdGenerator, true,
                scanOptimizeOption.getCanUseMinMaxOpt());
        scanRangeSource.setRuntimeFilterPruningColumns(runtimeFilterPruningColumns);
    }

    public void setupScanRangeLocations(boolean enableIncrementalScanRanges) throws StarRocksException {
        this.enableIncrementalScanRanges = enableIncrementalScanRanges;
        Preconditions.checkNotNull(tvrVersionRange, "tvrVersionRange id is null");
        if (tvrVersionRange.isEmpty()) {
            LOG.warn(String.format("Table %s has no snapshot!", icebergTable.getCatalogTableName()));
            return;
        }

        // Filter against the read schema (the targeted snapshot's schema for time travel), not the
        // current native schema, so snapshot-only column names (e.g. a column before a later rename)
        // are not dropped before IcebergMetadata calls scan.select(fieldNames).
        Set<String> readSchemaColumnNames = icebergTable.getReadSchema().columns().stream()
                .map(Types.NestedField::name)
                .collect(Collectors.toSet());
        List<String> requiredColumnNames = desc.getSlots().stream()
                .map(slot -> slot.getColumn().getName())
                .filter(readSchemaColumnNames::contains)
                .distinct()
                .collect(Collectors.toList());

        // Bounded-cost statistics-scan budgets: the external ANALYZE job stashes these on the session
        // variable so they ride the normal query path to the connector without a bespoke ANALYZE->connector
        // channel. These variables are INVISIBLE but NOT read-only (the job must set them at runtime), so a
        // user could SET/SET GLOBAL them; a truncated scan on an ordinary query would silently drop rows.
        // Only honor the budget inside a statistics-collection context so a plain SELECT can never be capped.
        long scanBytesCap = -1;
        long scanFilesCap = -1;
        long scanRowsCap = -1;
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.isStatisticsConnection()) {
            SessionVariable sessionVariable = connectContext.getSessionVariable();
            scanBytesCap = sessionVariable.getExternalStatsScanBytesCap();
            scanFilesCap = sessionVariable.getExternalStatsScanFilesCap();
            scanRowsCap = sessionVariable.getExternalStatsScanRowsCap();
        }

        // the min/max optimization reads the file statistics of the scanned columns only; the runtime filter probe
        // columns add theirs when the manifest cache keeps them
        boolean canUseMinMaxOpt = scanOptimizeOption.getCanUseMinMaxOpt();
        Set<String> statsColumnNames = new HashSet<>();
        if (canUseMinMaxOpt) {
            statsColumnNames.addAll(requiredColumnNames);
        }
        statsColumnNames.addAll(runtimeFilterProbeStatsColumnNames(readSchemaColumnNames, statsColumnNames));

        GetRemoteFilesParams params =
                IcebergGetRemoteFilesParams.newBuilder()
                        .setAllParams(tableFullMORParams)
                        .setParams(morParams)
                        .setTableVersionRange(tvrVersionRange)
                        .setPredicate(icebergJobPlanningPredicate)
                        .setEnableColumnStats(canUseMinMaxOpt || !statsColumnNames.isEmpty())
                        .setStatsColumnNames(canUseMinMaxOpt || !statsColumnNames.isEmpty() ? statsColumnNames : null)
                        // the runtime filter probe columns alone only want what the manifest cache already keeps
                        .setOptionalColumnStats(!canUseMinMaxOpt)
                        .setUsedForDelete(usedForDelete)
                        .setFieldNames(requiredColumnNames)
                        .setScanBytesCap(scanBytesCap)
                        .setScanFilesCap(scanFilesCap)
                        .setScanRowsCap(scanRowsCap)
                        .build();

        RemoteFileInfoSource remoteFileInfoSource;
        if (enableIncrementalScanRanges) {
            remoteFileInfoSource = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFilesAsync(icebergTable, params);
        } else {
            List<RemoteFileInfo> splits = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(icebergTable, params);
            if (splits.isEmpty()) {
                LOG.warn("There is no scan tasks after planFies on {}.{} and predicate: [{}]",
                        icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), icebergJobPlanningPredicate);
                return;
            }
            remoteFileInfoSource = new RemoteFileInfoDefaultSource(splits);
            if (morParams != IcebergMORParams.EMPTY) {
                boolean needToCheckEqualityIds = tableFullMORParams.size() != 3;
                IcebergRemoteSourceTrigger trigger = new IcebergRemoteSourceTrigger(
                        remoteFileInfoSource, morParams, needToCheckEqualityIds);
                Deque<RemoteFileInfo> remoteFileInfoDeque = trigger.getQueue(morParams);
                remoteFileInfoSource = new QueueIcebergRemoteFileInfoSource(trigger, remoteFileInfoDeque);
            }
        }

        scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                remoteFileInfoSource, morParams, desc, bucketProperties, partitionIdGenerator, false,
                scanOptimizeOption.getCanUseMinMaxOpt(), usedForDelete);
        scanRangeSource.setRuntimeFilterPruningColumns(runtimeFilterPruningColumns);
    }

    // A DATETIME slot only from an Iceberg timestamp (with or without time zone; the bounds of the former are
    // widened to the session time zone conservatively), not a nanosecond one, which has no file bounds here.
    private boolean fileStatsPruningType(SlotDescriptor slot) {
        PrimitiveType type = slot.getType().getPrimitiveType();
        if (!FILE_STATS_PRUNING_TYPES.contains(type)) {
            return false;
        }
        if (type != PrimitiveType.DATETIME) {
            return true;
        }
        Types.NestedField field = icebergTable.getReadSchema().findField(slot.getColumn().getName());
        return field != null && field.type() instanceof Types.TimestampType;
    }

    // The runtime filter probe columns outside `requested` of the types the BE builds a file zone map for. None for a
    // table with equality deletes: its scans share one planning per table, keyed by the requested statistics, and the
    // equality-delete scan requests none, so asking for them would plan the table twice.
    @VisibleForTesting
    public Set<String> runtimeFilterProbeStatsCandidates(Set<String> readSchemaColumnNames, Set<String> requested) {
        if (!tableFullMORParams.isEmpty()) {
            return Set.of();
        }
        return desc.getSlots().stream()
                .filter(slot -> slot.getColumn() != null && fileStatsPruningType(slot))
                .map(slot -> slot.getColumn().getName())
                .filter(runtimeFilterProbeColumnNames::contains)
                .filter(readSchemaColumnNames::contains)
                .filter(name -> !requested.contains(name))
                .collect(Collectors.toSet());
    }

    // The runtime filter probe columns, outside `requested`, whose file statistics the manifest cache keeps. Others
    // are left out: requesting them would make every query re-read the manifests.
    private Set<String> runtimeFilterProbeStatsColumnNames(Set<String> readSchemaColumnNames, Set<String> requested) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null || !connectContext.getSessionVariable().isEnableDynamicPruneScanRange()) {
            return Set.of();
        }
        Set<String> candidates = runtimeFilterProbeStatsCandidates(readSchemaColumnNames, requested);
        if (candidates.isEmpty()) {
            return Set.of();
        }
        try {
            Set<String> kept = icebergMetadata().getIcebergCatalog().statsColumnsKeptByManifestCache(
                    icebergTable.getNativeTable(), icebergTable.getReadSchema(), candidates);
            Tracers.record(Tracers.Module.EXTERNAL, "ICEBERG.RF_STATS_COLUMNS." + icebergTable.getCatalogDBName() +
                    "." + icebergTable.getCatalogTableName(), candidates + " kept " + kept);
            return kept;
        } catch (RuntimeException e) {
            LOG.debug("Cannot tell which runtime filter columns the manifest cache of {} keeps",
                    icebergTable.getCatalogTableName(), e);
            return Set.of();
        }
    }

    private void setupCloudCredential() {
        String catalogName = icebergTable.getCatalogName();
        if (catalogName == null) {
            return;
        }

        cloudConfiguration = IcebergUtil.getVendedCloudConfiguration(catalogName, icebergTable);
        // Non-null exactly when the configuration above is the table's own vended one.
        long nowMs = System.currentTimeMillis();
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        vendedCredentials = IcebergVendedCredentials.of(nativeTable, nowMs, () -> tableLoadedAtMs(nativeTable, nowMs));
    }

    @Override
    public TCloudConfiguration refreshScanCredentials(ConnectContext context) {
        IcebergVendedCredentials current = vendedCredentials;
        if (current == null) {
            return null;
        }
        if (System.currentTimeMillis() < current.refreshAfterMs()) {
            return toThrift(current);
        }
        // Every backend of the query asks at about the same time, each on a shared FrontendService thread. Only
        // one renews; the others answer at once with the current credentials, which are still valid, and their
        // backends ask again later instead of holding a thread for the duration of the catalog call.
        if (!vendedCredentialsLock.tryLock()) {
            return toThrift(current);
        }
        try {
            current = vendedCredentials;
            if (current == null) {
                return null;
            }
            long nowMs = System.currentTimeMillis();
            if (nowMs >= current.refreshAfterMs()) {
                if (nowMs < renewalRetryAfterMs) {
                    throw new StarRocksConnectorException("Renewing the vended credentials failed moments ago: "
                            + lastRenewalFailure);
                }
                try {
                    current = IcebergVendedCredentials.renewedFrom(icebergTable.getNativeTable(),
                            loadTableUncached(context), nowMs, icebergTable.getCatalogName() + "."
                                    + icebergTable.getCatalogDBName() + "." + icebergTable.getCatalogTableName());
                } catch (RuntimeException e) {
                    // From when the call ended: a slow failure must still hold back the whole window.
                    renewalRetryAfterMs = System.currentTimeMillis() + CredentialRefreshPolicy.FAILED_RENEWAL_RETRY_MS;
                    lastRenewalFailure = e.getMessage();
                    throw e;
                }
                renewalRetryAfterMs = 0;
                lastRenewalFailure = null;
                vendedCredentials = current;
                cloudConfiguration = current.cloudConfiguration();
                LOG.info("Renewed vended credentials of Iceberg table {}.{}.{} for scan node {},"
                                + " next refresh after {}", icebergTable.getCatalogName(),
                        icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), getId().asInt(),
                        current.refreshAfterMs());
            }
            return toThrift(current);
        } finally {
            vendedCredentialsLock.unlock();
        }
    }

    // Loads the table again past every table cache, with the running query's own context so a per-user token is
    // used the same way planning used it.
    @VisibleForTesting
    org.apache.iceberg.Table loadTableUncached(ConnectContext context) {
        return icebergMetadata().getIcebergCatalog().getTableUncached(context, icebergTable.getCatalogDBName(),
                icebergTable.getCatalogTableName());
    }

    // When the planned table was loaded from the catalog, for credentials that do not say when they expire. Unknown
    // on any failure: the refresh time then only comes sooner.
    @VisibleForTesting
    OptionalLong tableLoadedAtMs(org.apache.iceberg.Table nativeTable, long nowMs) {
        try {
            return icebergMetadata().getIcebergCatalog().tableLoadedAtMs(ConnectContext.get(),
                    icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), nativeTable, nowMs);
        } catch (RuntimeException e) {
            LOG.debug("Cannot tell when Iceberg table {}.{}.{} was loaded", icebergTable.getCatalogName(),
                    icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), e);
            return OptionalLong.empty();
        }
    }

    private IcebergMetadata icebergMetadata() {
        String catalogName = icebergTable.getCatalogName();
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
        if (connector == null) {
            throw new StarRocksConnectorException("Catalog " + catalogName + " no longer exists");
        }
        ConnectorMetadata metadata = connector.getMetadata();
        while (metadata instanceof DelegatingConnectorMetadata delegating) {
            ConnectorMetadata next = delegating.delegateFor(icebergTable);
            if (next == metadata) {
                break;
            }
            metadata = next;
        }
        if (!(metadata instanceof IcebergMetadata icebergMetadata)) {
            throw new StarRocksConnectorException("Catalog " + catalogName + " is not an Iceberg catalog");
        }
        return icebergMetadata;
    }

    private TCloudConfiguration toThrift(IcebergVendedCredentials credentials) {
        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        credentials.cloudConfiguration().toThrift(tCloudConfiguration);
        tCloudConfiguration.setRefresh_after_ms(credentials.refreshAfterMs());
        tCloudConfiguration.setRefresh_token(credentialRefreshToken());
        return tCloudConfiguration;
    }

    public void setCloudConfiguration(CloudConfiguration cloudConfiguration) {
        this.cloudConfiguration = cloudConfiguration;
        // An explicitly set configuration replaces the vended one and is not refreshed.
        this.vendedCredentials = null;
    }

    public void setUsedForDelete(boolean usedForDelete) {
        this.usedForDelete = usedForDelete;
    }

    public boolean isUsedForDelete() {
        return usedForDelete;
    }

    public void setRuntimeFilterProbeColumnNames(Set<String> runtimeFilterProbeColumnNames) {
        this.runtimeFilterProbeColumnNames = runtimeFilterProbeColumnNames;
    }

    @VisibleForTesting
    public Set<String> getRuntimeFilterProbeColumnNames() {
        return runtimeFilterProbeColumnNames;
    }

    public void setRuntimeFilterPruningColumns(Set<String> runtimeFilterPruningColumns) {
        this.runtimeFilterPruningColumns = runtimeFilterPruningColumns;
    }

    // Whether a runtime filter on the data column matching this slot of an equality delete scan may drop delete rows:
    // the slot holds the identity partition value of each delete file, see IcebergEqualityDeleteRewriteRule.
    public boolean isRuntimeFilterPruningSlot(SlotId slotId) {
        SlotDescriptor slot = desc.getSlot(slotId.asInt());
        return slot != null && slot.getColumn() != null &&
                runtimeFilterPruningColumns.contains(slot.getColumn().getName());
    }

    public void preProcessIcebergPredicate(ScalarOperator predicate) {
        this.icebergJobPlanningPredicate = predicate;
    }

    public IcebergTable getIcebergTable() {
        return icebergTable;
    }

    // for unit tests
    public ScalarOperator getIcebergJobPlanningPredicate() {
        return icebergJobPlanningPredicate;
    }

    // for unit tests
    public List<Integer> getExtendedColumnSlotIds() {
        return scanRangeSource.getExtendedColumnSlotIds();
    }

    public Set<String> getSeenEqualityDeleteFiles() {
        return scanRangeSource.getSeenEqDeleteFiles();
    }

    public IcebergMORParams getMORParams() {
        return morParams;
    }

    // for unit tests
    public IcebergTableMORParams getTableFullMORParams() {
        return tableFullMORParams;
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    public void setBucketProperties(List<BucketProperty> bucketProperties) {
        this.bucketProperties = Optional.of(bucketProperties);
    }

    @Override
    public Optional<List<BucketProperty>> getBucketProperties() {
        return this.bucketProperties;
    }

    @Override
    public int getBucketNums() {
        if (bucketProperties.isEmpty()) {
            throw new StarRocksConnectorException("Error when using bucket-aware execution for table: "
                    + icebergTable.getName());
        }

        List<Integer> bucketNums = this.bucketProperties.get().stream().map(
                BucketProperty::getBucketNum).toList();
        int res = bucketNums.get(0) + 1;
        for (int i = 1; i < bucketNums.size(); i++) {
            res = res * (bucketNums.get(i) + 1);
        }
        return res;
    }

    public void setEnableGlobalLateMaterialization(boolean enableGlobalLateMaterialization) {
        this.enableGlobalLateMaterialization = enableGlobalLateMaterialization;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.add("icebergTable=", icebergTable.getName());
        return helper.toString();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ")
                .append(icebergTable.getCatalogDBName())
                .append(".")
                .append(icebergTable.getName())
                .append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    explainExpr(conjuncts)).append("\n");
        }
        if (!scanNodePredicates.getMinMaxConjuncts().isEmpty()) {
            output.append(prefix).append("MIN/MAX PREDICATES: ").append(
                    explainExpr(scanNodePredicates.getMinMaxConjuncts())).append("\n");
        }
        if (tvrVersionRange != null) {
            output.append(prefix).append("TABLE VERSION: ").append(
                    tvrVersionRange.toString()).append("\n");
        }

        output.append(prefix).append(String.format("cardinality=%s", cardinality));
        output.append("\n");

        output.append(prefix).append(String.format("avgRowSize=%s", avgRowSize));
        output.append("\n");

        if (detailLevel == TExplainLevel.VERBOSE) {
            HdfsScanNode.appendDataCacheOptionsInExplain(output, prefix, dataCacheOptions);
            // for global dict
            output.append(explainColumnDict(prefix));
            output.append(explainColumnAccessPath(prefix));
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                Type type = slotDescriptor.getOriginType();
                if (type.isComplexType() || type.isVariantType()) {
                    output.append(prefix)
                            .append(String.format("Pruned type: %d <-> [%s]\n", slotDescriptor.getId().asInt(), type));
                }
            }
        }

        if (detailLevel == TExplainLevel.VERBOSE && !isResourceMappingCatalog(icebergTable.getCatalogName())) {
            appendPartitionsExplain(output, prefix);

            //record scan metrics, should cosume all scan ranges too.
            //print scan metrics
            if ((icebergScanMetricsReporter != null) && (icebergScanMetricsReporter.getScanReport() != null)) {
                output.append(prefix).append("Iceberg Scan Metrics: \n");
                output.append(prefix).append("\t").append(
                        ScanReportParser.toJson(icebergScanMetricsReporter.getScanReport())).append("\n");
            }
        }

        if (morParams.getScanTaskType() == IcebergMORParams.ScanTaskType.EQ_DELETE) {
            List<String> identifierColumnNames = morParams.getEqualityIds().stream()
                    .map(id -> icebergTable.getNativeTable().schema().findColumnName(id))
                    .collect(Collectors.toList());
            output.append(prefix).append("Iceberg identifier columns: ").append(identifierColumnNames);
            output.append("\n");
            if (!runtimeFilterPruningColumns.isEmpty()) {
                output.append(prefix).append("Runtime filter pruning columns: ")
                        .append(runtimeFilterPruningColumns.stream().sorted().toList()).append("\n");
            }
        }

        return output.toString();
    }

    // Lists every partition of the table and may plan all scan ranges. Skipped when the finished query's plan is
    // rendered into its profile: nothing bounds that metadata read once the query has ended.
    private void appendPartitionsExplain(StringBuilder output, String prefix) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.isExplainForProfile()) {
            return;
        }
        ConnectorMetadataRequestContext requestContext = new ConnectorMetadataRequestContext();
        requestContext.setTableVersionRange(tvrVersionRange);
        List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                icebergTable.getCatalogName(), icebergTable.getCatalogDBName(),
                icebergTable.getCatalogTableName(), requestContext);
        if (selectedPartitionCount == -1) {
            if (scanRangeSource != null) {
                // we have to consume all scan ranges to know how many partition been selected.
                while (scanRangeSource.hasMoreOutput()) {
                    scanRangeSource.getOutputs(1000);
                }
                selectedPartitionCount = scanRangeSource.selectedPartitionCount();
            } else {
                selectedPartitionCount = 0;
            }
        }

        output.append(prefix).append(
                String.format("partitions=%s/%s", selectedPartitionCount,
                        partitionNames.isEmpty() ? 1 : partitionNames.size()));
        output.append("\n");
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        msg.hdfs_scan_node = tHdfsScanNode;

        if (enableGlobalLateMaterialization) {
            msg.hdfs_scan_node.setEnable_global_late_materialization(true);
        }

        String sqlPredicates = getExplainString(conjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicates);
        if (scanRangeSource != null) {
            msg.hdfs_scan_node.setExtended_slot_ids(scanRangeSource.getExtendedColumnSlotIds());
        }
        msg.hdfs_scan_node.setTable_name(icebergTable.getName());
        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        IcebergVendedCredentials vended = vendedCredentials;
        if (vended != null) {
            tHdfsScanNode.setCloud_configuration(toThrift(vended));
        } else {
            HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        }
        HdfsScanNode.setMinMaxConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setDataCacheOptionsToThrift(tHdfsScanNode, dataCacheOptions);
        if (columnAccessPaths != null && !columnAccessPaths.isEmpty()) {
            tHdfsScanNode.setColumn_access_paths(columnAccessPathToThrift());
        }
        bucketProperties.ifPresent(properties -> HdfsScanNode.setBucketProperties(tHdfsScanNode, properties));

        setConnectorCatalogType(msg);
    }

    @Override
    public void prepareRetry() {
        clear();
        reachLimit = false;
        try {
            setupScanRangeLocations(enableIncrementalScanRanges);
        } catch (StarRocksException e) {
            throw new RuntimeException("Failed to reset IcebergScanNode for retry", e);
        }
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    protected boolean supportTopNRuntimeFilter() {
        return true;
    }

    @Override
    public void setScanSampleStrategy(RemoteFilesSampleStrategy strategy) {
        scanRangeSource.setSampleStrategy(strategy);
    }

    public Set<DataFile> getScannedDataFiles() {
        return scanRangeSource.getScannedDataFiles();
    }

    public Set<DeleteFile> getPosAppliedDeleteFiles() {
        return scanRangeSource.getPosAppliedDeleteFiles();
    }

    public Set<DeleteFile> getEqualAppliedDeleteFiles() {
        return scanRangeSource.getEqualAppliedDeleteFiles();
    }

    public Optional<Long> getBaseSnapshotId() {
        if (tvrVersionRange == null) {
            return Optional.empty();
        }
        return tvrVersionRange.end();
    }
}
