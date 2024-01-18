// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/OlapScanNode.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TLakeScanNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TNormalOlapScanNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TOlapScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OlapScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(OlapScanNode.class);

    private final List<TScanRangeLocations> result = new ArrayList<>();
    /*
     * When the field value is ON, the storage engine can return the data directly without pre-aggregation.
     * When the field value is OFF, the storage engine needs to aggregate the data before returning to scan node.
     * For example:
     * Aggregate table: k1, k2, v1 sum
     * Field value is ON
     * Query1: select k1, sum(v1) from table group by k1
     * This aggregation function in query is same as the schema.
     * So the field value is ON while the query can scan data directly.
     *
     * Field value is OFF
     * Query1: select k1 , k2 from table
     * This aggregation info is null.
     * Query2: select k1, min(v1) from table group by k1
     * This aggregation function in query is min which different from the schema.
     * So the data stored in storage engine need to be merged firstly before returning to scan node.
     */
    private boolean isPreAggregation = false;
    private String reasonOfPreAggregation = null;
    private OlapTable olapTable = null;
    private long selectedTabletsNum = 0;
    private long totalTabletsNum = 0;
    private long selectedIndexId = -1;
    private int selectedPartitionNum = 0;
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();
    private final Collection<String> selectedPartitionNames = Lists.newArrayList();
    private final Collection<Long> selectedPartitionVersions = Lists.newArrayList();
    private long actualRows = 0;

    // List of tablets will be scanned by current olap_scan_node
    private ArrayList<Long> scanTabletIds = Lists.newArrayList();
    private boolean isFinalized = false;

    private boolean isSortedByKeyPerTablet = false;

    private final HashSet<Long> scanBackendIds = new HashSet<>();

    private Map<Long, Integer> tabletId2BucketSeq = Maps.newHashMap();
    // a bucket seq may map to many tablets, and each tablet has a TScanRangeLocations.
    public ArrayListMultimap<Integer, TScanRangeLocations> bucketSeq2locations = ArrayListMultimap.create();

    private List<Expr> bucketExprs = Lists.newArrayList();
    private List<ColumnRefOperator> bucketColumns = Lists.newArrayList();

    // record the selected partition with the selected tablets belong to it
    private Map<Long, List<Long>> partitionToScanTabletMap;

    private boolean usePkIndex = false;

    // Constructs node to scan given data files of table 'tbl'.
    public OlapScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        olapTable = (OlapTable) desc.getTable();
    }

    public void setIsPreAggregation(boolean isPreAggregation, String reason) {
        this.isPreAggregation = isPreAggregation;
        this.reasonOfPreAggregation = reason;
    }

    public ArrayList<Long> getScanTabletIds() {
        return scanTabletIds;
    }

    public boolean isPreAggregation() {
        return isPreAggregation;
    }

    public void setCanTurnOnPreAggr(boolean canChangePreAggr) {
    }

    public void setIsSortedByKeyPerTablet(boolean isSortedByKeyPerTablet) {
        this.isSortedByKeyPerTablet = isSortedByKeyPerTablet;
    }

    public Collection<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
    }

    public Collection<String> getSelectedPartitionNames() {
        return selectedPartitionNames;
    }

    public Collection<Long> getSelectedPartitionVersions() {
        return selectedPartitionVersions;
    }

    // The dict id int column ids to dict string column ids
    private Map<Integer, Integer> dictStringIdToIntIds = Maps.newHashMap();

    public void setDictStringIdToIntIds(Map<Integer, Integer> dictStringIdToIntIds) {
        this.dictStringIdToIntIds = dictStringIdToIntIds;
    }

    public long getActualRows() {
        return actualRows;
    }

    public List<ColumnRefOperator> getBucketColumns() {
        return bucketColumns;
    }

    // TODO: Determine local shuffle keys by FE to make local shuffle use bucket columns of OlapScanNode.
    public void setBucketColumns(List<ColumnRefOperator> bucketColumns) {
        this.bucketColumns = bucketColumns;
    }

    public List<Expr> getBucketExprs() {
        return bucketExprs;
    }

    public void setBucketExprs(List<Expr> bucketExprs) {
        this.bucketExprs = bucketExprs;
    }

    // The column names applied dict optimization
    // used for explain
    private final List<String> appliedDictStringColumns = new ArrayList<>();

    public void updateAppliedDictStringColumns(Set<Integer> appliedColumnIds) {
        for (SlotDescriptor slot : desc.getSlots()) {
            if (appliedColumnIds.contains(slot.getId().asInt())) {
                appliedDictStringColumns.add(slot.getColumn().getName());
            }
        }
    }

    private final List<String> unUsedOutputStringColumns = new ArrayList<>();

    public void setUnUsedOutputStringColumns(Set<Integer> unUsedOutputColumnIds,
                                             Set<String> aggOrPrimaryKeyTableValueColumnNames) {
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            if (unUsedOutputColumnIds.contains(slot.getId().asInt()) &&
                    !aggOrPrimaryKeyTableValueColumnNames.contains(slot.getColumn().getName())) {
                unUsedOutputStringColumns.add(slot.getColumn().getName());
            }
        }
    }

    public OlapTable getOlapTable() {
        return olapTable;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("olapTable=" + olapTable.getName());
        return helper.toString();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        computePartitionInfo();
    }

    @Override
    public void finalizeStats(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }

        LOG.debug("OlapScanNode finalize. Tuple: {}", desc);
        try {
            getScanRangeLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }

        computeStats(analyzer);
        isFinalized = true;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        if (cardinality > 0) {
            long totalBytes = 0;
            avgRowSize = totalBytes / (float) cardinality;
            if (hasLimit()) {
                cardinality = Math.min(cardinality, limit);
            }
            numNodes = scanBackendIds.size();
        }
        // even current node scan has no data,at least on backend will be assigned when the fragment actually execute
        numNodes = numNodes <= 0 ? 1 : numNodes;
        // when node scan has no data, cardinality should be 0 instead of a invalid value after computeStats()
        cardinality = cardinality == -1 ? 0 : cardinality;
    }

    private Collection<Long> partitionPrune(RangePartitionInfo partitionInfo, PartitionNames partitionNames)
            throws AnalysisException {
        Map<Long, Range<PartitionKey>> keyRangeById = null;
        if (partitionNames != null) {
            keyRangeById = Maps.newHashMap();
            for (String partName : partitionNames.getPartitionNames()) {
                Partition part = olapTable.getPartition(partName, partitionNames.isTemp());
                if (part == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_SUCH_PARTITION, partName);
                }
                keyRangeById.put(part.getId(), partitionInfo.getRange(part.getId()));
            }
        } else {
            keyRangeById = partitionInfo.getIdToRange(false);
        }
        PartitionPruner partitionPruner = new RangePartitionPruner(keyRangeById,
                partitionInfo.getPartitionColumns(), columnFilters);
        return partitionPruner.prune();
    }

    private Collection<Long> distributionPrune(
            MaterializedIndex table,
            DistributionInfo distributionInfo) throws AnalysisException {
        DistributionPruner distributionPruner;
        if (DistributionInfo.DistributionInfoType.HASH == distributionInfo.getType()) {
            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
            distributionPruner = new HashDistributionPruner(table.getTabletIdsInOrder(),
                    info.getDistributionColumns(),
                    columnFilters,
                    info.getBucketNum());
            return distributionPruner.prune();
        } else {
            return null;
        }
    }

    public void addScanRangeLocations(Partition partition,
                                      MaterializedIndex index,
                                      List<Tablet> tablets,
                                      long localBeId) throws UserException {
        boolean enableQueryTabletAffinity =
                ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isEnableQueryTabletAffinity();
        int logNum = 0;
        int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
        String schemaHashStr = String.valueOf(schemaHash);
        long visibleVersion = partition.getVisibleVersion();
        String visibleVersionStr = String.valueOf(visibleVersion);
        selectedPartitionNames.add(partition.getName());
        selectedPartitionVersions.add(visibleVersion);
        for (Tablet tablet : tablets) {
            long tabletId = tablet.getId();
            LOG.debug("{} tabletId={}", (logNum++), tabletId);
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

            TInternalScanRange internalRange = new TInternalScanRange();
            internalRange.setDb_name("");
            internalRange.setSchema_hash(schemaHashStr);
            internalRange.setVersion(visibleVersionStr);
            internalRange.setVersion_hash("0");
            internalRange.setTablet_id(tabletId);
            internalRange.setPartition_id(partition.getId());

            // random shuffle List && only collect one copy
            List<Replica> allQueryableReplicas = Lists.newArrayList();
            List<Replica> localReplicas = Lists.newArrayList();
            tablet.getQueryableReplicas(allQueryableReplicas, localReplicas,
                    visibleVersion, localBeId, schemaHash);
            if (allQueryableReplicas.isEmpty()) {
                String replicaInfos = "";
                if (tablet instanceof LocalTablet) {
                    replicaInfos = ((LocalTablet) tablet).getReplicaInfos();
                }
                LOG.error("no queryable replica found in tablet {}. visible version {} replicas:{}",
                        tabletId, visibleVersion, replicaInfos);
                if (LOG.isDebugEnabled()) {
                    if (olapTable.isLakeTable()) {
                        LOG.debug("tablet: {}, shard: {}, backends: {}", tabletId, ((LakeTablet) tablet).getShardId(),
                                tablet.getBackendIds());
                    } else {
                        for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                            LOG.debug("tablet {}, replica: {}", tabletId, replica.toString());
                        }
                    }
                }
                throw new UserException(
                        "Failed to get scan range, no queryable replica found in tablet: " + tabletId + " " +
                                replicaInfos);
            }

            List<Replica> replicas = null;
            if (!localReplicas.isEmpty()) {
                replicas = localReplicas;
            } else {
                replicas = allQueryableReplicas;
            }
            // TODO: Implement a more robust strategy for tablet affinity.
            if (!enableQueryTabletAffinity) {
                Collections.shuffle(replicas);
            }
            boolean tabletIsNull = true;
            boolean collectedStat = false;
            for (Replica replica : replicas) {
                Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(replica.getBackendId());
                if (backend == null) {
                    LOG.debug("replica {} not exists", replica.getBackendId());
                    continue;
                }
                String ip = backend.getHost();
                int port = backend.getBePort();
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(ip, port));
                scanRangeLocation.setBackend_id(replica.getBackendId());
                scanRangeLocations.addToLocations(scanRangeLocation);
                internalRange.addToHosts(new TNetworkAddress(ip, port));
                tabletIsNull = false;

                //for CBO
                if (!collectedStat && replica.getRowCount() != -1) {
                    actualRows += replica.getRowCount();
                    collectedStat = true;
                }
                scanBackendIds.add(backend.getId());
            }
            if (tabletIsNull) {
                throw new UserException(tabletId + "have no alive replicas");
            }
            TScanRange scanRange = new TScanRange();
            scanRange.setInternal_scan_range(internalRange);
            scanRangeLocations.setScan_range(scanRange);

            bucketSeq2locations.put(tabletId2BucketSeq.get(tabletId), scanRangeLocations);

            result.add(scanRangeLocations);
        }
    }

    private void computePartitionInfo() throws AnalysisException {
        long start = System.currentTimeMillis();
        // Step1: compute partition ids
        PartitionNames partitionNames = desc.getRef().getPartitionNames();
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.getType() == PartitionType.RANGE) {
            selectedPartitionIds = partitionPrune((RangePartitionInfo) partitionInfo, partitionNames);
        } else {
            selectedPartitionIds = null;
        }
        if (selectedPartitionIds == null) {
            selectedPartitionIds = Lists.newArrayList();
            for (Partition partition : olapTable.getPartitions()) {
                if (!partition.hasData()) {
                    continue;
                }
                selectedPartitionIds.add(partition.getId());
            }
        } else {
            selectedPartitionIds = selectedPartitionIds.stream()
                    .filter(id -> olapTable.getPartition(id).hasData())
                    .collect(Collectors.toList());
        }
        selectedPartitionNum = selectedPartitionIds.size();
        LOG.debug("partition prune cost: {} ms, partitions: {}",
                (System.currentTimeMillis() - start), selectedPartitionIds);
    }

    public void selectBestRollupByRollupSelector() {
        selectedIndexId = olapTable.getBaseIndexId();
    }

    private void getScanRangeLocations() throws UserException {
        if (selectedPartitionIds.size() == 0) {
            return;
        }
        Preconditions.checkState(selectedIndexId != -1);
        // compute tablet info by selected index id and selected partition ids
        long start = System.currentTimeMillis();
        computeTabletInfo();
        LOG.debug("distribution prune cost: {} ms", (System.currentTimeMillis() - start));
    }

    private void computeTabletInfo() throws UserException {
        long localBeId = -1;
        if (Config.enable_local_replica_selection) {
            localBeId = GlobalStateMgr.getCurrentSystemInfo().getBackendIdByHost(FrontendOptions.getLocalHostAddress());
        }
        /**
         * The tablet info could be computed only once.
         * So the scanBackendIds should be empty in the beginning.
         */
        Preconditions.checkState(scanBackendIds.size() == 0);
        Preconditions.checkState(scanTabletIds.size() == 0);
        for (Long partitionId : selectedPartitionIds) {
            final Partition partition = olapTable.getPartition(partitionId);
            final MaterializedIndex selectedTable = partition.getIndex(selectedIndexId);
            final List<Tablet> tablets = Lists.newArrayList();
            final Collection<Long> tabletIds = distributionPrune(selectedTable, partition.getDistributionInfo());
            LOG.debug("distribution prune tablets: {}", tabletIds);

            List<Long> allTabletIds = selectedTable.getTabletIdsInOrder();
            if (tabletIds != null) {
                for (Long id : tabletIds) {
                    tablets.add(selectedTable.getTablet(id));
                }
                scanTabletIds.addAll(tabletIds);
            } else {
                tablets.addAll(selectedTable.getTablets());
                scanTabletIds.addAll(allTabletIds);
            }

            for (int i = 0; i < allTabletIds.size(); i++) {
                tabletId2BucketSeq.put(allTabletIds.get(i), i);
            }

            totalTabletsNum += selectedTable.getTablets().size();
            selectedTabletsNum += tablets.size();
            addScanRangeLocations(partition, selectedTable, tablets, localBeId);
        }
    }

    /**
     * We query meta to get request's data location
     * extra result info will pass to backend ScanNode
     */
    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        // TODO: unify them
        if (detailLevel != TExplainLevel.VERBOSE) {
            output.append(prefix).append("TABLE: ").append(olapTable.getName()).append("\n");
        } else {
            output.append(prefix).append("table: ").append(olapTable.getName())
                    .append(", ").append("rollup: ")
                    .append(olapTable.getIndexNameById(selectedIndexId)).append("\n");
        }

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }

        if (detailLevel != TExplainLevel.VERBOSE) {
            if (isPreAggregation) {
                output.append(prefix).append("PREAGGREGATION: ON").append("\n");
            } else {
                output.append(prefix).append("PREAGGREGATION: OFF. Reason: ").append(reasonOfPreAggregation)
                        .append("\n");
            }
            if (!conjuncts.isEmpty()) {
                output.append(prefix).append("PREDICATES: ").append(
                        getExplainString(conjuncts)).append("\n");
            }
        } else {
            if (isPreAggregation) {
                output.append(prefix).append("preAggregation: on").append("\n");
            } else {
                output.append(prefix).append("preAggregation: off. Reason: ").append(reasonOfPreAggregation)
                        .append("\n");
            }
            if (!conjuncts.isEmpty()) {
                output.append(prefix).append("Predicates: ").append(getVerboseExplain(conjuncts)).append("\n");
            }
            if (!dictStringIdToIntIds.isEmpty()) {
                List<String> flatDictList = dictStringIdToIntIds.entrySet().stream().limit(5)
                        .map((entry) -> "(" + entry.getKey() + "," + entry.getValue() + ")")
                        .collect(Collectors.toList());
                String format_template = "dictStringIdToIntIds=%s";
                if (dictStringIdToIntIds.size() > 5) {
                    format_template = format_template + "...";
                }
                output.append(prefix).append(String.format(format_template, Joiner.on(",").join(flatDictList)));
                output.append("\n");
            }

            if (!appliedDictStringColumns.isEmpty()) {
                int maxSize = Math.min(appliedDictStringColumns.size(), 5);
                List<String> printList = appliedDictStringColumns.subList(0, maxSize);
                String format_template = "dict_col=%s";
                if (dictStringIdToIntIds.size() > 5) {
                    format_template = format_template + "...";
                }
                output.append(prefix).append(String.format(format_template, Joiner.on(",").join(printList)));
                output.append("\n");
            }
        }

        if (detailLevel != TExplainLevel.VERBOSE) {
            output.append(prefix).append(String.format("partitions=%s/%s\n", selectedPartitionNum,
                    olapTable.getPartitions().size()));

            String indexName = olapTable.getIndexNameById(selectedIndexId);
            output.append(prefix).append(String.format("rollup: %s\n", indexName));

            output.append(prefix).append(String.format("tabletRatio=%s/%s\n", selectedTabletsNum, totalTabletsNum));

            // We print up to 10 tablet, and we print "..." if the number is more than 10
            if (scanTabletIds.size() > 10) {
                List<Long> firstTenTabletIds = scanTabletIds.subList(0, 10);
                output.append(prefix)
                        .append(String.format("tabletList=%s ...", Joiner.on(",").join(firstTenTabletIds)));
            } else {
                output.append(prefix).append(String.format("tabletList=%s", Joiner.on(",").join(scanTabletIds)));
            }

            output.append("\n");
            output.append(prefix).append(String.format("cardinality=%s\n", cardinality));
            output.append(prefix).append(String.format("avgRowSize=%s\n", avgRowSize));
            output.append(prefix).append(String.format("numNodes=%s\n", numNodes));
        } else {
            output.append(prefix).append(String.format(
                            "partitionsRatio=%s/%s",
                            selectedPartitionNum,
                            olapTable.getPartitions().size())).append(", ")
                    .append(String.format("tabletsRatio=%s/%s", selectedTabletsNum, totalTabletsNum)).append("\n");

            if (scanTabletIds.size() > 10) {
                List<Long> firstTenTabletIds = scanTabletIds.subList(0, 10);
                output.append(prefix)
                        .append(String.format("tabletList=%s ...", Joiner.on(",").join(firstTenTabletIds)));
            } else {
                output.append(prefix).append(String.format("tabletList=%s", Joiner.on(",").join(scanTabletIds)));
            }
            output.append("\n");

            output.append(prefix).append(String.format("actualRows=%s", actualRows))
                    .append(", ").append(String.format("avgRowSize=%s\n", avgRowSize));
        }

        if (detailLevel == TExplainLevel.VERBOSE) {
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                Type type = slotDescriptor.getOriginType();
                if (type.isComplexType()) {
                    output.append(prefix)
                            .append(String.format("Pruned type: %d <-> [%s]\n", slotDescriptor.getId().asInt(), type));
                }
            }

            if (!bucketColumns.isEmpty() && FeConstants.showLocalShuffleColumnsInExplain) {
                output.append(prefix).append("LocalShuffleColumns:\n");
                for (ColumnRefOperator col : bucketColumns) {
                    output.append(prefix).append("- ").append(col.toString()).append("\n");
                }
            }
        }

        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return result.size();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        List<String> keyColumnNames = new ArrayList<String>();
        List<TPrimitiveType> keyColumnTypes = new ArrayList<TPrimitiveType>();
        if (selectedIndexId != -1) {
            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(selectedIndexId);
<<<<<<< HEAD
            if (KeysType.PRIMARY_KEYS == olapTable.getKeysType() && indexMeta.getSortKeyIdxes() != null) {
                for (Integer sortKeyIdx : indexMeta.getSortKeyIdxes()) {
                    Column col = indexMeta.getSchema().get(sortKeyIdx);
                    keyColumnNames.add(col.getName());
                    keyColumnTypes.add(col.getPrimitiveType().toThrift());
                }
            } else {
                for (Column col : olapTable.getSchemaByIndexId(selectedIndexId)) {
                    if (!col.isKey()) {
                        break;
                    }
                    keyColumnNames.add(col.getName());
                    keyColumnTypes.add(col.getPrimitiveType().toThrift());
=======
            if (indexMeta != null) {
                if (KeysType.PRIMARY_KEYS == olapTable.getKeysType() && indexMeta.getSortKeyIdxes() != null) {
                    for (Integer sortKeyIdx : indexMeta.getSortKeyIdxes()) {
                        Column col = indexMeta.getSchema().get(sortKeyIdx);
                        keyColumnNames.add(col.getName());
                        keyColumnTypes.add(col.getPrimitiveType().toThrift());
                    }
                } else {
                    for (Column col : olapTable.getSchemaByIndexId(selectedIndexId)) {
                        if (!col.isKey()) {
                            break;
                        }
                        keyColumnNames.add(col.getName());
                        keyColumnTypes.add(col.getPrimitiveType().toThrift());
                    }
>>>>>>> 2.5.18
                }
            }
        }

        if (olapTable.isLakeTable()) {
            msg.node_type = TPlanNodeType.LAKE_SCAN_NODE;
            msg.lake_scan_node =
                    new TLakeScanNode(desc.getId().asInt(), keyColumnNames, keyColumnTypes, isPreAggregation);
            msg.olap_scan_node.setSort_key_column_names(keyColumnNames);
            msg.lake_scan_node.setRollup_name(olapTable.getIndexNameById(selectedIndexId));
            if (!conjuncts.isEmpty()) {
                msg.lake_scan_node.setSql_predicates(getExplainString(conjuncts));
            }
            if (null != sortColumn) {
                msg.lake_scan_node.setSort_column(sortColumn);
            }
            if (ConnectContext.get() != null) {
                msg.lake_scan_node.setEnable_column_expr_predicate(
                        ConnectContext.get().getSessionVariable().isEnableColumnExprPredicate());
            }
            msg.lake_scan_node.setDict_string_id_to_int_ids(dictStringIdToIntIds);

            if (!olapTable.hasDelete()) {
                msg.lake_scan_node.setUnused_output_column_name(unUsedOutputStringColumns);
            }
        } else {
            msg.node_type = TPlanNodeType.OLAP_SCAN_NODE;
            msg.olap_scan_node =
                    new TOlapScanNode(desc.getId().asInt(), keyColumnNames, keyColumnTypes, isPreAggregation);
            msg.olap_scan_node.setSort_key_column_names(keyColumnNames);
            msg.olap_scan_node.setRollup_name(olapTable.getIndexNameById(selectedIndexId));
            if (!conjuncts.isEmpty()) {
                msg.olap_scan_node.setSql_predicates(getExplainString(conjuncts));
            }
            if (null != sortColumn) {
                msg.olap_scan_node.setSort_column(sortColumn);
            }
            if (ConnectContext.get() != null) {
                msg.olap_scan_node.setEnable_column_expr_predicate(
                        ConnectContext.get().getSessionVariable().isEnableColumnExprPredicate());
                msg.olap_scan_node.setMax_parallel_scan_instance_num(
                        ConnectContext.get().getSessionVariable().getMaxParallelScanInstanceNum());
            }
            msg.olap_scan_node.setDict_string_id_to_int_ids(dictStringIdToIntIds);

            if (!olapTable.hasDelete()) {
                msg.olap_scan_node.setUnused_output_column_name(unUsedOutputStringColumns);
            }
            msg.olap_scan_node.setSorted_by_keys_per_tablet(isSortedByKeyPerTablet);

            if (!bucketExprs.isEmpty()) {
                msg.olap_scan_node.setBucket_exprs(Expr.treesToThrift(bucketExprs));
            }
            msg.olap_scan_node.setUse_pk_index(usePkIndex);
        }
    }

    // export some tablets
    public static OlapScanNode createOlapScanNodeByLocation(
            PlanNodeId id, TupleDescriptor desc, String planNodeName, List<TScanRangeLocations> locationsList) {
        OlapScanNode olapScanNode = new OlapScanNode(id, desc, planNodeName);
        olapScanNode.numInstances = 1;
        olapScanNode.selectedIndexId = olapScanNode.olapTable.getBaseIndexId();
        olapScanNode.selectedPartitionNum = 1;
        olapScanNode.selectedTabletsNum = 1;
        olapScanNode.totalTabletsNum = 1;
        olapScanNode.isPreAggregation = false;
        olapScanNode.isFinalized = true;
        olapScanNode.result.addAll(locationsList);

        return olapScanNode;
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
    }

    /**
     * Below function is added by new analyzer
     */
    public void updateScanInfo(List<Long> selectedPartitionIds,
                               List<Long> scanTabletIds,
                               long selectedIndexId) {
        this.scanTabletIds = new ArrayList<>(scanTabletIds);
        this.selectedTabletsNum = scanTabletIds.size();
        this.selectedPartitionIds = selectedPartitionIds;
        this.selectedPartitionNum = selectedPartitionIds.size();
        this.selectedIndexId = selectedIndexId;
        this.partitionToScanTabletMap = mapTabletsToPartitions();

        // FixMe(kks): For DUPLICATE table, isPreAggregation could always true
        this.isPreAggregation = true;
    }

    public void setSelectedPartitionIds(Collection<Long> selectedPartitionIds) {
        this.selectedPartitionIds = selectedPartitionIds;
    }

    public void setTabletId2BucketSeq(Map<Long, Integer> tabletId2BucketSeq) {
        this.tabletId2BucketSeq = tabletId2BucketSeq;
    }

    public void setTotalTabletsNum(long totalTabletsNum) {
        this.totalTabletsNum = totalTabletsNum;
    }

    public void setUsePkIndex(boolean usePkIndex) {
        this.usePkIndex = usePkIndex;
    }

    @Override
    public boolean canDoReplicatedJoin() {
        // TODO(wyb): necessary to support?
        if (olapTable.isLakeTable()) {
            return false;
        }
        ConnectContext ctx = ConnectContext.get();
        int backendSize = ctx.getTotalBackendNumber();
        int aliveBackendSize = ctx.getAliveBackendNumber();
        int schemaHash = olapTable.getSchemaHashByIndexId(selectedIndexId);
        for (Map.Entry<Long, List<Long>> entry : partitionToScanTabletMap.entrySet()) {
            long partitionId = entry.getKey();
            Partition partition = olapTable.getPartition(entry.getKey());
            if (olapTable.getPartitionInfo().getReplicationNum(partitionId) < backendSize) {
                return false;
            }
            long visibleVersion = partition.getVisibleVersion();
            MaterializedIndex materializedIndex = partition.getIndex(selectedIndexId);
            for (Long id : entry.getValue()) {
                LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(id);
                if (tablet.getQueryableReplicasSize(visibleVersion, schemaHash) != aliveBackendSize) {
                    return false;
                }
            }

        }
        return true;
    }

    @VisibleForTesting
    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public Map<Long, List<Long>> getPartitionToScanTabletMap() {
        return partitionToScanTabletMap;
    }

    private Set<Long> getHotPartitionIds(RangePartitionInfo partitionInfo) {
        KeysType keysType = olapTable.getKeysType();
        ConnectContext connectContext = ConnectContext.get();
        Set<Long> hotIds = Sets.newHashSet();
        if (connectContext == null) {
            return hotIds;
        }
        List<Map.Entry<Long, Range<PartitionKey>>> partitions = partitionInfo.getSortedRangeMap(false);
        int numHotIds = 0;
        int numPartitions = partitions.size();
        if (!canUseMultiVersionCache()) {
            int pkHotNum = connectContext.getSessionVariable().getQueryCacheHotPartitionNum();
            numHotIds = Math.min(numPartitions, Math.max(0, pkHotNum));
        }
        return partitions.subList(numPartitions - numHotIds, numPartitions).stream().map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private Optional<SlotId> associateSlotIdsWithColumns(FragmentNormalizer normalizer, TNormalPlanNode planNode,
                                                         Optional<Column> optPartitionColumn) {
        List<SlotDescriptor> slots = normalizer.getExecPlan().getDescTbl().getTupleDesc(tupleIds.get(0)).getSlots();
        List<Pair<SlotId, String>> slotIdToColNames =
                slots.stream().map(s -> new Pair<>(s.getId(), s.getColumn().getName()))
                        .collect(Collectors.toList());

        Optional<SlotId> optPartitionSlotId = Optional.empty();
        if (optPartitionColumn.isPresent()) {
            Column column = optPartitionColumn.get();
            SlotId slotId = slotIdToColNames.stream()
                    .filter(s -> s.second.equalsIgnoreCase(column.getName()))
                    .findFirst().map(s -> s.first).orElse(new SlotId(-1));
            optPartitionSlotId = Optional.of(slotId);
            // slotId.isValid means that whereClause contains no predicates involving partition columns. so the query
            // is equivalent to the query with a superset of all the partitions, so we needs add a fake slotId that
            // represents the partition column to the slot remapping. for an example:
            // table t0 is partition by dt and has there partitions:
            //  p0=[2022-01-01, 2022-01-02),
            //  p1=[2022-01-02, 2022-01-03),
            //  p2=[2022-01-03, 2022-01-04).
            // Q1: select count(*) from t0 will be performed p0,p1,p2. but dt is absent from OlapScanNode.
            // Q2: select count(*) from t0 where dt between and '2022-01-01' and '2022-01-01' should use the partial result
            // of Q1 on p0. but Q1 and Q2 has different SlotId re-mappings, so Q2's cache key cannot match the Q1's. so
            // we should add a fake slotId represents the partition column when we re-map slotIds.
            if (!slotId.isValid()) {
                slotIdToColNames.add(new Pair<>(slotId, column.getName()));
            }
        }
        slotIdToColNames.sort(Pair.comparingBySecond());
        List<SlotId> slotIds = slotIdToColNames.stream().map(s -> s.first).collect(Collectors.toList());
        List<Integer> remappedSlotIds = normalizer.remapSlotIds(slotIds);

        planNode.olap_scan_node.setRemapped_slot_ids(remappedSlotIds);
        planNode.olap_scan_node.setSelected_column(
                slotIdToColNames.stream().map(c -> c.second).collect(Collectors.toList()));
        return optPartitionSlotId;
    }

    private List<Expr> decomposeRangePredicates(FragmentNormalizer normalizer, TNormalPlanNode planNode,
                                                RangePartitionInfo rangePartitionInfo, List<Expr> conjuncts) {
        List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
        Set<Long> selectedPartIdSet = new HashSet<>(selectedPartitionIds);
        selectedPartIdSet.removeAll(getHotPartitionIds(rangePartitionInfo));

        Column column = partitionColumns.get(0);
        Optional<SlotId> optSlotId = associateSlotIdsWithColumns(normalizer, planNode, Optional.of(column));
        List<Map.Entry<Long, Range<PartitionKey>>> rangeMap = Lists.newArrayList();
        try {
            rangeMap = rangePartitionInfo.getSortedRangeMap(selectedPartIdSet);
        } catch (AnalysisException ignored) {
        }
        Preconditions.checkState(optSlotId.isPresent());
        return normalizer.getPartitionRangePredicates(conjuncts, rangeMap, rangePartitionInfo, optSlotId.get());
    }

    @Override
    public void normalizeConjuncts(FragmentNormalizer normalizer, TNormalPlanNode planNode, List<Expr> conjuncts) {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        // TODO (by satanson): predicates' decomposition
        //  At present, we support predicates' decomposition on RangePartition with single-column partition key.
        //  in the future, predicates' decomposition on RangePartition with multi-column partition key will be
        //  supported.
        if (partitionInfo.getType() == PartitionType.RANGE &&
                ((RangePartitionInfo) partitionInfo).getPartitionColumns().size() == 1) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            conjuncts = decomposeRangePredicates(normalizer, planNode, rangePartitionInfo, conjuncts);
        } else {
            associateSlotIdsWithColumns(normalizer, planNode, Optional.empty());
            normalizer.createSimpleRangeMap(getSelectedPartitionIds());
        }
        planNode.setConjuncts(normalizer.normalizeExprs(conjuncts));
    }

    // Only DUP_KEYS and AGG_KEYS without columns carrying REPLACE modifier can support
    // multi-version cache, for other types of data models, we can not get the final result
    // of the tablet from merging the snapshot result in cache and the delta rowsets in disk.
    private boolean canUseMultiVersionCache() {
        switch (olapTable.getKeysType()) {
            case PRIMARY_KEYS:
            case UNIQUE_KEYS:
                return false;
            case DUP_KEYS:
            case AGG_KEYS: {
                List<Column> columns = selectedIndexId == -1 ? olapTable.getBaseSchema() :
                        olapTable.getSchemaByIndexId(selectedIndexId);
                return columns.stream().noneMatch(
                        c -> c.isAggregated() && c.getAggregationType().isReplaceFamily());
            }
        }
        return false;
    }

    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        normalizer.setKeysType(olapTable.getKeysType());
        normalizer.setCanUseMultiVersion(canUseMultiVersionCache());

        List<Column> columns = selectedIndexId == -1 ? olapTable.getBaseSchema() :
                olapTable.getSchemaByIndexId(selectedIndexId);

        Set<String> aggColumnNames =
                columns.stream().filter(Column::isAggregated).map(Column::getName).collect(Collectors.toSet());
        Set<SlotId> aggColumnSlotIds =
                normalizer.getExecPlan().getDescTbl().getTupleDesc(tupleIds.get(0)).getSlots().stream()
                        .filter(s -> aggColumnNames.contains(s.getColumn().getName())).map(s -> s.getId())
                        .collect(Collectors.toSet());
        normalizer.setSlotsUseAggColumns(aggColumnSlotIds);

        TNormalOlapScanNode scanNode = new TNormalOlapScanNode();
        scanNode.setTablet_id(olapTable.getId());
        scanNode.setIndex_id(selectedIndexId);
        List<String> keyColumnNames = new ArrayList<String>();
        List<TPrimitiveType> keyColumnTypes = new ArrayList<TPrimitiveType>();
        if (selectedIndexId != -1) {
            for (Column col : olapTable.getSchemaByIndexId(selectedIndexId)) {
                if (!col.isKey()) {
                    break;
                }
                keyColumnNames.add(col.getName());
                keyColumnTypes.add(col.getPrimitiveType().toThrift());
            }
        }
        scanNode.setKey_column_names(keyColumnNames);
        scanNode.setKey_column_types(keyColumnTypes);
        scanNode.setIs_preaggregation(isPreAggregation);
        scanNode.setSort_column(sortColumn);
        scanNode.setRollup_name(olapTable.getIndexNameById(selectedIndexId));

        List<Integer> dictStringIds =
                dictStringIdToIntIds.keySet().stream().sorted(Integer::compareTo).collect(Collectors.toList());
        List<Integer> dictIntIds = dictStringIds.stream().map(dictStringIdToIntIds::get).collect(Collectors.toList());
        scanNode.setDict_string_ids(dictStringIds);
        scanNode.setDict_int_ids(dictIntIds);
        planNode.setNode_type(TPlanNodeType.OLAP_SCAN_NODE);
        planNode.setOlap_scan_node(scanNode);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }

    private Map<Long, List<Long>> mapTabletsToPartitions() {
        Map<Long, Long> tabletToPartitionMap = Maps.newHashMapWithExpectedSize(selectedPartitionIds.size());
        Map<Long, List<Long>> partitionToTabletMap = Maps.newHashMapWithExpectedSize(selectedPartitionIds.size() / 2);

        for (Long partitionId : selectedPartitionIds) {
            partitionToTabletMap.put(partitionId, Lists.newArrayList());
            Partition partition = olapTable.getPartition(partitionId);
            MaterializedIndex materializedIndex = partition.getIndex(selectedIndexId);
            for (long tabletId : materializedIndex.getTabletIdsInOrder()) {
                tabletToPartitionMap.put(tabletId, partitionId);
            }
        }
        for (Long tabletId : scanTabletIds) {
            // for query: select count(1) from t tablet(tablet_id0, tablet_id1,...), the user-provided tablet_id
            // maybe invalid.
            Preconditions.checkState(tabletToPartitionMap.containsKey(tabletId),
                    String.format("Invalid tablet id: '%s'", tabletId));
            long partitionId = tabletToPartitionMap.get(tabletId);
            partitionToTabletMap.computeIfAbsent(partitionId, k -> Lists.newArrayList()).add(tabletId);
        }

        return partitionToTabletMap;
    }
}
