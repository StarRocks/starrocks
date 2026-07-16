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

package com.starrocks.planner.lance;

import com.starrocks.catalog.LanceTable;
import com.starrocks.common.VectorSearchOptions;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ReadOptions;
import org.lance.index.Index;
import org.lance.schema.LanceField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.optimizer.rule.transformation.RewriteLanceToVectorPlanRule.LANCE_VECTOR_COLUMN_PARAM;

public class LanceScanNode extends ScanNode {
    private final LanceTable lanceTable;
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private VectorSearchOptions vectorSearchOptions = new VectorSearchOptions();

    public LanceScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        this.lanceTable = (LanceTable) desc.getTable();
    }

    public LanceTable getLanceTable() {
        return lanceTable;
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    public void setVectorSearchOptions(VectorSearchOptions vectorSearchOptions) {
        this.vectorSearchOptions = vectorSearchOptions == null ? new VectorSearchOptions() : vectorSearchOptions.copy();
    }

    @Override
    public boolean isConnectorScanNode() {
        return true;
    }

    public void setupScanRangeLocations() {
        String datasetUri = lanceTable.getDatasetURI();
        Map<String, String> storageOptions = LanceConfig.buildStorageOptions(lanceTable.getProperties());
        SessionVariable sessionVariable = ConnectContext.getSessionVariableOrDefault();
        validateJsonColumnsWithReader(sessionVariable);
        ReadOptions.Builder builder = new ReadOptions.Builder();
        if (!storageOptions.isEmpty()) {
            builder.setStorageOptions(storageOptions);
        }

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset dataset = Dataset.open(allocator, datasetUri, builder.build())) {
            if (vectorSearchOptions.isEnableUseANN()) {
                setupVectorScanRangeLocations(dataset, datasetUri, storageOptions);
                return;
            }
            for (Fragment fragment : dataset.getFragments()) {
                scanRangeLocationsList.add(toScanRangeLocations(datasetUri, fragment, storageOptions, sessionVariable));
            }
        } catch (Exception e) {
            throw new StarRocksConnectorException("Failed to plan lance scan ranges for %s: %s",
                    datasetUri, e.getMessage());
        }
    }

    private void setupVectorScanRangeLocations(Dataset dataset, String datasetUri, Map<String, String> storageOptions) {
        SessionVariable sessionVariable = ConnectContext.getSessionVariableOrDefault();
        if (!useNativeReader(sessionVariable)) {
            throw new StarRocksConnectorException("Lance vector search only supports the native Lance reader");
        }

        String vectorColumn = vectorSearchOptions.getQueryParams().get(LANCE_VECTOR_COLUMN_PARAM);
        if (vectorColumn == null || vectorColumn.isEmpty()) {
            throw new StarRocksConnectorException("Missing Lance vector column in vector search options");
        }

        int vectorFieldId = findFieldId(dataset, vectorColumn);
        List<Index> vectorSegments = loadVectorIndexSegments(dataset, vectorFieldId, datasetUri, vectorColumn);
        Map<Integer, Fragment> fragmentsById = new HashMap<>();
        Set<Integer> liveFragments = new HashSet<>();
        for (Fragment fragment : dataset.getFragments()) {
            fragmentsById.put(fragment.getId(), fragment);
            liveFragments.add(fragment.getId());
        }
        validateSegmentCoverage(vectorSegments, liveFragments, datasetUri, vectorColumn);

        for (Index segment : vectorSegments) {
            scanRangeLocationsList.add(toVectorScanRangeLocations(datasetUri, segment, fragmentsById, storageOptions));
        }
    }

    private int findFieldId(Dataset dataset, String vectorColumn) {
        for (LanceField field : dataset.getLanceSchema().fields()) {
            if (field.getName().equals(vectorColumn)) {
                return field.getId();
            }
        }
        throw new StarRocksConnectorException("Cannot find Lance vector column %s in dataset %s",
                vectorColumn, lanceTable.getDatasetURI());
    }

    private List<Index> loadVectorIndexSegments(Dataset dataset, int vectorFieldId, String datasetUri, String vectorColumn) {
        List<Index> matched = new ArrayList<>();
        Set<String> indexNames = new HashSet<>();
        for (Index index : dataset.getIndexes()) {
            if (index.fields().contains(vectorFieldId)) {
                matched.add(index);
                indexNames.add(index.name());
            }
        }
        if (matched.isEmpty()) {
            throw new StarRocksConnectorException("Cannot find Lance vector index for column %s in %s",
                    vectorColumn, datasetUri);
        }
        if (indexNames.size() > 1) {
            throw new StarRocksConnectorException(
                    "Found multiple Lance vector indexes for column %s in %s: %s",
                    vectorColumn, datasetUri, indexNames);
        }
        return matched;
    }

    private void validateSegmentCoverage(List<Index> segments, Set<Integer> liveFragments,
                                         String datasetUri, String vectorColumn) {
        Set<Integer> covered = new HashSet<>();
        for (Index segment : segments) {
            List<Integer> fragments = segment.fragments().orElseThrow(() ->
                    new StarRocksConnectorException("Lance vector index segment %s has no fragment coverage",
                            segment.uuid()));
            for (Integer fragmentId : fragments) {
                if (!liveFragments.contains(fragmentId)) {
                    continue;
                }
                if (!covered.add(fragmentId)) {
                    throw new StarRocksConnectorException(
                            "Lance vector index segments overlap on fragment %s for column %s in %s",
                            fragmentId, vectorColumn, datasetUri);
                }
            }
        }
        if (!covered.equals(liveFragments)) {
            Set<Integer> missing = new HashSet<>(liveFragments);
            missing.removeAll(covered);
            throw new StarRocksConnectorException(
                    "Lance vector index does not cover all live fragments for column %s in %s, missing fragments: %s",
                    vectorColumn, datasetUri, missing);
        }
    }

    private TScanRangeLocations toScanRangeLocations(String datasetUri, Fragment fragment,
                                                     Map<String, String> storageOptions,
                                                     SessionVariable sessionVariable) {
        int rowCount = Math.max(fragment.countRows(), 1);
        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        boolean useNativeReader = useNativeReader(sessionVariable);
        hdfsScanRange.setUse_lance_native_reader(useNativeReader);
        hdfsScanRange.setDataset_uri(datasetUri);
        hdfsScanRange.setFragment_id(fragment.getId());
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(rowCount);
        hdfsScanRange.setFile_length(rowCount);
        hdfsScanRange.setFile_format(THdfsFileFormat.UNKNOWN);
        hdfsScanRange.setRelative_path(datasetUri + "#" + fragment.getId());
        if (!storageOptions.isEmpty()) {
            hdfsScanRange.setLance_storage_options(storageOptions);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);

        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        scanRangeLocations.setScan_range(scanRange);
        scanRangeLocations.setLocations(new ArrayList<TScanRangeLocation>());
        return scanRangeLocations;
    }

    private void validateJsonColumnsWithReader(SessionVariable sessionVariable) {
        if (useNativeReader(sessionVariable)) {
            return;
        }
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized() || !containsJsonType(slot.getType())) {
                continue;
            }
            String columnName = slot.getColumn() == null ? slot.getId().toString() : slot.getColumn().getName();
            throw new StarRocksConnectorException(
                    "Lance JSON columns are supported only by the native Lance reader. " +
                            "Disable lance_force_jni_reader to query column %s", columnName);
        }
    }

    static boolean containsJsonType(Type type) {
        if (type == null) {
            return false;
        }
        if (type.isJsonType()) {
            return true;
        }
        if (type.isArrayType()) {
            return containsJsonType(((ArrayType) type).getItemType());
        }
        if (type.isMapType()) {
            MapType mapType = (MapType) type;
            return containsJsonType(mapType.getKeyType()) || containsJsonType(mapType.getValueType());
        }
        if (type.isStructType()) {
            for (StructField field : ((StructType) type).getFields()) {
                if (containsJsonType(field.getType())) {
                    return true;
                }
            }
        }
        return false;
    }

    private TScanRangeLocations toVectorScanRangeLocations(String datasetUri, Index segment,
                                                           Map<Integer, Fragment> fragmentsById,
                                                           Map<String, String> storageOptions) {
        long rowCount = 0;
        for (Integer fragmentId : segment.fragments().orElse(List.of())) {
            Fragment fragment = fragmentsById.get(fragmentId);
            if (fragment != null) {
                rowCount += Math.max(fragment.countRows(), 1);
            }
        }
        if (rowCount <= 0) {
            rowCount = 1;
        }

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setUse_lance_native_reader(true);
        hdfsScanRange.setDataset_uri(datasetUri);
        hdfsScanRange.setFragment_id(-1);
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(rowCount);
        hdfsScanRange.setFile_length(rowCount);
        hdfsScanRange.setFile_format(THdfsFileFormat.UNKNOWN);
        hdfsScanRange.setRelative_path(datasetUri + "#index_segment=" + segment.uuid());
        hdfsScanRange.setLance_vector_search_options(vectorSearchOptions.toThrift());
        hdfsScanRange.setLance_index_segment_uuids(List.of(segment.uuid().toString()));
        if (!storageOptions.isEmpty()) {
            hdfsScanRange.setLance_storage_options(storageOptions);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);

        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        scanRangeLocations.setScan_range(scanRange);
        scanRangeLocations.setLocations(new ArrayList<TScanRangeLocation>());
        return scanRangeLocations;
    }

    static boolean useNativeReader(SessionVariable sessionVariable) {
        if (sessionVariable.getLanceForceJNIReader()) {
            return false;
        }
        if (sessionVariable.getLanceForceNativeReader()) {
            return true;
        }
        return true;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(lanceTable.getName()).append("\n");
        output.append(prefix).append("DATASET URI: ").append(lanceTable.getDatasetURI()).append("\n");
        output.append(prefix).append("PREDICATES: ").append(getExplainString(conjuncts)).append("\n");
        if (vectorSearchOptions.isEnableUseANN()) {
            output.append(vectorSearchOptions.getExplainString(prefix));
        }
        if (columnAccessPaths != null) {
            output.append(explainColumnAccessPath(prefix));
        }
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        tHdfsScanNode.setTable_name(lanceTable.getName());
        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        msg.hdfs_scan_node = tHdfsScanNode;
    }
}
