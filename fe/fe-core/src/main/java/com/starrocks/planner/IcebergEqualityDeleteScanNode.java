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

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.AnalysisException;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanRange;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergEqualityDeleteScanNode extends IcebergScanNode {
    private final List<Integer> equalityIds;
    private final Set<String> seenFiles = new HashSet<>();
    private final boolean needCheckEqualityIds;

    public IcebergEqualityDeleteScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                                         List<Integer> equalityIds, boolean needCheckEqualityIds) {
        super(id, desc, planNodeName);
        this.equalityIds = equalityIds;
        this.needCheckEqualityIds = needCheckEqualityIds;
    }

    public void buildScanRanges(FileScanTask task, Map<StructLike, Long> partitionKeyToId,
                                   Map<Long, List<Integer>> idToPartitionSlots, DescriptorTable descTbl)
            throws AnalysisException {
        for (DeleteFile file : task.deletes()) {
            if (file.content() != FileContent.EQUALITY_DELETES) {
                continue;
            }

            if (needCheckEqualityIds && !file.equalityFieldIds().equals(equalityIds)) {
                continue;
            }

            if (!seenFiles.contains(file.path().toString())) {
                THdfsScanRange hdfsScanRange = buildScanRange(task, file, partitionKeyToId, idToPartitionSlots, descTbl);
                fillResult(hdfsScanRange);
                seenFiles.add(file.path().toString());
            }
        }
    }

    public List<Integer> getEqualityIds() {
        return equalityIds;
    }

    public Set<String> getSeenFiles() {
        return seenFiles;
    }

    public boolean isNeedCheckEqualityIds() {
        return needCheckEqualityIds;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        List<String> identifierColumnNames = equalityIds.stream()
                .map(id -> icebergTable.getNativeTable().schema().findColumnName(id))
                .collect(Collectors.toList());
        output.append(prefix).append("Iceberg identifier columns: ").append(identifierColumnNames).append("\n");
        output.append("\n");

        return super.getNodeExplainString(prefix, detailLevel) + output;
    }
}
