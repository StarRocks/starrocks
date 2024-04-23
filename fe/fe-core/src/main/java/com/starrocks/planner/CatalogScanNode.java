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

import com.google.common.base.Preconditions;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.THdfsScanNode;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

public abstract class CatalogScanNode extends ScanNode {

    protected CloudConfiguration cloudConfiguration = null;
    // isEnableOriginalPruneComplexTypes means is use the original catalog's subfield prune rule
    private boolean isUsingOriginalPruneComplexTypes = false;

    public CatalogScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        setupCloudCredential(desc.getTable());
    }

    protected void setupCloudCredential(Table table) {
        String catalogName = table.getCatalogName();
        if (catalogName == null) {
            return;
        }
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
        Preconditions.checkState(connector != null,
                String.format("connector of catalog %s should not be null", catalogName));
        cloudConfiguration = connector.getMetadata().getCloudConfiguration();
        Preconditions.checkState(cloudConfiguration != null,
                String.format("cloudConfiguration of catalog %s should not be null", catalogName));
    }

    public void setColumnAccessPaths(boolean isEnableOriginalPruneComplexTypes,
                                     List<ColumnAccessPath> columnAccessPaths) {
        this.isUsingOriginalPruneComplexTypes = isEnableOriginalPruneComplexTypes;
        this.columnAccessPaths = columnAccessPaths;
    }

    protected String explainCatalogComplexTypePrune(String prefix) {
        StringBuilder output = new StringBuilder();
        if (isUsingOriginalPruneComplexTypes) {
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                Type type = slotDescriptor.getOriginType();
                if (type.isComplexType()) {
                    output.append(prefix)
                            .append(String.format("Pruned type: %d [%s] <-> [%s]\n", slotDescriptor.getId().asInt(), slotDescriptor.getColumn().getName(), type));
                }
            }
        } else {
            output.append(explainColumnAccessPath(prefix));
        }
        return output.toString();
    }

    protected void setColumnAccessPathToThrift(THdfsScanNode tHdfsScanNode) {
        if (isUsingOriginalPruneComplexTypes) {
            // We've already pruned SlotDescriptor's type, don't need to pass ColumnAccessPath anymore
            return;
        }
        if (CollectionUtils.isNotEmpty(columnAccessPaths)) {
            tHdfsScanNode.setColumn_access_paths(columnAccessPathToThrift());
        }
    }

    protected void setCloudConfigurationToThrift(THdfsScanNode tHdfsScanNode) {
        if (cloudConfiguration != null) {
            TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
            cloudConfiguration.toThrift(tCloudConfiguration);
            tHdfsScanNode.setCloud_configuration(tCloudConfiguration);
        }
    }
}
