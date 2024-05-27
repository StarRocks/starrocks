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

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TExplainLevel;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class DeltaLakeScanNodeTest {
    @Test
    public void testInit(@Mocked GlobalStateMgr globalStateMgr,
                         @Mocked CatalogConnector connector,
                         @Mocked DeltaLakeTable table) {
        String catalog = "XXX";
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
                result = connector;
                connector.getMetadata().getCloudConfiguration();
                result = cc;
                table.getCatalogName();
                result = catalog;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(0), desc, "XXX");
    }

    @Test
    public void testNodeExplain(@Mocked GlobalStateMgr globalStateMgr, @Mocked CatalogConnector connector,
                            @Mocked DeltaLakeTable table) {
        String catalogName = "delta0";
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
                result = connector;
                minTimes = 0;

                connector.getMetadata().getCloudConfiguration();
                result = cloudConfiguration;
                minTimes = 0;

                table.getCatalogName();
                result = catalogName;
                minTimes = 0;

                table.getName();
                result = "table0";
                minTimes = 0;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(0), desc, "Delta Scan Node");
        Assert.assertFalse(scanNode.getNodeExplainString("", TExplainLevel.NORMAL).contains("partitions"));
        Assert.assertTrue(scanNode.getNodeExplainString("", TExplainLevel.VERBOSE).contains("partitions"));
    }
}