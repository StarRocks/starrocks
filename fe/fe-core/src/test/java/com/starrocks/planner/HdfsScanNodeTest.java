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
import com.starrocks.catalog.HiveTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;

import java.util.HashMap;

public class HdfsScanNodeTest {
    @Test
    public void testInit(@Mocked GlobalStateMgr globalStateMgr,
                         @Mocked CatalogConnector connector,
                         @Mocked HiveTable table) {
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
        HdfsScanNode scanNode = new HdfsScanNode(new PlanNodeId(0), desc, "XXX");
    }
}