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

package com.starrocks.connector;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import com.starrocks.catalog.MvId;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Assert;
import org.junit.Test;

public class ConnectorTblMetaInfoMgrTest {

    @Test
    public void testInspect() {
        ConnectorTblMetaInfoMgr mgr = GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr();

        String json = mgr.inspect();
        Assert.assertEquals("{}", json);

        ConnectorTableInfo tableInfo = new ConnectorTableInfo(
                ImmutableSet.of(
                        new MvId(1, 1),
                        new MvId(1, 2)
                )
        );
        mgr.addConnectorTableInfo("cat", "db", "tbl", tableInfo);
        json = mgr.inspect();
        Assert.assertEquals("{\"cat.db.tbl\":[{\"dbId\":1,\"id\":1},{\"dbId\":1,\"id\":2}]}", json);

        mgr.removeConnectorTableInfo("cat", "db", "tbl", tableInfo);
        json = mgr.inspect();
        Assert.assertEquals("{}", json);

        ConnectorTableInfo tableInfo1 = new ConnectorTableInfo(null);
        tableInfo1.updateMetaInfo(tableInfo);
        JsonElement element = tableInfo1.inspect();
        Assert.assertEquals("[{\"dbId\":1,\"id\":1},{\"dbId\":1,\"id\":2}]", element.toString());
    }
}