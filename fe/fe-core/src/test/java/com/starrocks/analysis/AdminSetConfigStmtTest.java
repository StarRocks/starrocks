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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/AdminSetConfigStmtTest.java

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

package com.starrocks.analysis;

import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class AdminSetConfigStmtTest {
    private static ConnectContext connectContext;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        String stmt = "admin set frontend config(\"alter_table_timeout_second\" = \"60\");";
        AdminSetConfigStmt adminSetConfigStmt =
                (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
        ConfigBase.setConfig(adminSetConfigStmt);
    }

    @Test
    public void testSetMysqlVersion() throws Exception {
        String stmt = "admin set frontend config(\"mysql_server_version\" = \"5.1.1\");";
        AdminSetConfigStmt adminSetConfigStmt =
                (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
        DDLStmtExecutor.execute(adminSetConfigStmt, connectContext);
        Assert.assertEquals("5.1.1", GlobalVariable.version);
    }

    @Test
    public void testUnknownConfig() throws Exception {
        String stmt = "admin set frontend config(\"unknown_config\" = \"unknown\");";
        AdminSetConfigStmt adminSetConfigStmt =
                (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Config 'unknown_config' does not exist or is not mutable");
        ConfigBase.setConfig(adminSetConfigStmt);
    }

    @Test
    public void testSetConfigFail() throws Exception {
        TSetConfigResponse response = new TSetConfigResponse();
        response.setStatus(new TStatus(TStatusCode.UNKNOWN));

        try (MockedStatic<ThriftRPCRequestExecutor> thriftConnectionPoolMockedStatic =
                Mockito.mockStatic(ThriftRPCRequestExecutor.class)) {
            thriftConnectionPoolMockedStatic.when(()
                            -> ThriftRPCRequestExecutor.call(Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(response);

            Frontend leader = new Frontend(FrontendNodeType.LEADER, "fe1", "192.168.0.1", 9091);
            List<Frontend> frontends = new ArrayList<>();
            frontends.add(leader);
            frontends.add(new Frontend(FrontendNodeType.FOLLOWER, "fe2", "192.168.0.2", 9092));

            new MockUp<NodeMgr>() {
                @Mock
                public List<Frontend> getFrontends(FrontendNodeType nodeType) {
                    return frontends;
                }

                @Mock
                public Pair<String, Integer> getSelfNode() {
                    return new Pair<>("192.168.0.1", 9091);
                }
            };

            String stmt = "admin set frontend config(\"alter_table_timeout_second\" = \"60\");";
            AdminSetConfigStmt adminSetConfigStmt =
                    (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
            expectedEx.expect(DdlException.class);
            ConfigBase.setConfig(adminSetConfigStmt);
        }
    }
}

