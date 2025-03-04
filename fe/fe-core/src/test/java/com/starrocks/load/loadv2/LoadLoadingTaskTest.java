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

package com.starrocks.load.loadv2;

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.LoadException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class LoadLoadingTaskTest {
    @Injectable
    private ConnectContext connectContext;

    private VariableMgr variableMgr = new VariableMgr();

    @Test
    public void testBuilder(
            @Mocked GlobalStateMgr globalStateMgr) {

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNextId();
                result = 1;
                times = 1;
            }};

        // json options
        LoadJob.JSONOptions jsonOptions = new LoadJob.JSONOptions();
        jsonOptions.jsonRoot = "\"$.data\"";
        jsonOptions.jsonPaths = "[\"$.key1\"]";
        jsonOptions.stripOuterArray = true;

        LoadLoadingTask task = new LoadLoadingTask.Builder()
                .setJSONOptions(jsonOptions)
                .build();


        LoadJob.JSONOptions realJSONOptions =  Deencapsulation.getField(task, "jsonOptions");
        Assert.assertEquals(jsonOptions, realJSONOptions);
    }

    @Test
    public void testBuildTopLevelProfile(@Mocked GlobalStateMgr globalStateMgr,
                                          @Mocked GlobalTransactionMgr globalTransactionMgr,
                                          @Mocked TransactionState transactionState) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNextId();
                result = 1;
                connectContext.getSessionVariable();
                result = new SessionVariable();
                connectContext.getQualifiedUser();
                result = "test_user";
                connectContext.getDatabase();
                result = "test_db";

                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getVariableMgr();
                minTimes = 0;
                result = variableMgr;
            }
        };

        // Call the method under test
        Database database = new Database(10000L, "test");
        OlapTable olapTable = new OlapTable(10001L, "tbl", null, KeysType.AGG_KEYS, null, null);
        LoadStmt stmt = new LoadStmt(null, null, new BrokerDesc(null), null, null);
        LoadLoadingTask loadLoadingTask = new LoadLoadingTask.Builder().setDb(database).setLoadStmt(stmt)
                .setTable(olapTable).setContext(connectContext).setOriginStmt(new OriginStatement("")).build();
        RuntimeProfile profile = loadLoadingTask.buildRunningTopLevelProfile();
        // Perform assertions to verify the behavior
        assertNotNull("Profile should not be null", profile);

        profile = loadLoadingTask.buildFinishedTopLevelProfile();
        // Perform assertions to verify the behavior
        assertNotNull("Profile should not be null", profile);
    }

    @Test
    public void testExecuteTaskWhenMetaDropped(@Mocked CatalogIdGenerator idGenerator) {
        new Expectations() {
            {
                idGenerator.getNextId();
                result = 1;
                times = 1;
            }};

        Database database = new Database(10000L, "test");
        OlapTable olapTable = new OlapTable(10001L, "tbl", null, KeysType.AGG_KEYS, null, null);
        LoadLoadingTask loadLoadingTask =
                new LoadLoadingTask.Builder().setDb(database).setTable(olapTable).setContext(ConnectContext.build()).build();

        // database not exist
        boolean exceptionThrown = false;
        try {
            loadLoadingTask.executeTask();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof LoadException);
            exceptionThrown = true;
        }

        Assert.assertTrue(exceptionThrown);

        // table not exist
        exceptionThrown = false;
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(database);
        try {
            loadLoadingTask.executeTask();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof LoadException);
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);
    }

    @Test
    public void testExecuteTask(@Mocked CatalogIdGenerator idGenerator) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setTimeZone("UTC");
        new Expectations() {
            {
                idGenerator.getNextId();
                result = 1;
                connectContext.getQualifiedUser();
                result = "test_user";
                connectContext.getSessionVariable();
                result = sessionVariable;
            }};

        Database database = new Database(10000L, "test");
        OlapTable olapTable = new OlapTable(10001L, "tbl", null, KeysType.AGG_KEYS, null, null);
        connectContext.setStartTime();
        LoadLoadingTask loadLoadingTask = new LoadLoadingTask.Builder().setLoadId(new TUniqueId(2, 3))
                .setContext(connectContext).setDb(database).setTable(olapTable).setCallback(new BrokerLoadJob()).build();

        boolean exceptionThrown = false;
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(database);
        database.registerTableUnlocked(olapTable);
        try {
            loadLoadingTask.prepare();
        } catch (Exception e) {
        }

        try {
            loadLoadingTask.executeTask();
        } catch (Exception e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);
    }
}
