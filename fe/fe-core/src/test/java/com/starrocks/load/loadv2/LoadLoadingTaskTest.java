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

import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.LoadException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class LoadLoadingTaskTest {
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
    public void testExecuteTaskWhenMetaDropped(@Mocked CatalogIdGenerator idGenerator) {
        new Expectations() {
            {
                idGenerator.getNextId();
                result = 1;
                times = 1;
            }};

        Database database = new Database(10000L, "test");
        OlapTable olapTable = new OlapTable(10001L, "tbl", null, KeysType.AGG_KEYS, null, null);
        LoadLoadingTask loadLoadingTask = new LoadLoadingTask.Builder().setDb(database).setTable(olapTable).build();

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
}
