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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/external/elasticsearch/EsTestCase.java

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

package com.starrocks.external.elasticsearch;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.meta.MetaContext;
import com.starrocks.server.GlobalStateMgr;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class EsTestCase {

    protected static FakeEditLog fakeEditLog;
    protected static FakeGlobalStateMgr fakeGlobalStateMgr;
    protected static GlobalStateMgr masterGlobalStateMgr;
    protected static String mappingsStr = "";

    @BeforeClass
    public static void init() throws Exception {
        fakeEditLog = new FakeEditLog();
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        masterGlobalStateMgr = GlobalStateMgrTestUtil.createTestState();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_40);
        metaContext.setThreadLocalInfo();
        // masterGlobalStateMgr.setJournalVersion(FeMetaVersion.VERSION_40);
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
    }

    protected String loadJsonFromFile(String fileName) throws IOException, URISyntaxException {
        File file = new File(MappingPhaseTest.class.getClassLoader().getResource(fileName).toURI());
        InputStream is = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder jsonStr = new StringBuilder();
        String line = "";
        while ((line = br.readLine()) != null) {
            jsonStr.append(line);
        }
        br.close();
        is.close();
        return jsonStr.toString();
    }

    public EsTable fakeEsTable(String table, String index, String type, List<Column> columns) throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(EsTable.HOSTS, "127.0.0.1:8200");
        props.put(EsTable.INDEX, index);
        props.put(EsTable.TYPE, type);
        props.put(EsTable.VERSION, "6.5.3");
        return new EsTable(new Random().nextLong(), table, columns, props, null);

    }
}
