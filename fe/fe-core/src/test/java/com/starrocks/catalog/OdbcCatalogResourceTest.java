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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/OdbcCatalogResourceTest.java

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

package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.analysis.Analyzer;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.meta.MetaContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

public class OdbcCatalogResourceTest {
    private String name;
    private String type;

    private String host;
    private String port;
    private String user;
    private String passwd;
    private Map<String, String> properties;
    private Analyzer analyzer;

    @Before
    public void setUp() {
        name = "odbc";
        type = "odbc_catalog";
        host = "127.0.0.1";
        port = "7777";
        user = "starrocks";
        passwd = "starrocks";
        properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("host", host);
        properties.put("port", port);
        properties.put("user", user);
        properties.put("password", passwd);
        analyzer = AccessTestUtil.fetchAdminAnalyzer();
    }

    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_92);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./odbcCatalogResource");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        OdbcCatalogResource odbcCatalogResource1 = new OdbcCatalogResource("odbc1");
        odbcCatalogResource1.write(dos);

        Map<String, String> configs = new HashMap<>();
        configs.put("host", "host");
        configs.put("port", "port");
        configs.put("user", "user");
        configs.put("password", "password");
        OdbcCatalogResource odbcCatalogResource2 = new OdbcCatalogResource("odbc2");
        odbcCatalogResource2.setProperties(configs);
        odbcCatalogResource2.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        OdbcCatalogResource rOdbcCatalogResource1 = (OdbcCatalogResource) OdbcCatalogResource.read(dis);
        OdbcCatalogResource rOdbcCatalogResource2 = (OdbcCatalogResource) OdbcCatalogResource.read(dis);

        Assert.assertEquals("odbc1", rOdbcCatalogResource1.getName());
        Assert.assertEquals("odbc2", rOdbcCatalogResource2.getName());

        Assert.assertEquals(rOdbcCatalogResource2.getProperties("host"), "host");
        Assert.assertEquals(rOdbcCatalogResource2.getProperties("port"), "port");
        Assert.assertEquals(rOdbcCatalogResource2.getProperties("user"), "user");
        Assert.assertEquals(rOdbcCatalogResource2.getProperties("password"), "password");

        // 3. delete files
        dis.close();
        file.delete();
    }
}
