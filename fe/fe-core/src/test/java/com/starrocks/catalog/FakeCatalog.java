// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/FakeCatalog.java

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

import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;

public class FakeCatalog extends MockUp<Catalog> {

    private static Catalog catalog;
    private static int metaVersion;
    private static SystemInfoService systemInfo = new SystemInfoService();

    public static void setCatalog(Catalog catalog) {
        FakeCatalog.catalog = catalog;
    }

    public static void setMetaVersion(int metaVersion) {
        FakeCatalog.metaVersion = metaVersion;
    }

    public static void setSystemInfo(SystemInfoService systemInfo) {
        FakeCatalog.systemInfo = systemInfo;
    }

    @Mock
    public static Catalog getCurrentCatalog() {
        return catalog;
    }

    @Mock
    public static Catalog getInstance() {
        return catalog;
    }

    @Mock
    public static int getCurrentCatalogJournalVersion() {
        return metaVersion;
    }

    @Mock
    public static SystemInfoService getCurrentSystemInfo() {
        return systemInfo;
    }

}