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


package com.starrocks.server;

import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.File;

public class WarehouseMgrTest {
    private static StarRocksAssert starRocksAssert;
    private String fileName = "./testWarehouseMgr";

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert();
    }

    @After
    public void tearDownCreate() throws Exception {
        File file = new File(fileName);
        file.delete();
    }
}
