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

package com.starrocks.common.proc;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.connector.paimon.Partition;
import com.starrocks.server.MetadataMgr;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.util.Lists;
import org.apache.paimon.catalog.Catalog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class PaimonPartitionsProcDirTest {
    @Mocked
    Catalog paimonNativeCatalog;
    @Mocked
    org.apache.paimon.table.Table nativeTable;
    private PaimonMetadata metadata;

    @Before
    public void setUp() throws DdlException, AnalysisException {
        this.metadata = new PaimonMetadata("paimon_catalog", new HdfsEnvironment(), paimonNativeCatalog, null);
    }

    @Test
    public void testFetchResult() throws AnalysisException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("/catalog/");
        stringBuilder.append("paimon_catalog");
        stringBuilder.append("/").append("db1");
        stringBuilder.append("/").append("tb1");
        stringBuilder.append("/partitions");

        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(String catalogName, String dbName) {
                return new Database(1L, "db1");
            }

            @Mock
            public Table getTable(String catalogName, String dbName, String tblName) {
                return new PaimonTable("paimon_catalog", "db1", "tb1", null, nativeTable, 1L);
            }

            @Mock
            public List<PartitionInfo> getPartitions(String catalogName, Table table, List<String> partitionNames) {
                Partition p1 = new Partition("dt=20240901", 1727079167000L, 5L, 12112L, 11L);
                Partition p2 = new Partition("dt=null", 1727079167000L, 1L, 12L, 1L);
                return Lists.newArrayList(p1, p2);
            }
        };

        ProcNodeInterface paimonProc = ProcService.getInstance().open(stringBuilder.toString());
        Assert.assertTrue(paimonProc instanceof PaimonTablePartitionsProcDir);

        ProcResult procResult = ((PartitionsProcDir) paimonProc).fetchResultByFilter(null, null, null);
        Assert.assertEquals(procResult.getRows().size(), 2);
        Assert.assertEquals(procResult.getRows().get(0).size(), 7);
        Assert.assertEquals(procResult.getColumnNames().size(), 7);

    }
}
