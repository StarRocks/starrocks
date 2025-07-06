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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExternalProcDirTest {

    @BeforeEach
    public void setUp() throws Exception {
        new MockUp<MetadataMgr>() {
            @Mock
            public List<String> listDbNames(ConnectContext context, String catalogName) throws DdlException {
                ImmutableSet.Builder<String> dbNames = ImmutableSet.builder();
                dbNames.add("hive_test").add("temp_db").add("ods");
                return ImmutableList.copyOf(dbNames.build());
            }

            @Mock
            public Database getDb(ConnectContext context, String catalogName, String dbName) {
                return new Database();
            }

            @Mock
            public List<String> listTableNames(ConnectContext context, String catalogName, String dbName) throws DdlException {
                ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
                tableNames.add("test1");
                tableNames.add("sr");
                tableNames.add("starrocks");
                return ImmutableList.copyOf(tableNames.build());
            }
            @Mock
            public Table getTable(ConnectContext context, String catalogName, String dbName, String tblName) {
                return new HiveTable();
            }
        };

        new MockUp<HiveTable>() {
            public List<Column> getFullSchema() {
                List<Column> fullSchema = new ArrayList<>();
                Column columnId = new Column("id", Type.INT);
                Column columnName = new Column("name", Type.VARCHAR);
                fullSchema.add(columnId);
                fullSchema.add(columnName);
                return fullSchema;
            }
        };

    }

    @Test
    public void testExternalDbsFetchResultNormal() {
        ExternalDbsProcDir dir = new ExternalDbsProcDir("hive_catalog");
        ProcResult result = dir.fetchResult();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof BaseProcResult);

        Assertions.assertEquals(Lists.newArrayList("DbName"),
                result.getColumnNames());
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Arrays.asList("hive_test"));
        rows.add(Arrays.asList("ods"));
        rows.add(Arrays.asList("temp_db"));
        Assertions.assertEquals(rows, result.getRows());

        Config.enable_check_db_state = false;
        result = dir.fetchResult();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof BaseProcResult);

        Assertions.assertEquals(Lists.newArrayList("DbName"),
                result.getColumnNames());
        rows.clear();
        rows.add(Arrays.asList("hive_test"));
        rows.add(Arrays.asList("ods"));
        rows.add(Arrays.asList("temp_db"));
        Assertions.assertEquals(rows, result.getRows());
    }

    @Test
    public void testExternalTablesFetchResultNormal() throws AnalysisException {
        ExternalTablesProcDir dir = new ExternalTablesProcDir("hive_catalog", "temp_db");
        ProcResult result = dir.fetchResult();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof BaseProcResult);
        Assertions.assertEquals(Lists.newArrayList("TableName"),
                result.getColumnNames());
        Assertions.assertEquals(3, result.getRows().size());
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Arrays.asList("sr"));
        rows.add(Arrays.asList("starrocks"));
        rows.add(Arrays.asList("test1"));
        Assertions.assertEquals(rows, result.getRows());
    }

    @Test
    public void testExternalTableFetchResultNormal() throws AnalysisException {
        Table table = new HiveTable();
        ExternalTableProcDir dir = new ExternalTableProcDir(table);
        ProcResult result = dir.fetchResult();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof BaseProcResult);
        Assertions.assertEquals(Lists.newArrayList("Nodes"), result.getColumnNames());
        Assertions.assertEquals(1, result.getRows().size());
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Arrays.asList("schema"));
        Assertions.assertEquals(rows, result.getRows());
    }

    @Test
    public void testExternalScemaFetchResultNormal() throws AnalysisException {
        Table table = new HiveTable();
        List<Column> fullSchema = new ArrayList<>();
        Column columnId = new Column("id", Type.INT, false, "id");
        Column columnName = new Column("name", Type.VARCHAR, false, "name");
        fullSchema.add(columnId);
        fullSchema.add(columnName);
        table.setNewFullSchema(fullSchema);
        ExternalSchemaProcNode dir = new ExternalSchemaProcNode(table);
        ProcResult result = dir.fetchResult();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof BaseProcResult);
        Assertions.assertEquals(Lists.newArrayList("Field", "Type", "Null", "Key", "Default", "Extra", "Comment"),
                result.getColumnNames());
        Assertions.assertEquals(2, result.getRows().size());
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Arrays.asList("id", "INT", "No", "false", FeConstants.NULL_STRING, "", "id"));
        rows.add(Arrays.asList("name", "VARCHAR", "No", "false", FeConstants.NULL_STRING, "", "name"));
        Assertions.assertEquals(rows, result.getRows());
    }
}
