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


package com.starrocks.load;

import com.google.api.client.util.Sets;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.CsvFormat;
import com.starrocks.common.StarRocksException;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BrokerFileGroupTest {
    @Mocked
    private Database db;
    @Mocked
    private OlapTable olapTable;
    @Mocked
    private HiveTable hiveTable;
    private static StarRocksAssert starRocksAssert;


    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        starRocksAssert.withDatabase("testDb");
        List<String> tables = Arrays.asList("olapTable");
        String sql = "create table testDb.%s (k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) " +
                "AGGREGATE KEY(k1, k2, k3, k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";

        tables.forEach(t -> {
            try {
                starRocksAssert.withTable(String.format(sql, t));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testCSVParams() throws StarRocksException {
        CsvFormat csvFormat = new CsvFormat((byte) '\'', (byte) '|', 3, true);
        List<String> filePaths = new ArrayList<>();
        filePaths.add("/a/b/c/file");
        DataDescription desc = new DataDescription("olapTable", null, 
                                filePaths, null, null, 
                                null, null, null,
                                false, null, null, csvFormat);
        desc.analyze("testDb");

        BrokerFileGroup fileGroup = new BrokerFileGroup(desc);
        fileGroup.parseFormatProperties(desc);
        Assertions.assertEquals('\'', fileGroup.getEnclose());
        Assertions.assertEquals('|', fileGroup.getEscape());
        Assertions.assertEquals(3, fileGroup.getSkipHeader());
        Assertions.assertEquals(true, fileGroup.isTrimspace());
    }

    @Test
    public void testCSVParamsWithSpecialCharacter() throws StarRocksException {
        CsvFormat csvFormat = new CsvFormat((byte) '\t', (byte) '\\', 3, true);
        List<String> filePaths = new ArrayList<>();
        filePaths.add("/a/b/c/file");
        DataDescription desc = new DataDescription("olapTable", null, 
                                filePaths, null, null, 
                                null, null, null,
                                false, null, null, csvFormat);
        desc.analyze("testDb");

        BrokerFileGroup fileGroup = new BrokerFileGroup(desc);
        fileGroup.parseFormatProperties(desc);
        Assertions.assertEquals('\\', fileGroup.getEscape());
        Assertions.assertEquals('\t', fileGroup.getEnclose());
        Assertions.assertEquals(92, fileGroup.getEscape());
        Assertions.assertEquals(9, fileGroup.getEnclose());
    }

    @Test
    public void testParseHiveTable() throws StarRocksException {
        // k1 = bitmap_dict(k1)
        SlotRef slotRef1 = new SlotRef(null, "k1");
        List<Expr> params1 = Lists.newArrayList(slotRef1);
        BinaryPredicate predicate1 = new BinaryPredicate(BinaryType.EQ, slotRef1,
                new FunctionCallExpr("bitmap_dict", params1));

        // k3 = k2 + 1
        SlotRef slotRef2 = new SlotRef(null, "k2");
        SlotRef slotRef3 = new SlotRef(null, "k3");
        BinaryPredicate predicate2 = new BinaryPredicate(
                BinaryType.EQ, slotRef3,
                new ArithmeticExpr(ArithmeticExpr.Operator.ADD, slotRef2, new IntLiteral(1, Type.INT)));
        DataDescription desc = new DataDescription("olapTable", null, "hiveTable", false,
                Lists.newArrayList(predicate1, predicate2), null);
        desc.analyze("testDb");

        // schema
        Column k1 = new Column("k1", Type.BITMAP);
        Column k2 = new Column("k2", Type.INT);
        Column k3 = new Column("k3", Type.INT);
        Column k4 = new Column("k4", Type.INT);

        new Expectations() {
            {
                db.getTable("olapTable");
                result = olapTable;
                db.getTable("hiveTable");
                result = hiveTable;
                olapTable.getBaseSchema();
                result = Lists.newArrayList(k3, k1);
                hiveTable.getBaseSchema();
                result = Lists.newArrayList(k1, k2, k4);
                hiveTable.getId();
                result = 10;
            }
        };

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }
        };

        BrokerFileGroup fileGroup = new BrokerFileGroup(desc);
        fileGroup.parse(db, desc);
        Assertions.assertEquals(Lists.newArrayList("k1", "k2"), fileGroup.getFileFieldNames());
        Assertions.assertEquals(10, fileGroup.getSrcTableId());
    }

    @Test
    public void testTableFunctionTableCSVDelimiter() throws StarRocksException {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", "fake://some_bucket/some_path/*");
        properties.put("format", "CSV");
        properties.put("csv.column_separator", "\\x01");
        properties.put("csv.row_delimiter", "\\x02");

        TableFunctionTable table = new TableFunctionTable(properties);
        BrokerFileGroup fileGroup = new BrokerFileGroup(table, Sets.newHashSet());
        Assertions.assertEquals("\1", fileGroup.getColumnSeparator());
        Assertions.assertEquals("\2", fileGroup.getRowDelimiter());
    }
}