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

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.catalog.TableName;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.type.DateType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.expressions.Term;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.TimeZone;

public class IcebergPartitionUtilsTest extends TableTestBase {
    @TempDir
    public static File staticTemp;
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(staticTemp, "junit").toURI().toString());
    }

    @Test
    public void testNormalizeTimePartitionName() {
        new MockUp<TimeUtils>() {
            @Mock
            public  TimeZone getTimeZone() {
                return TimeZone.getTimeZone("GMT+6");
            }
        };
        // year
        // with time zone
        String partitionName = "2020";
        PartitionField partitionField = SPEC_D_2.fields().get(0);
        String result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                DateType.DATETIME);
        Assertions.assertEquals("2020-01-01 06:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                DateType.DATE);
        Assertions.assertEquals("2020-01-01", result);
        // without time zone
        partitionField = SPEC_E_2.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                DateType.DATETIME);
        Assertions.assertEquals("2020-01-01 00:00:00", result);

        // month
        // with time zone
        partitionName = "2020-02";
        partitionField = SPEC_D_3.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                DateType.DATETIME);
        Assertions.assertEquals("2020-02-01 06:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                DateType.DATE);
        Assertions.assertEquals("2020-02-01", result);
        // without time zone
        partitionField = SPEC_E_3.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                DateType.DATETIME);
        Assertions.assertEquals("2020-02-01 00:00:00", result);

        // day
        // with time zone
        partitionName = "2020-01-02";
        partitionField = SPEC_D_4.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                DateType.DATETIME);
        Assertions.assertEquals("2020-01-02 06:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                DateType.DATE);
        Assertions.assertEquals("2020-01-02", result);
        // without time zone
        partitionField = SPEC_E_4.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                DateType.DATETIME);
        Assertions.assertEquals("2020-01-02 00:00:00", result);

        // hour
        partitionName = "2020-01-02-12";
        partitionField = SPEC_D_5.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                DateType.DATETIME);
        Assertions.assertEquals("2020-01-02 18:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                DateType.DATE);
        Assertions.assertEquals("2020-01-02", result);
        // without time zone
        partitionField = SPEC_E_5.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                DateType.DATETIME);
        Assertions.assertEquals("2020-01-02 12:00:00", result);
    }

    @Test
    public void testConvertPartitionExprToTerm() {
        TableName tableName = new TableName("db", "tbl");
        SlotRef slotRef = new SlotRef(tableName, "ts");

        // hour transform
        FunctionCallExpr hourExpr = new FunctionCallExpr("hour", Lists.newArrayList(slotRef));
        Term hourTerm = IcebergPartitionUtils.convertPartitionExprToTerm(hourExpr);
        Assertions.assertNotNull(hourTerm);

        // non-SlotRef child in FunctionCallExpr
        FunctionCallExpr badChildExpr = new FunctionCallExpr("day", Lists.newArrayList(new IntLiteral(1)));
        Assertions.assertThrows(SemanticException.class,
                () -> IcebergPartitionUtils.convertPartitionExprToTerm(badChildExpr));

        // unsupported expr type (IntLiteral)
        Assertions.assertThrows(SemanticException.class,
                () -> IcebergPartitionUtils.convertPartitionExprToTerm(new IntLiteral(1)));
    }

    @Test
    public void testNormalizePartitionExpr() {
        TableName tableName = new TableName("db", "tbl");
        SlotRef slotRef = new SlotRef(tableName, "ts");

        // identity transform returns quoted column name
        FunctionCallExpr identityExpr = new FunctionCallExpr("identity", Lists.newArrayList(slotRef));
        Assertions.assertEquals("`ts`", IcebergPartitionUtils.normalizePartitionExpr(identityExpr));

        // non-SlotRef child in FunctionCallExpr
        FunctionCallExpr badChildExpr = new FunctionCallExpr("day", Lists.newArrayList(new IntLiteral(1)));
        Assertions.assertThrows(SemanticException.class,
                () -> IcebergPartitionUtils.normalizePartitionExpr(badChildExpr));

        // unsupported transform (void)
        FunctionCallExpr voidExpr = new FunctionCallExpr("void", Lists.newArrayList(slotRef));
        Assertions.assertThrows(SemanticException.class,
                () -> IcebergPartitionUtils.normalizePartitionExpr(voidExpr));

        // unsupported expr type
        Assertions.assertThrows(SemanticException.class,
                () -> IcebergPartitionUtils.normalizePartitionExpr(new IntLiteral(1)));
    }

    @Test
    public void testGetPartitionExprSourceColumn() {
        TableName tableName = new TableName("db", "tbl");
        SlotRef slotRef = new SlotRef(tableName, "ts");

        // SlotRef returns column name directly
        Assertions.assertEquals("ts", IcebergPartitionUtils.getPartitionExprSourceColumn(slotRef));

        // FunctionCallExpr with SlotRef child
        FunctionCallExpr dayExpr = new FunctionCallExpr("day", Lists.newArrayList(slotRef));
        Assertions.assertEquals("ts", IcebergPartitionUtils.getPartitionExprSourceColumn(dayExpr));

        // non-SlotRef child in FunctionCallExpr
        FunctionCallExpr badChildExpr = new FunctionCallExpr("day", Lists.newArrayList(new IntLiteral(1)));
        Assertions.assertThrows(SemanticException.class,
                () -> IcebergPartitionUtils.getPartitionExprSourceColumn(badChildExpr));

        // unsupported expr type
        Assertions.assertThrows(SemanticException.class,
                () -> IcebergPartitionUtils.getPartitionExprSourceColumn(new IntLiteral(1)));
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }
}
