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

package com.starrocks.connector.trino;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveView;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.persist.gson.GsonUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TrinoViewTest {
    @Mocked
    private Table table;

    @Test
    public void testTrinoViewDefinitionBasic() {
        String json = "{\"originalSql\":\"SELECT *\\nFROM\\n  (\\n   SELECT *\\n   FROM\\n     emps\\n) \\n\",\"catalog\":" +
                "\"hive\",\"schema\":\"test\",\"columns\":[{\"name\":\"empid\",\"type\":\"integer\"},{\"name\":\"deptno\",\"" +
                "type\":\"integer\"},{\"name\":\"salary\",\"type\":\"double\"},{\"name\":\"new_col\",\"type\":\"varchar\"}]," +
                "\"owner\":\"xxxx\",\"runAsInvoker\":false}";
        TrinoViewDefinition trinoViewDefinition = GsonUtils.GSON.fromJson(json,
                TrinoViewDefinition.class);
        Assert.assertEquals(trinoViewDefinition.getOriginalSql(), "SELECT *\n" +
                "FROM\n" +
                "  (\n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     emps\n" +
                ") \n");
        Assert.assertEquals(trinoViewDefinition.getColumns().size(), 4);
        Assert.assertEquals(trinoViewDefinition.getColumns().get(0).getName(), "empid");
        Assert.assertEquals(trinoViewDefinition.getColumns().get(0).getType(), "integer");
        Assert.assertEquals(trinoViewDefinition.getColumns().get(3).getName(), "new_col");
        Assert.assertEquals(trinoViewDefinition.getColumns().get(3).getType(), "varchar");
    }

    @Test
    public void testTrinoViewDecodeBasic() {
        String viewOriginalText = "/* Presto View: eyJvcmlnaW5hbFNxbCI6IlNFTEVDVCAqXG5GUk9NXG4gIChcbiAgIFNFTEVDVCAqXG4gI" +
                "CBGUk9NXG4gICAgIGVtcHNcbikgXG4iLCJjYXRhbG9nIjoiaGl2ZSIsInNjaGVtYSI6Inl3YiIsImNvbHVtbnMiOlt7Im5hbWUiOiJl" +
                "bXBpZCIsInR5cGUiOiJpbnRlZ2VyIn0seyJuYW1lIjoiZGVwdG5vIiwidHlwZSI6ImludGVnZXIifSx7Im5hbWUiOiJzYWxhcnkiLC" +
                "J0eXBlIjoiZG91YmxlIn0seyJuYW1lIjoibmV3X2NvbCIsInR5cGUiOiJ2YXJjaGFyIn1dLCJvd25lciI6IndlbmJvIiwicnVuQXNJ" +
                "bnZva2VyIjpmYWxzZX0= */";
        new Expectations() {
            {
                table.getViewOriginalText();
                result = viewOriginalText;

                table.getDbName();
                result = "testDb";
            }
        };

        HiveView hiveView = HiveMetastoreApiConverter.toHiveView(table, "hive");
        Assert.assertEquals(hiveView.getInlineViewDef(), "SELECT *\n" +
                "FROM\n" +
                "  (\n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     emps\n" +
                ") \n");
        Assert.assertEquals(hiveView.getFullSchema().size(), 4);
        Assert.assertEquals(hiveView.getFullSchema().get(0).getName(), "empid");
        Assert.assertEquals(hiveView.getFullSchema().get(0).getType(), ScalarType.INT);
        Assert.assertEquals(hiveView.getFullSchema().get(2).getName(), "salary");
        Assert.assertEquals(hiveView.getFullSchema().get(2).getType(), ScalarType.DOUBLE);
        Assert.assertEquals(hiveView.getFullSchema().get(3).getName(), "new_col");
        Assert.assertEquals(hiveView.getFullSchema().get(3).getType(), ScalarType.createDefaultCatalogString());
    }

    @Test
    public void testTrinoViewDecodeAllType() {
        String viewOriginalText = "/* Presto View: eyJvcmlnaW5hbFNxbCI6IlNFTEVDVCAqXG5GUk9NXG4gIChcbiAgIFNFTEVDVCAqXG4g" +
                "ICBGUk9NXG4gICAgIGFsbF9kYXRhX3R5cGVzXG4pIFxuIiwiY2F0YWxvZyI6ImhpdmUiLCJzY2hlbWEiOiJ5d2IiLCJjb2x1bW5zIj" +
                "pbeyJuYW1lIjoiaWQiLCJ0eXBlIjoiaW50ZWdlciJ9LHsibmFtZSI6InNtYWxsaW50X2NvbCIsInR5cGUiOiJzbWFsbGludCJ9LHsib" +
                "mFtZSI6InRpbnlpbnRfY29sIiwidHlwZSI6InRpbnlpbnQifSx7Im5hbWUiOiJiaWdpbnRfY29sIiwidHlwZSI6ImJpZ2ludCJ9LHsi" +
                "bmFtZSI6ImZsb2F0X2NvbCIsInR5cGUiOiJyZWFsIn0seyJuYW1lIjoiZG91YmxlX2NvbCIsInR5cGUiOiJkb3VibGUifSx7Im5hbWU" +
                "iOiJib29sZWFuX2NvbCIsInR5cGUiOiJib29sZWFuIn0seyJuYW1lIjoic3RyaW5nX2NvbCIsInR5cGUiOiJ2YXJjaGFyIn0seyJuYW" +
                "1lIjoiY2hhcl9jb2wiLCJ0eXBlIjoiY2hhcigxMCkifSx7Im5hbWUiOiJ2YXJjaGFyX2NvbCIsInR5cGUiOiJ2YXJjaGFyKDIwKSJ9L" +
                "HsibmFtZSI6ImJpbmFyeV9jb2wiLCJ0eXBlIjoidmFyYmluYXJ5In0seyJuYW1lIjoiZGVjaW1hbF9jb2wiLCJ0eXBlIjoiZGVjaW1h" +
                "bCgxMCwyKSJ9LHsibmFtZSI6ImRhdGVfY29sIiwidHlwZSI6ImRhdGUifSx7Im5hbWUiOiJ0aW1lc3RhbXBfY29sIiwidHlwZSI6InR" +
                "pbWVzdGFtcCgzKSJ9LHsibmFtZSI6ImFycmF5X2NvbCIsInR5cGUiOiJhcnJheShpbnRlZ2VyKSJ9LHsibmFtZSI6Im1hcF9jb2wiLC" +
                "J0eXBlIjoibWFwKHZhcmNoYXIsaW50ZWdlcikifSx7Im5hbWUiOiJzdHJ1Y3RfY29sIiwidHlwZSI6InJvdyhcImZpZWxkMVwiIGlud" +
                "GVnZXIsXCJmaWVsZDJcIiB2YXJjaGFyKSJ9XSwib3duZXIiOiJ3ZW5ibyIsInJ1bkFzSW52b2tlciI6ZmFsc2V9 */";
        new Expectations() {
            {
                table.getViewOriginalText();
                result = viewOriginalText;

                table.getDbName();
                result = "testDb";
            }
        };

        HiveView hiveView = HiveMetastoreApiConverter.toHiveView(table, "hive");
        Assert.assertEquals(hiveView.getInlineViewDef(), "SELECT *\n" +
                "FROM\n" +
                "  (\n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     all_data_types\n" +
                ") \n");
        Assert.assertEquals(hiveView.getFullSchema().size(), 17);
        Assert.assertEquals(hiveView.getFullSchema().get(0).getName(), "id");
        Assert.assertEquals(hiveView.getFullSchema().get(0).getType(), ScalarType.INT);
        Assert.assertEquals(hiveView.getFullSchema().get(4).getName(), "float_col");
        Assert.assertEquals(hiveView.getFullSchema().get(4).getType(), ScalarType.FLOAT);
        Assert.assertEquals(hiveView.getFullSchema().get(5).getName(), "double_col");
        Assert.assertEquals(hiveView.getFullSchema().get(5).getType(), ScalarType.DOUBLE);
        Assert.assertEquals(hiveView.getFullSchema().get(9).getName(), "varchar_col");
        Assert.assertEquals(hiveView.getFullSchema().get(9).getType(), ScalarType.createVarcharType(20));
        Assert.assertEquals(hiveView.getFullSchema().get(10).getName(), "binary_col");
        Assert.assertEquals(hiveView.getFullSchema().get(10).getType(), ScalarType.VARBINARY);
        Assert.assertEquals(hiveView.getFullSchema().get(13).getName(), "timestamp_col");
        Assert.assertEquals(hiveView.getFullSchema().get(13).getType(), ScalarType.DATETIME);
        Assert.assertEquals(hiveView.getFullSchema().get(14).getName(), "array_col");
        Assert.assertEquals(hiveView.getFullSchema().get(14).getType(), ScalarType.ARRAY_INT);
        Assert.assertEquals(hiveView.getFullSchema().get(15).getName(), "map_col");
        Assert.assertEquals(hiveView.getFullSchema().get(15).getType(),
                new MapType(ScalarType.createDefaultCatalogString(), ScalarType.INT));
        Assert.assertEquals(hiveView.getFullSchema().get(16).getName(), "struct_col");
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(new StructField("field1", ScalarType.INT));
        structFields.add(new StructField("field2", ScalarType.createDefaultCatalogString()));
        Assert.assertEquals(hiveView.getFullSchema().get(16).getType(), new StructType(structFields));
    }

    @Test
    public void testSchemaForTrinoView() {
        List<TrinoViewDefinition.ViewColumn> columns = new ArrayList<>();
        columns.add(new TrinoViewDefinition.ViewColumn("tinyint_col", "tinyint"));
        columns.add(new TrinoViewDefinition.ViewColumn("smallint_col", "smallint"));
        columns.add(new TrinoViewDefinition.ViewColumn("int_col", "int"));
        columns.add(new TrinoViewDefinition.ViewColumn("bigint_col", "bigint"));
        columns.add(new TrinoViewDefinition.ViewColumn("float_col", "real"));
        columns.add(new TrinoViewDefinition.ViewColumn("double_col", "double"));
        columns.add(new TrinoViewDefinition.ViewColumn("decimal_col", "decimal(10,2)"));
        columns.add(new TrinoViewDefinition.ViewColumn("varchar_col", "varchar(20)"));
        columns.add(new TrinoViewDefinition.ViewColumn("char_col", "char(10)"));
        columns.add(new TrinoViewDefinition.ViewColumn("string_col", "varchar"));
        columns.add(new TrinoViewDefinition.ViewColumn("boolean_col", "boolean"));
        columns.add(new TrinoViewDefinition.ViewColumn("timestamp_col", "timestamp"));
        columns.add(new TrinoViewDefinition.ViewColumn("array_col", "array(integer)"));
        columns.add(new TrinoViewDefinition.ViewColumn("map_col", "map(varchar,integer)"));
        columns.add(new TrinoViewDefinition.ViewColumn("struct_col", "row(\"field1\" integer, \"field2\" varchar)"));
        TrinoViewDefinition trinoViewDefinition = new TrinoViewDefinition("SELECT * FROM all_data_types", "hive",
                "test", columns, null, "xxx", false);

        List<Column> columnList = HiveMetastoreApiConverter.toFullSchemasForTrinoView(table, trinoViewDefinition);
        Assert.assertEquals(columnList.size(), 15);
        Assert.assertEquals(columnList.get(0).getType(), ScalarType.TINYINT);
        Assert.assertEquals(columnList.get(1).getType(), ScalarType.SMALLINT);
        Assert.assertEquals(columnList.get(2).getType(), ScalarType.INT);
        Assert.assertEquals(columnList.get(3).getType(), ScalarType.BIGINT);
        Assert.assertEquals(columnList.get(4).getType(), ScalarType.FLOAT);
        Assert.assertEquals(columnList.get(5).getType(), ScalarType.DOUBLE);
        Assert.assertEquals(columnList.get(6).getType(), ScalarType.createDecimalV3NarrowestType(10, 2));
        Assert.assertEquals(columnList.get(7).getType(), ScalarType.createVarcharType(20));
        Assert.assertEquals(columnList.get(8).getType(), ScalarType.createCharType(10));
        Assert.assertEquals(columnList.get(9).getType(), ScalarType.createDefaultCatalogString());
        Assert.assertEquals(columnList.get(10).getType(), ScalarType.BOOLEAN);
        Assert.assertEquals(columnList.get(11).getType(), ScalarType.DATETIME);
        Assert.assertEquals(columnList.get(12).getType(), ScalarType.ARRAY_INT);
        Assert.assertEquals(columnList.get(13).getType(),
                new MapType(ScalarType.createDefaultCatalogString(), ScalarType.INT));
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(new StructField("field1", ScalarType.INT));
        structFields.add(new StructField("field2", ScalarType.createDefaultCatalogString()));
        Assert.assertEquals(columnList.get(14).getType(), new StructType(structFields));
    }
}
