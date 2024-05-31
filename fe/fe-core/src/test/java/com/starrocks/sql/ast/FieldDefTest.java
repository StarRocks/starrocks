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

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class FieldDefTest {
    @Test
    public void testAnalyze() {
        StructField field1 = new StructField("v1", Type.INT);
        StructField field2 = new StructField("v2", Type.VARCHAR);
        StructField field3 = new StructField("v3", Type.INT);

        StructType subStructType = new StructType(Lists.newArrayList(field2, field3));
        StructField field4 = new StructField("v4", subStructType);
        StructType type = new StructType(Lists.newArrayList(field1, field4));

        Column structCol1 = new Column("structCol1", type);

        Type addType = ScalarType.createType(PrimitiveType.INT);
        TypeDef addTypeDef = new TypeDef(addType);
        Column intCol1 = new Column("intCol1", addType);

        FieldDef dropFieldDef1 = new FieldDef("v2", Lists.newArrayList("v1"), null, null);

        // base column not exist;
        Assertions.assertThrows(AnalysisException.class, () -> dropFieldDef1.analyze(null, true));
    
        // base column is not struct column
        Assertions.assertThrows(AnalysisException.class, () -> dropFieldDef1.analyze(intCol1, true));

        // nested field is not struct
        Assertions.assertThrows(AnalysisException.class, () -> dropFieldDef1.analyze(structCol1, true));

        // drop field is not exist
        FieldDef dropFieldDef2 = new FieldDef("v1", Lists.newArrayList("v4"), null, null);
        Assertions.assertThrows(AnalysisException.class, () -> dropFieldDef2.analyze(structCol1, true));

        // normal drop field
        FieldDef dropFieldDef3 = new FieldDef("v2", Lists.newArrayList("v4"), null, null);
        Assertions.assertDoesNotThrow(() -> dropFieldDef3.analyze(structCol1, true));        

        // add exist field
        FieldDef addFieldDef1 = new FieldDef("v2", Lists.newArrayList("v4"), addTypeDef, null);
        Assertions.assertThrows(AnalysisException.class, () -> addFieldDef1.analyze(structCol1, false));

        // type not exist
        FieldDef addFieldDef2 = new FieldDef("v5", Lists.newArrayList("v4"), null, null);
        Assertions.assertThrows(AnalysisException.class, () -> addFieldDef2.analyze(structCol1, false));

        // normal add field
        FieldDef addFieldDef3 = new FieldDef("v5", Lists.newArrayList("v4"), addTypeDef, null);
        Assertions.assertDoesNotThrow(() -> addFieldDef3.analyze(structCol1, false));

    }
}