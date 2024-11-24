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
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class StructFieldDescTest {
    @Test
    public void testAnalyze() {
        StructField field1 = new StructField("v1", Type.INT);
        StructField field2 = new StructField("v2", Type.VARCHAR);
        StructField field3 = new StructField("v3", Type.INT);

        Assert.assertEquals("StructField[name='v3', type=INT, position=0, fieldId=-1, fieldPhysicalName='']",
                field3.toString());
        StructField unnamedField = new StructField(null, Type.VARCHAR);
        Assert.assertEquals("StructField[name='', type=VARCHAR, position=0, fieldId=-1, fieldPhysicalName='']",
                unnamedField.toString());

        StructType subStructType = new StructType(Lists.newArrayList(field2, field3));
        StructField field4 = new StructField("v4", subStructType);
        StructType type = new StructType(Lists.newArrayList(field1, field4));

        Column structCol1 = new Column("structCol1", type);

        Type addType = ScalarType.createType(PrimitiveType.INT);
        TypeDef addTypeDef = new TypeDef(addType);
        Column intCol1 = new Column("intCol1", addType);

        StructFieldDesc dropFieldDesc1 = new StructFieldDesc("v2", Lists.newArrayList("v1"), null, null);

        // base column not exist;
        Assertions.assertThrows(AnalysisException.class, () -> dropFieldDesc1.analyze(null, true));
    
        // base column is not struct column
        Assertions.assertThrows(AnalysisException.class, () -> dropFieldDesc1.analyze(intCol1, true));

        // nested field is not struct
        Assertions.assertThrows(AnalysisException.class, () -> dropFieldDesc1.analyze(structCol1, true));

        // drop field is not exist
        StructFieldDesc dropFieldDesc2 = new StructFieldDesc("v1", Lists.newArrayList("v6"), null, null);
        Assertions.assertThrows(AnalysisException.class, () -> dropFieldDesc2.analyze(structCol1, true));

        // normal drop field
        StructFieldDesc dropFieldDesc3 = new StructFieldDesc("v2", Lists.newArrayList("v4"), null, null);
        Assertions.assertDoesNotThrow(() -> dropFieldDesc3.analyze(structCol1, true));        

        // add exist field
        StructFieldDesc addFieldDesc1 = new StructFieldDesc("v2", Lists.newArrayList("v4"), addTypeDef, null);
        Assertions.assertThrows(AnalysisException.class, () -> addFieldDesc1.analyze(structCol1, false));

        // type not exist
        StructFieldDesc addFieldDesc2 = new StructFieldDesc("v5", Lists.newArrayList("v6"), null, null);
        Assertions.assertThrows(AnalysisException.class, () -> addFieldDesc2.analyze(structCol1, false));

        // normal add field
        StructFieldDesc addFieldDesc3 = new StructFieldDesc("v5", Lists.newArrayList("v4"), addTypeDef, null);
        Assertions.assertDoesNotThrow(() -> addFieldDesc3.analyze(structCol1, false));

    }
}