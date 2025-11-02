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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ExprTest.java

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

package com.starrocks.analysis;

import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExprTest {
    @Test
    public void testUncheckedCastTo() throws AnalysisException {
        // uncheckedCastTo should return new object

        // date
        DateLiteral dateLiteral = new DateLiteral(2020, 4, 5, 12, 0, 5, 0);
        Assertions.assertTrue(dateLiteral.getType().isDatetime());
        DateLiteral castLiteral = (DateLiteral) dateLiteral.uncheckedCastTo(Type.DATE);
        Assertions.assertFalse(dateLiteral == castLiteral);
        Assertions.assertTrue(dateLiteral.getType().isDatetime());
        Assertions.assertTrue(castLiteral.getType().isDate());

        Assertions.assertEquals(0, castLiteral.getHour());
        Assertions.assertEquals(0, castLiteral.getMinute());
        Assertions.assertEquals(0, castLiteral.getSecond());

        Assertions.assertEquals(12, dateLiteral.getHour());
        Assertions.assertEquals(0, dateLiteral.getMinute());
        Assertions.assertEquals(5, dateLiteral.getSecond());

        DateLiteral dateLiteral2 = new DateLiteral(2020, 4, 5);
        Assertions.assertTrue(dateLiteral2.getType().isDate());
        castLiteral = (DateLiteral) dateLiteral2.uncheckedCastTo(Type.DATETIME);
        Assertions.assertFalse(dateLiteral2 == castLiteral);
        Assertions.assertTrue(dateLiteral2.getType().isDate());
        Assertions.assertTrue(castLiteral.getType().isDatetime());

        Assertions.assertEquals(0, castLiteral.getHour());
        Assertions.assertEquals(0, castLiteral.getMinute());
        Assertions.assertEquals(0, castLiteral.getSecond());

        // float
        FloatLiteral floatLiteral = new FloatLiteral(0.1, Type.FLOAT);
        Assertions.assertTrue(floatLiteral.getType().isFloat());
        FloatLiteral castFloatLiteral = (FloatLiteral) floatLiteral.uncheckedCastTo(Type.DOUBLE);
        Assertions.assertTrue(floatLiteral.getType().isFloat());
        Assertions.assertTrue(castFloatLiteral.getType().isDouble());
        Assertions.assertFalse(floatLiteral == castFloatLiteral);
        FloatLiteral castFloatLiteral2 = (FloatLiteral) floatLiteral.uncheckedCastTo(Type.FLOAT);
        Assertions.assertTrue(floatLiteral == castFloatLiteral2);

        // int
        IntLiteral intLiteral = new IntLiteral(200);
        Assertions.assertTrue(intLiteral.getType().isSmallint());
        IntLiteral castIntLiteral = (IntLiteral) intLiteral.uncheckedCastTo(Type.INT);
        Assertions.assertTrue(intLiteral.getType().isSmallint());
        Assertions.assertTrue(castIntLiteral.getType().isInt());
        Assertions.assertFalse(intLiteral == castIntLiteral);
        IntLiteral castIntLiteral2 = (IntLiteral) intLiteral.uncheckedCastTo(Type.SMALLINT);
        Assertions.assertTrue(intLiteral == castIntLiteral2);

        // null
        NullLiteral nullLiternal = NullLiteral.create(Type.DATE);
        Assertions.assertTrue(nullLiternal.getType().isDate());
        NullLiteral castNullLiteral = (NullLiteral) nullLiternal.uncheckedCastTo(Type.DATETIME);
        Assertions.assertTrue(nullLiternal.getType().isDate());
        Assertions.assertTrue(castNullLiteral.getType().isDatetime());
        Assertions.assertFalse(nullLiternal == castNullLiteral);
        NullLiteral castNullLiteral2 = (NullLiteral) nullLiternal.uncheckedCastTo(Type.DATE);
        Assertions.assertTrue(nullLiternal == castNullLiteral2);

        // string
        StringLiteral stringLiteral = new StringLiteral("abc");
        Assertions.assertTrue(stringLiteral.getType().isVarchar());
        StringLiteral castStringLiteral = (StringLiteral) stringLiteral.uncheckedCastTo(Type.CHAR);
        Assertions.assertTrue(stringLiteral.getType().isVarchar());
        Assertions.assertTrue(castStringLiteral.getType().isChar());
        Assertions.assertFalse(stringLiteral == castStringLiteral);
        StringLiteral castStringLiteral2 = (StringLiteral) stringLiteral.uncheckedCastTo(Type.VARCHAR);
        Assertions.assertTrue(stringLiteral == castStringLiteral2);
    }
}
