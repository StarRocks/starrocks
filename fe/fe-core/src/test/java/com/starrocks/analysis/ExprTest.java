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
import com.starrocks.common.exception.AnalysisException;
import org.junit.Assert;
import org.junit.Test;

public class ExprTest {
    @Test
    public void testUncheckedCastTo() throws AnalysisException {
        // uncheckedCastTo should return new object

        // date
        DateLiteral dateLiteral = new DateLiteral(2020, 4, 5, 12, 0, 5, 0);
        Assert.assertTrue(dateLiteral.getType().isDatetime());
        DateLiteral castLiteral = (DateLiteral) dateLiteral.uncheckedCastTo(Type.DATE);
        Assert.assertFalse(dateLiteral == castLiteral);
        Assert.assertTrue(dateLiteral.getType().isDatetime());
        Assert.assertTrue(castLiteral.getType().isDate());

        Assert.assertEquals(0, castLiteral.getHour());
        Assert.assertEquals(0, castLiteral.getMinute());
        Assert.assertEquals(0, castLiteral.getSecond());

        Assert.assertEquals(12, dateLiteral.getHour());
        Assert.assertEquals(0, dateLiteral.getMinute());
        Assert.assertEquals(5, dateLiteral.getSecond());

        DateLiteral dateLiteral2 = new DateLiteral(2020, 4, 5);
        Assert.assertTrue(dateLiteral2.getType().isDate());
        castLiteral = (DateLiteral) dateLiteral2.uncheckedCastTo(Type.DATETIME);
        Assert.assertFalse(dateLiteral2 == castLiteral);
        Assert.assertTrue(dateLiteral2.getType().isDate());
        Assert.assertTrue(castLiteral.getType().isDatetime());

        Assert.assertEquals(0, castLiteral.getHour());
        Assert.assertEquals(0, castLiteral.getMinute());
        Assert.assertEquals(0, castLiteral.getSecond());

        // float
        FloatLiteral floatLiteral = new FloatLiteral(0.1, Type.FLOAT);
        Assert.assertTrue(floatLiteral.getType().isFloat());
        FloatLiteral castFloatLiteral = (FloatLiteral) floatLiteral.uncheckedCastTo(Type.DOUBLE);
        Assert.assertTrue(floatLiteral.getType().isFloat());
        Assert.assertTrue(castFloatLiteral.getType().isDouble());
        Assert.assertFalse(floatLiteral == castFloatLiteral);
        FloatLiteral castFloatLiteral2 = (FloatLiteral) floatLiteral.uncheckedCastTo(Type.FLOAT);
        Assert.assertTrue(floatLiteral == castFloatLiteral2);

        // int
        IntLiteral intLiteral = new IntLiteral(200);
        Assert.assertTrue(intLiteral.getType().isSmallint());
        IntLiteral castIntLiteral = (IntLiteral) intLiteral.uncheckedCastTo(Type.INT);
        Assert.assertTrue(intLiteral.getType().isSmallint());
        Assert.assertTrue(castIntLiteral.getType().isInt());
        Assert.assertFalse(intLiteral == castIntLiteral);
        IntLiteral castIntLiteral2 = (IntLiteral) intLiteral.uncheckedCastTo(Type.SMALLINT);
        Assert.assertTrue(intLiteral == castIntLiteral2);

        // null
        NullLiteral nullLiternal = NullLiteral.create(Type.DATE);
        Assert.assertTrue(nullLiternal.getType().isDate());
        NullLiteral castNullLiteral = (NullLiteral) nullLiternal.uncheckedCastTo(Type.DATETIME);
        Assert.assertTrue(nullLiternal.getType().isDate());
        Assert.assertTrue(castNullLiteral.getType().isDatetime());
        Assert.assertFalse(nullLiternal == castNullLiteral);
        NullLiteral castNullLiteral2 = (NullLiteral) nullLiternal.uncheckedCastTo(Type.DATE);
        Assert.assertTrue(nullLiternal == castNullLiteral2);

        // string
        StringLiteral stringLiteral = new StringLiteral("abc");
        Assert.assertTrue(stringLiteral.getType().isVarchar());
        StringLiteral castStringLiteral = (StringLiteral) stringLiteral.uncheckedCastTo(Type.CHAR);
        Assert.assertTrue(stringLiteral.getType().isVarchar());
        Assert.assertTrue(castStringLiteral.getType().isChar());
        Assert.assertFalse(stringLiteral == castStringLiteral);
        StringLiteral castStringLiteral2 = (StringLiteral) stringLiteral.uncheckedCastTo(Type.VARCHAR);
        Assert.assertTrue(stringLiteral == castStringLiteral2);
    }
}
