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

package com.starrocks.qe;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShowResultSetMetaDataTest {
    @Test
    public void testNormal() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder().build();
        Assertions.assertEquals(0, metaData.getColumnCount());

        metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("col1", ScalarType.createType(PrimitiveType.INT)))
                .addColumn(new Column("col2", ScalarType.createType(PrimitiveType.INT)))
                .build();

        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("col1", metaData.getColumn(0).getName());
        Assertions.assertEquals("col2", metaData.getColumn(1).getName());
    }

    @Test
    public void testOutBound() {
        assertThrows(IndexOutOfBoundsException.class, () -> {
            ShowResultSetMetaData metaData = ShowResultSetMetaData.builder().build();
            metaData.getColumn(1);
            Assertions.fail("No exception throws.");
        });
    }
}