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

package com.starrocks.common.util;

import com.starrocks.catalog.PartitionKey;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.MaxLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class PartitionKeySerializerTest {

    private Type originalMaxLiteralType;

    @BeforeEach
    public void setUp() {
        originalMaxLiteralType = MaxLiteral.MAX_VALUE.getType();
    }

    @AfterEach
    public void tearDown() {
        MaxLiteral.MAX_VALUE.setType(originalMaxLiteralType);
    }

    @Test
    public void testWriteAndReadPartitionKey() throws Exception {
        PartitionKey partitionKey = new PartitionKey();
        IntLiteral intLiteral = new IntLiteral(42L);
        partitionKey.pushColumn(intLiteral, PrimitiveType.INT);

        LargeIntLiteral largeIntLiteral = new LargeIntLiteral("123456789012345678901234567890");
        partitionKey.pushColumn(largeIntLiteral, PrimitiveType.LARGEINT);

        DateLiteral dateLiteral = new DateLiteral(2023, 8, 12);
        partitionKey.pushColumn(dateLiteral, PrimitiveType.DATE);

        DateLiteral dateTimeLiteral = new DateLiteral(2024, 1, 2, 3, 4, 5, 0);
        partitionKey.pushColumn(dateTimeLiteral, PrimitiveType.DATETIME);

        StringLiteral stringLiteral = new StringLiteral("partition_value");
        partitionKey.pushColumn(stringLiteral, PrimitiveType.VARCHAR);

        partitionKey.pushColumn(MaxLiteral.MAX_VALUE, PrimitiveType.BIGINT);

        PartitionKey restoredKey = serializeAndDeserialize(partitionKey);

        Assertions.assertEquals(partitionKey.getTypes(), restoredKey.getTypes());

        List<LiteralExpr> restoredLiterals = restoredKey.getKeys();
        Assertions.assertEquals(6, restoredLiterals.size());

        Assertions.assertEquals(intLiteral.getValue(), ((IntLiteral) restoredLiterals.get(0)).getValue());
        Assertions.assertEquals(largeIntLiteral.getValue(), ((LargeIntLiteral) restoredLiterals.get(1)).getValue());

        DateLiteral restoredDate = (DateLiteral) restoredLiterals.get(2);
        Assertions.assertTrue(restoredDate.getType().isDate());
        Assertions.assertEquals(dateLiteral.getYear(), restoredDate.getYear());
        Assertions.assertEquals(dateLiteral.getMonth(), restoredDate.getMonth());
        Assertions.assertEquals(dateLiteral.getDay(), restoredDate.getDay());

        DateLiteral restoredDateTime = (DateLiteral) restoredLiterals.get(3);
        Assertions.assertTrue(restoredDateTime.getType().isDatetime());
        Assertions.assertEquals(dateTimeLiteral.getYear(), restoredDateTime.getYear());
        Assertions.assertEquals(dateTimeLiteral.getMonth(), restoredDateTime.getMonth());
        Assertions.assertEquals(dateTimeLiteral.getDay(), restoredDateTime.getDay());
        Assertions.assertEquals(dateTimeLiteral.getHour(), restoredDateTime.getHour());
        Assertions.assertEquals(dateTimeLiteral.getMinute(), restoredDateTime.getMinute());
        Assertions.assertEquals(dateTimeLiteral.getSecond(), restoredDateTime.getSecond());

        Assertions.assertEquals(stringLiteral.getStringValue(),
                ((StringLiteral) restoredLiterals.get(4)).getStringValue());

        LiteralExpr restoredMax = restoredLiterals.get(5);
        Assertions.assertSame(MaxLiteral.MAX_VALUE, restoredMax);
        Assertions.assertEquals(PrimitiveType.BIGINT, restoredKey.getTypes().get(5));
        Assertions.assertEquals(PrimitiveType.BIGINT, restoredMax.getType().getPrimitiveType());
    }

    @Test
    public void testWriteFailsWhenKeyTypeSizeMismatch() {
        PartitionKey partitionKey = new PartitionKey();
        partitionKey.pushColumn(new IntLiteral(1L), PrimitiveType.INT);
        partitionKey.getKeys().add(new IntLiteral(2L));

        Assertions.assertThrows(IOException.class, () -> PartitionKeySerializer.write(
                new DataOutputStream(new ByteArrayOutputStream()), partitionKey));
    }

    private PartitionKey serializeAndDeserialize(PartitionKey partitionKey) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            PartitionKeySerializer.write(out, partitionKey);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            return PartitionKeySerializer.read(in);
        }
    }
}
