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

package com.starrocks.connector.paimon;

import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class PaimonGlobalIndexRequestTest {
    @Test
    public void testJsonAndTransportRoundTrip() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.GE, column, ConstantOperator.createInt(10));
        PaimonGlobalIndexRequest request = PaimonGlobalIndexRequest.create(
                "file:/tmp/orders", "main", 42L, 3L, 1000L, new IndexCondition(predicate, Map.of(1, 7)));

        PaimonGlobalIndexRequest fromJson = PaimonGlobalIndexRequest.parse(request.toJson());
        PaimonGlobalIndexRequest fromTransport =
                PaimonGlobalIndexRequest.parseTransport(request.toTransportString());

        Assertions.assertEquals(42L, fromJson.getSnapshotId());
        Assertions.assertTrue(fromJson.matchesSchema(3L, 1000L));
        Assertions.assertFalse(fromJson.matchesSchema(4L, 1000L));
        Assertions.assertFalse(fromJson.matchesSchema(3L, 1001L));
        Assertions.assertEquals(Map.of(7, IndexType.RANGE), fromJson.getRequiredIndexes());
        Assertions.assertEquals(42L, fromTransport.getSnapshotId());
        Assertions.assertEquals(Map.of(7, IndexType.RANGE), fromTransport.getRequiredIndexes());
        Assertions.assertEquals(request.toJson(), fromJson.toJson());
        Assertions.assertEquals(request.toJson(), fromTransport.toJson());
        Assertions.assertTrue(fromTransport.toJson().contains("\"v\":10"));
        Assertions.assertFalse(fromTransport.toJson().contains("\"v\":10.0"));
        Assertions.assertFalse(request.toTransportString().contains("\"snapshotId\""));
    }

    @Test
    public void testRejectsInvalidEnvelope() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parse("{\"version\":2,\"snapshotId\":1}"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parseTransport("not-base64"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parse(
                        "{\"version\":1,\"snapshotId\":1,\"predicate\":{},"
                                + "\"indexes\":{\"k\":\"unknown\"}}"));
    }

    @Test
    public void testRejectsMissingOrMismatchedBindings() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "renamed", true);
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ, column, ConstantOperator.createInt(10));
        PaimonGlobalIndexRequest request = PaimonGlobalIndexRequest.create(
                "file:/tmp/orders", "main", 42L, 3L, 1000L, new IndexCondition(predicate, Map.of(1, 700)));
        String json = request.toJson();
        Assertions.assertEquals(Map.of(700, IndexType.RANGE), request.getRequiredIndexes());
        for (String invalid : java.util.List.of(
                json.replace("\"fieldId\":700,", ""),
                json.replace("\"snapshotId\":42,", ""),
                json.replace("\"schemaId\":3,", ""),
                json.replace("\"schemaTimeMillis\":1000,", ""),
                json.replace("\"schemaId\":3", "\"schemaId\":-1"),
                json.replace("\"schemaTimeMillis\":1000", "\"schemaTimeMillis\":-1"),
                json.replace("\"main\"", "\"branch1\""),
                json.replace("\"btree\"", "\"bitmap\""))) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonGlobalIndexRequest.parse(invalid));
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonGlobalIndexRequest.create(
                "file:/tmp/orders", "main", 42L, 3L, 1000L, new IndexCondition(predicate)));
    }
}
