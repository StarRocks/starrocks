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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class PaimonGlobalIndexRequestTest {
    @Test
    public void testJsonAndTransportRoundTrip() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.GE, column, ConstantOperator.createInt(10));
        PaimonGlobalIndexRequest request = PaimonGlobalIndexRequest.create(
                42L, new IndexCondition(predicate, Map.of("k", ConnectorIndexType.RANGE)));

        PaimonGlobalIndexRequest fromJson = PaimonGlobalIndexRequest.parse(request.toJson());
        PaimonGlobalIndexRequest fromTransport =
                PaimonGlobalIndexRequest.parseTransport(request.toTransportString());

        Assertions.assertEquals(42L, fromJson.getSnapshotId());
        Assertions.assertEquals(PaimonGlobalIndexRequest.PREDICATE_VERSION, fromJson.getVersion());
        Assertions.assertFalse(fromJson.isTopN());
        Assertions.assertEquals(Map.of("k", ConnectorIndexType.RANGE), fromJson.getRequiredIndexes());
        Assertions.assertEquals(42L, fromTransport.getSnapshotId());
        Assertions.assertEquals(Map.of("k", ConnectorIndexType.RANGE), fromTransport.getRequiredIndexes());
        Assertions.assertEquals(request.toJson(), fromJson.toJson());
        Assertions.assertEquals(request.toJson(), fromTransport.toJson());
        Assertions.assertTrue(fromTransport.toJson().contains("\"v\":10"));
        Assertions.assertFalse(fromTransport.toJson().contains("\"v\":10.0"));
        Assertions.assertFalse(request.toTransportString().contains("\"snapshotId\""));
    }

    @Test
    public void testVectorTopNRoundTrip() {
        ColumnRefOperator vector = new ColumnRefOperator(2, ArrayType.ARRAY_FLOAT, "embedding", true);
        ArrayOperator query = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        CallOperator score = new CallOperator(FunctionSet.APPROX_INNER_PRODUCT, FloatType.FLOAT,
                List.of(query, vector));
        TopNIndexCondition condition = new TopNIndexCondition(null, score,
                Map.of(vector.getName(), ConnectorIndexType.VECTOR), 10, 2, false);

        PaimonGlobalIndexRequest request = PaimonGlobalIndexRequest.parse(
                PaimonGlobalIndexRequest.create(43L, condition).toJson());

        Assertions.assertTrue(request.isTopN());
        Assertions.assertEquals(PaimonGlobalIndexRequest.TOP_N_VERSION, request.getVersion());
        Assertions.assertEquals(12, request.getLocalLimit());
        Assertions.assertFalse(request.isAscending());
        Assertions.assertEquals(Map.of("embedding", ConnectorIndexType.VECTOR), request.getRequiredIndexes());
        Assertions.assertEquals("ca", request.getScoreExpression().get("o").getAsString());
        Assertions.assertEquals("embedding", request.getScoreExpression().getAsJsonArray("a")
                .get(0).getAsJsonObject().get("n").getAsString());
        Assertions.assertNull(request.getPredicate());
    }

    @Test
    public void testRejectsInvalidEnvelope() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parse("{\"version\":3,\"snapshotId\":1}"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parseTransport("not-base64"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parse(
                        "{\"version\":1,\"snapshotId\":1,\"predicate\":{},"
                                + "\"indexes\":{\"k\":\"unknown\"}}"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parse(
                        "{\"version\":2,\"snapshotId\":1,\"kind\":\"top_n\",\"predicate\":{},"
                                + "\"indexes\":{\"embedding\":\"vector\"}}"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parse(
                        "{\"version\":1,\"snapshotId\":1,\"kind\":\"top_n\",\"predicate\":{},"
                                + "\"indexes\":{\"k\":\"range\"}}"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parse(
                        "{\"version\":2,\"snapshotId\":1,\"kind\":\"top_n\","
                                + "\"scoreExpression\":{\"o\":\"ca\",\"f\":\"approx_l2_distance\","
                                + "\"a\":[{},{}]},\"localLimit\":1,\"ascending\":false,"
                                + "\"indexes\":{\"embedding\":\"vector\"}}"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parse(
                        "{\"version\":2,\"snapshotId\":1,\"kind\":\"top_n\","
                                + "\"scoreExpression\":{\"o\":\"ca\",\"f\":\"unsupported\","
                                + "\"a\":[{},{}]},\"localLimit\":1,\"ascending\":true,"
                                + "\"indexes\":{\"embedding\":\"vector\"}}"));
    }
}
