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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class PaimonGlobalIndexRequestTest {
    @Test
    public void testSchemaBoundLiteralValidation() {
        TableSchema schema = new TableSchema(3L, List.of(new DataField(7, "k", DataTypes.TINYINT())),
                7, List.of(), List.of(), Map.of(), null);
        for (String value : List.of("-128", "127")) {
            requestWithPredicate(binary("tinyint", "tinyint", value)).validatePredicate(schema);
        }
        for (String predicate : List.of(
                binary("tinyint", "tinyint", "128"), binary("tinyint", "tinyint", "-129"),
                binary("tinyint", "tinyint", "1.5"), binary("tinyint", "tinyint", "9223372036854775808"),
                binary("tinyint", "tinyint", "1e1000000000"), binary("tinyint", "tinyint", "-1e1000000000"),
                binary("tinyint", "tinyint", "1e-1000000000"),
                binary("tinyint", "varchar(" + "9".repeat(100000) + ")", "1"),
                binary("tinyint", "int", "1"), binary("int", "int", "1"),
                binary("tinyint", "varchar", "\"bad\""), binary("tinyint", "tinyint", "null"),
                "{\"o\":\"ca\",\"f\":\"starts_with\",\"a\":[{\"o\":\"cr\",\"n\":\"k\",\"t\":\"tinyint\"},"
                        + "{\"o\":\"co\",\"t\":\"tinyint\",\"v\":1}]}",
                "{\"o\":\"ip\",\"ng\":false,\"c\":{}}")) {
            Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(2), () ->
                    Assertions.assertThrows(IllegalArgumentException.class,
                            () -> requestWithPredicate(predicate).validatePredicate(schema), predicate));
        }
        TableSchema wrongName = new TableSchema(3L, List.of(new DataField(7, "renamed", DataTypes.TINYINT())),
                7, List.of(), List.of(), Map.of(), null);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> requestWithPredicate(binary("tinyint", "tinyint", "1")).validatePredicate(wrongName));
        TableSchema floatSchema = new TableSchema(3L, List.of(new DataField(7, "k", DataTypes.FLOAT())),
                7, List.of(), List.of(), Map.of(), null);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> requestWithPredicate(binary("float", "float", "1e39")).validatePredicate(floatSchema));
    }

    private static String binary(String columnType, String literalType, String value) {
        return "{\"o\":\"b\",\"b\":\"EQ\",\"c\":[{\"o\":\"cr\",\"t\":\"" + columnType
                + "\",\"n\":\"k\"},{\"o\":\"co\",\"t\":\"" + literalType + "\",\"v\":" + value + "}]}";
    }

    private static PaimonGlobalIndexRequest requestWithPredicate(String predicate) {
        return PaimonGlobalIndexRequest.parse("{\"version\":3,\"snapshotId\":42,\"schemaId\":3,"
                + "\"schemaTimeMillis\":1000,\"tablePath\":\"file:/tmp/orders\",\"branch\":\"main\","
                + "\"indexes\":{\"k\":{\"fieldId\":7,\"provider\":\"btree\"}},\"predicate\":" + predicate + "}");
    }

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
