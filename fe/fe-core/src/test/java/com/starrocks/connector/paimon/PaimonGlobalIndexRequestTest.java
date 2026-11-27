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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PaimonGlobalIndexRequestTest {
    private static PaimonGlobalIndexRequest ann(int k) {
        return PaimonGlobalIndexRequest.createAnn("file:/orders", "main", 42, 3, 1000,
                7, "vec", "l2", List.of(1.0f, 2.0f, 3.0f, 4.0f), k);
    }

    private static ScalarOperator filterPredicate() {
        return new BinaryPredicateOperator(BinaryType.EQ, new ColumnRefOperator(101, IntegerType.INT, "k", true),
                ConstantOperator.createInt(3));
    }

    @Test
    public void testFilteredAnnRoundTripAndImmutableBase() {
        var base = ann(3);
        String original = base.toJson();
        var ranges = new java.util.ArrayList<>(List.of(new Range(0, 99), new Range(256, 511)));
        var filtered = PaimonGlobalIndexRequest.createFilteredAnn(base, filterPredicate(), Map.of(101, 9), ranges);
        ranges.clear();
        Assertions.assertEquals(original, base.toJson());
        Assertions.assertEquals(base.getVector(), filtered.getVector());
        Assertions.assertEquals(base.getSnapshotId(), filtered.getSnapshotId());
        Assertions.assertEquals(base.getResultBytesLimit(), filtered.getResultBytesLimit());
        Assertions.assertTrue(filtered.isFilteredAnn());
        Assertions.assertTrue(filtered.hasScalarPredicate());
        Assertions.assertEquals(Set.of(9), filtered.getScalarFieldIds());
        Assertions.assertEquals(Set.of(7, 9), filtered.getRequiredFieldIds());
        var parsed = PaimonGlobalIndexRequest.parseTransport(filtered.toTransportString());
        Assertions.assertEquals(filtered.toJson(), parsed.toJson());
        Assertions.assertEquals(List.of(new Range(0, 99), new Range(256, 511)), parsed.getPartitionRanges());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> parsed.getPartitionRanges().clear());
        var schema = new TableSchema(3, List.of(
                DataTypes.FIELD(7, "vec", DataTypes.ARRAY(DataTypes.FLOAT().notNull()).notNull()),
                DataTypes.FIELD(9, "k", DataTypes.INT())), 9, List.of(), List.of(), Map.of(), null);
        parsed.validateVector(schema);
        parsed.validatePredicate(schema);
    }

    @Test
    public void testFilteredAnnScalarOnlyAndPartitionOnly() {
        var base = ann(3);
        var scalarOnly = PaimonGlobalIndexRequest.createFilteredAnn(base, filterPredicate(), Map.of(101, 9), null);
        Assertions.assertNull(scalarOnly.getPartitionRanges());
        Assertions.assertTrue(PaimonGlobalIndexRequest.parse(scalarOnly.toJson()).hasScalarPredicate());
        var partitionOnly = PaimonGlobalIndexRequest.createFilteredAnn(base, null, Map.of(), List.of(new Range(3, 7)));
        Assertions.assertFalse(partitionOnly.hasScalarPredicate());
        Assertions.assertTrue(partitionOnly.getScalarFieldIds().isEmpty());
        Assertions.assertFalse(partitionOnly.toJson().contains("indexes"));
        Assertions.assertFalse(partitionOnly.toJson().contains("predicate"));
        Assertions.assertTrue(PaimonGlobalIndexRequest.parse(partitionOnly.toJson()).isFilteredAnn());
    }

    @Test
    public void testFilteredAnnRejectsRangesAndIncompleteBindings() {
        var base = ann(3);
        for (List<Range> ranges : List.of(List.<Range>of(), List.of(new Range(-1, 3)),
                List.of(new Range(0, Long.MAX_VALUE)), List.of(new Range(0, 8), new Range(8, 10)),
                List.of(new Range(5, 7), new Range(0, 3)), java.util.Collections.nCopies(4097, new Range(0, 0)))) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonGlobalIndexRequest.createFilteredAnn(base, null, Map.of(), ranges));
        }
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.createFilteredAnn(base, null, Map.of(), null));
        var valid = PaimonGlobalIndexRequest.createFilteredAnn(base, filterPredicate(), Map.of(101, 9),
                List.of(new Range(0, 8)));
        for (String json : List.of(
                valid.toJson().replace("\"from\":0", "\"from\":0.5"),
                valid.toJson().replace("\"to\":8", "\"to\":\"8\""),
                valid.toJson().replace("\"from\":0,", ""),
                valid.toJson().replace("\"fieldId\":9", "\"fieldId\":9.1"),
                valid.toJson().replace("\"provider\":\"btree\"", "\"provider\":\"bitmap\""),
                valid.toJson().replace("\"indexes\":{\"k\":{\"fieldId\":9,\"provider\":\"btree\"}}", "\"indexes\":null"))) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonGlobalIndexRequest.parse(json), json);
        }
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.createFilteredAnn(valid, null, Map.of(), null));
    }

    @Test
    public void testAnnRoundTripAndSchemaBinding() {
        PaimonGlobalIndexRequest request = ann(3);
        PaimonGlobalIndexRequest parsed = PaimonGlobalIndexRequest.parseTransport(request.toTransportString());
        Assertions.assertTrue(parsed.isAnn());
        Assertions.assertEquals(6, parsed.getVersion());
        Assertions.assertEquals(request.getVector(), parsed.getVector());
        Assertions.assertEquals(Set.of(7), parsed.getRequiredFieldIds());
        Assertions.assertFalse(parsed.toJson().contains("predicate"));
        Assertions.assertFalse(parsed.toJson().contains("indexes"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> parsed.getVector().queryVector().add(5f));
        TableSchema schema = new TableSchema(3L,
                List.of(new DataField(7, "vec", DataTypes.ARRAY(DataTypes.FLOAT().notNull()).notNull())),
                7, List.of(), List.of(), Map.of(), null);
        parsed.validateVector(schema);
        for (var type : List.of(DataTypes.ARRAY(DataTypes.FLOAT()).notNull(),
                DataTypes.ARRAY(DataTypes.FLOAT().notNull()), DataTypes.ARRAY(DataTypes.DOUBLE().notNull()).notNull())) {
            TableSchema unsupported = new TableSchema(3L, List.of(new DataField(7, "vec", type)),
                    7, List.of(), List.of(), Map.of(), null);
            Assertions.assertThrows(IllegalArgumentException.class, () -> parsed.validateVector(unsupported));
        }
        TableSchema renamed = new TableSchema(3L,
                List.of(new DataField(7, "renamed", DataTypes.ARRAY(DataTypes.FLOAT().notNull()).notNull())),
                7, List.of(), List.of(), Map.of(), null);
        Assertions.assertThrows(IllegalArgumentException.class, () -> parsed.validateVector(renamed));
    }

    @Test
    public void testAnnRejectsInvalidEnvelopeAndVector() {
        for (String json : List.of(
                ann(3).toJson().replace("unfiltered_top_n", "filtered_top_n"),
                ann(3).toJson().replace("lumina", "btree"),
                ann(3).toJson().replace("l2", "invalid"),
                ann(3).toJson().replace("\"k\":3", "\"k\":0"),
                ann(3).toJson().replace("\"fieldId\":7", "\"fieldId\":-1"),
                ann(3).toJson().replace("\"fieldId\":7,", ""),
                ann(3).toJson().replace("\"k\":3", "\"k\":3.5"),
                ann(3).toJson().replace("\"queryVector\":[1.0,2.0,3.0,4.0]", "\"queryVector\":[\"1\"]"),
                ann(3).toJson().replace("\"resultBytesLimit\":1048576", "\"resultBytesLimit\":39"),
                ann(3).toJson().replace("\"queryVector\":[1.0,2.0,3.0,4.0]", "\"queryVector\":[]"),
                ann(3).toJson().replace("\"queryVector\":[1.0,2.0,3.0,4.0]", "\"queryVector\":[1e100]"),
                ann(3).toJson().replace("\"version\":6", "\"version\":5"),
                ann(3).toJson().replace("\"version\":6", "\"version\":6,\"predicate\":{}"))) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonGlobalIndexRequest.parse(json), json);
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> ann(Integer.MAX_VALUE));
        Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonGlobalIndexRequest.createAnn(
                "file:/orders", "main", 42, 3, 1000, 7, "vec", "l2",
                java.util.Collections.nCopies(4097, 1.0f), 1));
    }

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
        return PaimonGlobalIndexRequest.parse("{\"version\":5,\"resultBytesLimit\":1048576,\"snapshotId\":42,\"schemaId\":3,"
                + "\"schemaTimeMillis\":1000,\"tablePath\":\"file:/tmp/orders\",\"branch\":\"main\","
                + "\"indexes\":{\"k\":{\"fieldId\":7,\"provider\":\"btree\"}},\"predicate\":" + predicate + "}");
    }

    @Test
    public void testJsonAndTransportRoundTrip() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.GE, column, ConstantOperator.createInt(10));
        PaimonGlobalIndexRequest request = PaimonGlobalIndexRequest.create(
                "file:/tmp/orders", "main", 42L, 3L, 1000L, predicate, Map.of(1, 7));

        PaimonGlobalIndexRequest fromJson = PaimonGlobalIndexRequest.parse(request.toJson());
        PaimonGlobalIndexRequest fromTransport =
                PaimonGlobalIndexRequest.parseTransport(request.toTransportString());

        Assertions.assertEquals(42L, fromJson.getSnapshotId());
        Assertions.assertTrue(fromJson.matchesSchema(3L, 1000L));
        Assertions.assertFalse(fromJson.matchesSchema(4L, 1000L));
        Assertions.assertFalse(fromJson.matchesSchema(3L, 1001L));
        Assertions.assertEquals(Set.of(7), fromJson.getRequiredFieldIds());
        Assertions.assertEquals(42L, fromTransport.getSnapshotId());
        Assertions.assertEquals(Set.of(7), fromTransport.getRequiredFieldIds());
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
                "file:/tmp/orders", "main", 42L, 3L, 1000L, predicate, Map.of(1, 700));
        String json = request.toJson();
        Assertions.assertEquals(Set.of(700), request.getRequiredFieldIds());
        for (String invalid : List.of(
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
                "file:/tmp/orders", "main", 42L, 3L, 1000L, predicate, Map.of()));
    }

    @Test
    public void testTransportAndResultLimitBounds() {
        String json = requestWithPredicate(binary("int", "int", "1")).toJson();
        for (String invalid : List.of("null", "{", json.replace("1048576", "0"),
                json.replace("1048576", "1048577"), json.replace("1048576", "-1"))) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonGlobalIndexRequest.parse(invalid));
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonGlobalIndexRequest.parseTransport(null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parseTransport("A".repeat(128 * 1024 + 1)));
        Assertions.assertEquals(1048576, PaimonGlobalIndexRequest.parse(json).getResultBytesLimit());
        Assertions.assertEquals("main", PaimonGlobalIndexRequest.parse(json).getBranch());
    }

    @Test
    public void testAmbiguousBindingsAndOversizedRequests() {
        var first = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, IntegerType.INT, "k", true), ConstantOperator.createInt(1));
        var second = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(2, IntegerType.INT, "k", true), ConstantOperator.createInt(2));
        Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonGlobalIndexRequest.create(
                "file:/orders", "main", 42, 0, 1000, Utils.compoundAnd(first, second), Map.of(1, 7, 2, 8)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonGlobalIndexRequest.create(
                "file:/orders", "main", 42, 0, 1000, null, Map.of()));
        var oversized = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, VarcharType.VARCHAR, "k", true), ConstantOperator.createVarchar("x".repeat(65536)));
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.create("file:/orders", "main", 42, 0, 1000, oversized, Map.of(1, 7)))
                .getMessage().contains("request is too large"));
    }

}
