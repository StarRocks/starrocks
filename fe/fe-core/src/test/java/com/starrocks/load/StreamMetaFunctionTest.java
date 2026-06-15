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

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class StreamMetaFunctionTest {

    private static FunctionCallExpr call(String name, Expr... args) {
        return new FunctionCallExpr(name, Lists.newArrayList(args));
    }

    @Test
    public void testStreamMetaFuncOf() {
        Load.StreamMetaFunc topic = Load.streamMetaFuncOf(call("kafka_topic"));
        Assertions.assertNotNull(topic);
        Assertions.assertEquals("KAFKA", topic.source);
        Assertions.assertEquals(Load.StreamMetaKind.TOPIC, topic.kind);

        Load.StreamMetaFunc header = Load.streamMetaFuncOf(call("kafka_header", new StringLiteral("k")));
        Assertions.assertNotNull(header);
        Assertions.assertTrue(header.takesKey);

        // Name matching is case-insensitive; a regular (non-metadata) function -> null.
        Assertions.assertNotNull(Load.streamMetaFuncOf(call("KAFKA_TOPIC")));
        Assertions.assertNull(Load.streamMetaFuncOf(call("from_unixtime")));
    }

    @Test
    public void testValidateSourceMatch() {
        // kafka_offset() only for KAFKA.
        List<ImportColumnDesc> kafkaOffset = Lists.newArrayList(new ImportColumnDesc("c", call("kafka_offset")));
        Assertions.assertThrows(DdlException.class,
                () -> Load.validateStreamMetaFunctions(kafkaOffset, "PULSAR", "json", true));
        Assertions.assertDoesNotThrow(() -> Load.validateStreamMetaFunctions(kafkaOffset, "KAFKA", "json", true));

        // pulsar_message_id() only for PULSAR.
        List<ImportColumnDesc> pulsarMid = Lists.newArrayList(new ImportColumnDesc("c", call("pulsar_message_id")));
        Assertions.assertThrows(DdlException.class,
                () -> Load.validateStreamMetaFunctions(pulsarMid, "KAFKA", "json", true));
    }

    @Test
    public void testValidateFormat() {
        // Metadata functions require JSON or Avro; CSV (empty format) is rejected.
        List<ImportColumnDesc> descs = Lists.newArrayList(new ImportColumnDesc("c", call("kafka_topic")));
        Assertions.assertThrows(DdlException.class, () -> Load.validateStreamMetaFunctions(descs, "KAFKA", "", true));
        Assertions.assertDoesNotThrow(() -> Load.validateStreamMetaFunctions(descs, "KAFKA", "json", true));
        Assertions.assertDoesNotThrow(() -> Load.validateStreamMetaFunctions(descs, "KAFKA", "avro", true));
    }

    @Test
    public void testValidateAvroRequiresNativeReader() {
        List<ImportColumnDesc> descs = Lists.newArrayList(new ImportColumnDesc("c", call("kafka_topic")));
        // Avro without the native reader is rejected (the non-native AvroScanner does not fill metadata).
        Assertions.assertThrows(DdlException.class,
                () -> Load.validateStreamMetaFunctions(descs, "KAFKA", "avro", false));
        Assertions.assertThrows(DdlException.class,
                () -> Load.validateStreamMetaFunctions(descs, "KAFKA", "avro", null));
        Assertions.assertDoesNotThrow(() -> Load.validateStreamMetaFunctions(descs, "KAFKA", "avro", true));
        // JSON ignores the reader flag.
        Assertions.assertDoesNotThrow(() -> Load.validateStreamMetaFunctions(descs, "KAFKA", "json", false));
    }

    @Test
    public void testValidateAvroRejectedForPulsar() {
        // Pulsar tasks send non-JSON data to the BE as CSV, so Avro metadata extraction can never work.
        List<ImportColumnDesc> descs = Lists.newArrayList(new ImportColumnDesc("c", call("pulsar_topic")));
        Assertions.assertThrows(DdlException.class,
                () -> Load.validateStreamMetaFunctions(descs, "PULSAR", "avro", true));
        Assertions.assertDoesNotThrow(() -> Load.validateStreamMetaFunctions(descs, "PULSAR", "json", null));
    }

    @Test
    public void testValidateArityAndKey() {
        // header lookup requires a single string-literal key.
        List<ImportColumnDesc> noArg = Lists.newArrayList(new ImportColumnDesc("c", call("kafka_header")));
        Assertions.assertThrows(DdlException.class, () -> Load.validateStreamMetaFunctions(noArg, "KAFKA", "json", true));

        List<ImportColumnDesc> nonLiteral =
                Lists.newArrayList(new ImportColumnDesc("c", call("kafka_header", new SlotRef(null, "x"))));
        Assertions.assertThrows(DdlException.class, () -> Load.validateStreamMetaFunctions(nonLiteral, "KAFKA", "json", true));

        List<ImportColumnDesc> ok =
                Lists.newArrayList(new ImportColumnDesc("c", call("kafka_header", new StringLiteral("trace-id"))));
        Assertions.assertDoesNotThrow(() -> Load.validateStreamMetaFunctions(ok, "KAFKA", "json", true));

        // scalar functions take no arguments.
        List<ImportColumnDesc> extraArg =
                Lists.newArrayList(new ImportColumnDesc("c", call("kafka_topic", new StringLiteral("x"))));
        Assertions.assertThrows(DdlException.class, () -> Load.validateStreamMetaFunctions(extraArg, "KAFKA", "json", true));
    }

    @Test
    public void testCollectKinds() {
        List<ImportColumnDesc> descs = Lists.newArrayList(
                new ImportColumnDesc("a", call("kafka_topic")),
                new ImportColumnDesc("b", call("kafka_header", new StringLiteral("h"))),
                new ImportColumnDesc("plain", call("from_unixtime")));
        Set<Load.StreamMetaKind> kinds = Load.collectStreamMetaKinds(descs);
        Assertions.assertTrue(kinds.contains(Load.StreamMetaKind.TOPIC));
        Assertions.assertTrue(kinds.contains(Load.StreamMetaKind.HEADER));
        Assertions.assertFalse(kinds.contains(Load.StreamMetaKind.KEY));
    }

    @Test
    public void testLowerRootAndNested() {
        // root call: t = kafka_topic(); nested call: ts = from_unixtime(kafka_timestamp_ms()).
        ImportColumnDesc tDesc = new ImportColumnDesc("t", call("kafka_topic"));
        ImportColumnDesc tsDesc = new ImportColumnDesc("ts", call("from_unixtime", call("kafka_timestamp_ms")));
        List<ImportColumnDesc> descs = Lists.newArrayList(tDesc, tsDesc);

        List<Load.StreamMetaBinding> bindings = Load.lowerStreamMetaFunctions(descs, null);

        Assertions.assertEquals(2, bindings.size());
        // Two hidden bare columns appended.
        Assertions.assertEquals(4, descs.size());
        Assertions.assertTrue(descs.get(2).isColumn());
        Assertions.assertTrue(descs.get(3).isColumn());

        // Root rewritten to a SlotRef to the hidden topic column.
        Expr rewrittenRoot = descs.get(0).getExpr();
        Assertions.assertTrue(rewrittenRoot instanceof SlotRef);

        // Nested call's child rewritten to a SlotRef; the outer function stays.
        Expr rewrittenNested = descs.get(1).getExpr();
        Assertions.assertTrue(rewrittenNested instanceof FunctionCallExpr);
        Assertions.assertTrue(rewrittenNested.getChild(0) instanceof SlotRef);
    }

    @Test
    public void testLowerDedup() {
        // Two identical kafka_topic() calls collapse to one hidden column.
        List<ImportColumnDesc> descs = Lists.newArrayList(
                new ImportColumnDesc("a", call("kafka_topic")),
                new ImportColumnDesc("b", call("kafka_topic")));
        List<Load.StreamMetaBinding> bindings = Load.lowerStreamMetaFunctions(descs, null);

        Assertions.assertEquals(1, bindings.size());
        Assertions.assertEquals(3, descs.size()); // a, b, one hidden column
        SlotRef refA = (SlotRef) descs.get(0).getExpr();
        SlotRef refB = (SlotRef) descs.get(1).getExpr();
        Assertions.assertEquals(refA.getColumnName(), refB.getColumnName());
        Assertions.assertEquals(bindings.get(0).hiddenName, refA.getColumnName());
    }

    @Test
    public void testLowerNameCollisionAvoided() {
        // An existing column named like the first generated hidden name forces the generator to skip it
        // (the same mechanism seeds destination-table column names, preventing table-column collisions).
        List<ImportColumnDesc> descs = Lists.newArrayList(
                new ImportColumnDesc("__starrocks_rl_meta_0"),
                new ImportColumnDesc("t", call("kafka_topic")));
        List<Load.StreamMetaBinding> bindings = Load.lowerStreamMetaFunctions(descs, null);
        Assertions.assertEquals(1, bindings.size());
        Assertions.assertNotEquals("__starrocks_rl_meta_0", bindings.get(0).hiddenName);
    }

    @Test
    public void testLowerNoMetaUnchanged() {
        // No metadata function -> nothing rewritten, no hidden column.
        List<ImportColumnDesc> descs = Lists.newArrayList(
                new ImportColumnDesc("a"),
                new ImportColumnDesc("b", call("from_unixtime", new SlotRef(null, "x"))));
        List<Load.StreamMetaBinding> bindings = Load.lowerStreamMetaFunctions(descs, null);
        Assertions.assertTrue(bindings.isEmpty());
        Assertions.assertEquals(2, descs.size());
    }
}
