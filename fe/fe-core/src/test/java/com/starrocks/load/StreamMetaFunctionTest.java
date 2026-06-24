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
import com.starrocks.load.routineload.RoutineLoadMetadata.StreamMetaBinding;
import com.starrocks.load.routineload.RoutineLoadMetadata.StreamMetaKind;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.ImportMetadataStmt;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.starrocks.load.routineload.RoutineLoadMetadata.bindingsFromMetadata;
import static com.starrocks.load.routineload.RoutineLoadMetadata.collectStreamMetaKinds;
import static com.starrocks.load.routineload.RoutineLoadMetadata.metaKeySpecOf;
import static com.starrocks.load.routineload.RoutineLoadMetadata.validateIncludeMetadata;

// Tests the INCLUDE METADATA clause front: key->kind/type mapping, source/format/alias validation,
// kind collection (consumer gates), and binding construction.
public class StreamMetaFunctionTest {

    private static ImportMetadataStmt meta(String... keyAlias) {
        List<ImportMetadataStmt.Item> items = Lists.newArrayList();
        for (int i = 0; i < keyAlias.length; i += 2) {
            items.add(new ImportMetadataStmt.Item(keyAlias[i], keyAlias[i + 1], NodePosition.ZERO));
        }
        return new ImportMetadataStmt(items);
    }

    @Test
    public void testMetaKeySpecOf() {
        Assertions.assertNotNull(metaKeySpecOf("KAFKA", "TOPIC"));
        Assertions.assertEquals(StreamMetaKind.TOPIC, metaKeySpecOf("KAFKA", "TOPIC").kind);
        Assertions.assertEquals(StreamMetaKind.OFFSET, metaKeySpecOf("KAFKA", "OFFSET").kind);
        // Kafka TIMESTAMP_MS and Pulsar PUBLISH_TIME_MS both map to TIMESTAMP; HEADERS/PROPERTIES -> HEADERS.
        Assertions.assertEquals(StreamMetaKind.TIMESTAMP, metaKeySpecOf("KAFKA", "TIMESTAMP_MS").kind);
        Assertions.assertEquals(StreamMetaKind.TIMESTAMP, metaKeySpecOf("PULSAR", "PUBLISH_TIME_MS").kind);
        Assertions.assertEquals(StreamMetaKind.EVENT_TIME, metaKeySpecOf("PULSAR", "EVENT_TIME_MS").kind);
        Assertions.assertEquals(StreamMetaKind.HEADERS, metaKeySpecOf("PULSAR", "PROPERTIES").kind);
        // case-insensitive on both source and key.
        Assertions.assertNotNull(metaKeySpecOf("kafka", "topic"));
        // OFFSET is Kafka-only; MESSAGE_ID is Pulsar-only; an unknown key/source -> null.
        Assertions.assertNull(metaKeySpecOf("PULSAR", "OFFSET"));
        Assertions.assertNull(metaKeySpecOf("KAFKA", "MESSAGE_ID"));
        Assertions.assertNull(metaKeySpecOf("KAFKA", "BOGUS"));
        Assertions.assertNull(metaKeySpecOf("ROCKETMQ", "TOPIC"));
    }

    @Test
    public void testValidateSourceKeys() {
        // OFFSET only for KAFKA.
        Assertions.assertThrows(DdlException.class,
                () -> validateIncludeMetadata(meta("OFFSET", "o"), null, "PULSAR", "json"));
        Assertions.assertDoesNotThrow(
                () -> validateIncludeMetadata(meta("OFFSET", "o"), null, "KAFKA", "json"));
        // MESSAGE_ID only for PULSAR.
        Assertions.assertThrows(DdlException.class,
                () -> validateIncludeMetadata(meta("MESSAGE_ID", "m"), null, "KAFKA", "json"));
        Assertions.assertDoesNotThrow(
                () -> validateIncludeMetadata(meta("MESSAGE_ID", "m"), null, "PULSAR", "json"));
    }

    @Test
    public void testValidateFormat() {
        // INCLUDE METADATA requires JSON or Avro; CSV (empty format) is rejected.
        Assertions.assertThrows(DdlException.class,
                () -> validateIncludeMetadata(meta("TOPIC", "t"), null, "KAFKA", ""));
        Assertions.assertDoesNotThrow(
                () -> validateIncludeMetadata(meta("TOPIC", "t"), null, "KAFKA", "json"));
        Assertions.assertDoesNotThrow(
                () -> validateIncludeMetadata(meta("TOPIC", "t"), null, "KAFKA", "avro"));
    }

    @Test
    public void testValidateAvroRejectedForPulsar() {
        // Pulsar tasks send non-JSON data to the BE as CSV, so Avro metadata extraction can never work.
        Assertions.assertThrows(DdlException.class,
                () -> validateIncludeMetadata(meta("TOPIC", "t"), null, "PULSAR", "avro"));
        Assertions.assertDoesNotThrow(
                () -> validateIncludeMetadata(meta("TOPIC", "t"), null, "PULSAR", "json"));
    }

    @Test
    public void testValidateAliasCollisions() {
        // duplicate alias.
        Assertions.assertThrows(DdlException.class,
                () -> validateIncludeMetadata(meta("TOPIC", "x", "PARTITION", "x"), null, "KAFKA", "json"));
        // alias equal to a bare COLUMNS field.
        List<ImportColumnDesc> cols = Lists.newArrayList(new ImportColumnDesc("id"));
        Assertions.assertThrows(DdlException.class,
                () -> validateIncludeMetadata(meta("PARTITION", "id"), cols, "KAFKA", "json"));
        // alias equal to the synthetic __op column.
        Assertions.assertThrows(DdlException.class,
                () -> validateIncludeMetadata(meta("PARTITION", "__op"), null, "KAFKA", "json"));
        // reserved __starrocks prefix.
        Assertions.assertThrows(DdlException.class,
                () -> validateIncludeMetadata(meta("PARTITION", "__starrocks_x"), null, "KAFKA", "json"));
        // a clean clause passes (alias distinct from bare COLUMNS fields).
        List<ImportColumnDesc> ok = Lists.newArrayList(new ImportColumnDesc("id"), new ImportColumnDesc("payload"));
        Assertions.assertDoesNotThrow(
                () -> validateIncludeMetadata(meta("PARTITION", "p", "HEADERS", "h"), ok, "KAFKA", "json"));
        // distinct aliases on the same kind are allowed.
        Assertions.assertDoesNotThrow(
                () -> validateIncludeMetadata(meta("KEY", "a", "KEY", "b"), null, "KAFKA", "json"));
    }

    @Test
    public void testCollectKinds() {
        Set<StreamMetaKind> kinds = collectStreamMetaKinds(meta("TOPIC", "t", "HEADERS", "h"), "KAFKA");
        Assertions.assertTrue(kinds.contains(StreamMetaKind.TOPIC));
        Assertions.assertTrue(kinds.contains(StreamMetaKind.HEADERS));
        Assertions.assertFalse(kinds.contains(StreamMetaKind.KEY));
        // Pulsar PROPERTIES -> HEADERS kind (drives need_message_headers).
        Set<StreamMetaKind> pulsar = collectStreamMetaKinds(meta("PROPERTIES", "p"), "PULSAR");
        Assertions.assertTrue(pulsar.contains(StreamMetaKind.HEADERS));
        // null clause -> empty.
        Assertions.assertTrue(collectStreamMetaKinds(null, "KAFKA").isEmpty());
    }

    @Test
    public void testBindings() {
        List<StreamMetaBinding> bindings = bindingsFromMetadata(meta("PARTITION", "p", "HEADERS", "h"), "KAFKA");
        Assertions.assertEquals(2, bindings.size());
        // alias is the hidden source-column name.
        Assertions.assertEquals("p", bindings.get(0).hiddenName);
        Assertions.assertEquals(StreamMetaKind.PARTITION, bindings.get(0).kind);
        Assertions.assertEquals("h", bindings.get(1).hiddenName);
        Assertions.assertEquals(StreamMetaKind.HEADERS, bindings.get(1).kind);
        // no clause -> empty.
        Assertions.assertTrue(bindingsFromMetadata(null, "KAFKA").isEmpty());
    }
}
