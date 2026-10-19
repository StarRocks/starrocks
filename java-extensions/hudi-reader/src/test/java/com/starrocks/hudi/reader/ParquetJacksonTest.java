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

package com.starrocks.hudi.reader;

import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import shaded.parquet.com.fasterxml.jackson.core.ErrorReportConfiguration;
import shaded.parquet.com.fasterxml.jackson.core.JsonFactory;
import shaded.parquet.com.fasterxml.jackson.core.JsonParser;
import shaded.parquet.com.fasterxml.jackson.core.JsonProcessingException;
import shaded.parquet.com.fasterxml.jackson.databind.JsonNode;
import shaded.parquet.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

class ParquetJacksonTest {
    @Test
    void testMetadataSerialization() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("message test { required int64 id; }");
        FileMetaData file = new FileMetaData(schema, Collections.singletonMap("key", "value"), "StarRocks");
        ParquetMetadata metadata = new ParquetMetadata(file, Collections.emptyList());
        JsonNode restored = new ObjectMapper().readTree(ParquetMetadata.toJSON(metadata));
        Assertions.assertEquals("value", restored.path("keyValueMetaData").path("key").asText());
        Assertions.assertEquals("StarRocks", restored.path("createdBy").asText());
        Assertions.assertEquals("test", restored.path("schema").path("name").asText());
    }

    @Test
    void testDataInputErrorTokenIsBounded() throws Exception {
        // Exercise the relocated parser used by Parquet, including the CVE-2026-89425 fix.
        JsonFactory factory = JsonFactory.builder().errorReportConfiguration(
                ErrorReportConfiguration.builder().maxErrorTokenLength(32).build()).build();
        String token = String.join("", Collections.nCopies(300, "x"));
        DataInput input = new DataInputStream(new ByteArrayInputStream((token + " ").getBytes(StandardCharsets.UTF_8)));
        try (JsonParser parser = factory.createParser(input)) {
            JsonProcessingException error = Assertions.assertThrows(JsonProcessingException.class, parser::nextToken);
            Assertions.assertFalse(error.getMessage().contains(token));
        }
    }
}
