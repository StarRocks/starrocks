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

package com.starrocks.load.pipe;

import com.starrocks.catalog.TableName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PipeBatchSizeReplayTest {

    @Test
    public void testReplayKeepsPersistedBatchSizeWhenStoredStringOverflows() {
        FilePipeSource source = new FilePipeSource("s3://bucket/path", "csv", new HashMap<>());
        long persisted = 0L;
        source.setBatchSize(persisted);
        Pipe pipe = new Pipe(new PipeId(1, 2), "p", new TableName("db", "t"), source,
                "insert into t select * from files()");

        Map<String, String> properties = new HashMap<>();
        properties.put("batch_size", "17179869184G");
        pipe.processProperties(properties);

        Assertions.assertEquals(persisted, source.getBatchSize());
        Assertions.assertEquals("17179869184G", pipe.getProperties().get("batch_size"));
    }

    @Test
    public void testReplayStillAppliesABatchSizeThatFits() {
        FilePipeSource source = new FilePipeSource("s3://bucket/path", "csv", new HashMap<>());
        source.setBatchSize(1L);
        Pipe pipe = new Pipe(new PipeId(1, 2), "p", new TableName("db", "t"), source,
                "insert into t select * from files()");

        Map<String, String> properties = new HashMap<>();
        properties.put("batch_size", "2MB");
        pipe.processProperties(properties);

        Assertions.assertEquals(2L * 1024 * 1024, source.getBatchSize());
    }

    @Test
    public void testReplayStillRejectsAMalformedBatchSize() {
        FilePipeSource source = new FilePipeSource("s3://bucket/path", "csv", new HashMap<>());
        source.setBatchSize(1L);
        Pipe pipe = new Pipe(new PipeId(1, 2), "p", new TableName("db", "t"), source,
                "insert into t select * from files()");

        Map<String, String> properties = new HashMap<>();
        properties.put("batch_size", "abc");
        Assertions.assertThrows(RuntimeException.class, () -> pipe.processProperties(properties));
        Assertions.assertEquals(1L, source.getBatchSize());
    }
}
