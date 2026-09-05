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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.thrift.TResultBatch;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PartitionAccessTimeStoreTest {

    @Test
    public void testBuildUpsertSql() {
        String sql = PartitionAccessTimeStore.buildUpsertSql(
                Lists.newArrayList(new PartitionAccessTimeEntry(1, 2, 3, 111L), new PartitionAccessTimeEntry(1, 2, 4, 222L)));
        Assertions.assertTrue(sql.startsWith("INSERT INTO _statistics_.partition_access_time"));
        Assertions.assertTrue(sql.contains("(1, 2, 3, 111)"));
        Assertions.assertTrue(sql.contains("(1, 2, 4, 222)"));
    }

    @Test
    public void testBuildDeleteByPartitionIdsSql() {
        String sql = PartitionAccessTimeStore.buildDeleteByPartitionIdsSql(Arrays.asList(5L, 6L));
        Assertions.assertTrue(sql.startsWith("DELETE FROM _statistics_.partition_access_time"));
        Assertions.assertTrue(sql.contains("partition_id IN (5, 6)"));
    }

    // The repo executor returns each row as a JSON {"data":[...]} buffer (HTTP_PROTOCAL sink).
    private static ByteBuffer jsonRow(String json) {
        return ByteBuffer.wrap(json.getBytes(Charset.defaultCharset()));
    }

    @Test
    public void testLoadAllDecodesJsonRows() {
        new MockUp<SimpleExecutor>() {
            @Mock
            public List<TResultBatch> executeDQL(String sql) {
                TResultBatch batch = new TResultBatch();
                batch.setRows(Lists.newArrayList(
                        jsonRow("{\"data\": [1, 2, 100, 111]}"),
                        jsonRow("{\"data\": [1, 2, 300, 222]}")));
                return Lists.newArrayList(batch);
            }
        };
        List<PartitionAccessTimeEntry> all = new PartitionAccessTimeStore().loadAll();
        Assertions.assertEquals(2, all.size());
        Assertions.assertEquals(1L, all.get(0).getDbId());
        Assertions.assertEquals(2L, all.get(0).getTableId());
        Assertions.assertEquals(100L, all.get(0).getPartitionId());
        Assertions.assertEquals(111L, all.get(0).getAccessTimeMs());
        Assertions.assertEquals(300L, all.get(1).getPartitionId());
        Assertions.assertEquals(222L, all.get(1).getAccessTimeMs());
    }

    @Test
    public void testLoadAllPropagatesQueryFailure() {
        new MockUp<SimpleExecutor>() {
            @Mock
            public List<TResultBatch> executeDQL(String sql) {
                throw new RuntimeException("table gone");
            }
        };
        // loadAll must NOT swallow the failure -- the caller (persister) needs to know so it retries the
        // one-time baseline load next cycle rather than starting from an empty map.
        Assertions.assertThrows(RuntimeException.class, () -> new PartitionAccessTimeStore().loadAll());
    }

    @Test
    public void testUpsertChunksWithinExprChildrenLimit() {
        // Read the real chunk size (don't mutate the global expr_children_limit -- the SQL parser reads it, so
        // lowering it could disturb a concurrently-running test). One entry past a single chunk => exactly 2
        // INSERTs, proving a large flush is split and never trips the parser's INSERT row-count cap.
        List<String> sqls = new ArrayList<>();
        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDML(String sql) {
                sqls.add(sql);
            }
        };
        int chunkSize = Math.max(1, Config.expr_children_limit / 2);
        List<PartitionAccessTimeEntry> entries = new ArrayList<>();
        for (int i = 0; i < chunkSize + 1; i++) {
            entries.add(new PartitionAccessTimeEntry(1, 1, i, 10));
        }
        new PartitionAccessTimeStore().upsert(entries);
        Assertions.assertEquals(2, sqls.size());
    }

    @Test
    public void testDeleteChunksWithinExprChildrenLimit() {
        // Same chunking as upsert: one id past a single chunk => exactly 2 DELETEs, so a large cleanup never
        // trips the parser's IN-list cap. Reads the real chunk size without mutating the global config.
        List<String> sqls = new ArrayList<>();
        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDML(String sql) {
                sqls.add(sql);
            }
        };
        int chunkSize = Math.max(1, Config.expr_children_limit / 2);
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < chunkSize + 1; i++) {
            ids.add((long) i);
        }
        new PartitionAccessTimeStore().deleteByPartitionIds(ids);
        Assertions.assertEquals(2, sqls.size());
    }

    @Test
    public void testDeleteChunksWithinDeleteInListCap() {
        // When the delete-specific cap is smaller than expr_children_limit/2, chunking must respect it, otherwise
        // DeleteAnalyzer rejects the oversized DELETE and dropped-partition rows accumulate forever.
        int savedDeleteCap = Config.max_allowed_in_element_num_of_delete;
        Config.max_allowed_in_element_num_of_delete = 3;
        List<String> sqls = new ArrayList<>();
        try {
            new MockUp<SimpleExecutor>() {
                @Mock
                public void executeDML(String sql) {
                    sqls.add(sql);
                }
            };
            List<Long> ids = new ArrayList<>();
            for (int i = 0; i < 7; i++) {
                ids.add((long) i);
            }
            new PartitionAccessTimeStore().deleteByPartitionIds(ids);
            // 7 ids capped at 3 per DELETE => ceil(7/3) = 3 statements.
            Assertions.assertEquals(3, sqls.size());
        } finally {
            Config.max_allowed_in_element_num_of_delete = savedDeleteCap;
        }
    }
}
