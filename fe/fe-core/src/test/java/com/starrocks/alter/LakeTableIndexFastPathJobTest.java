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

package com.starrocks.alter;

import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.thrift.TDropIndexInfo;
import com.starrocks.thrift.TIndexType;
import com.starrocks.thrift.TOlapTableIndex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Light unit coverage for the Lake ADD/DROP INDEX fast-path Job classes.
 * We test the shape / serialization-friendliness of the classes rather than
 * the full AlterJobV2 lifecycle (which needs a running catalog). The
 * lifecycle is exercised by e2e SQL tests in a separate follow-up.
 */
public class LakeTableIndexFastPathJobTest {

    @Test
    public void testAddIndexJobConstruction() {
        long indexId = 101L;
        Index ix = new Index(indexId, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());
        TOlapTableIndex thrift = new TOlapTableIndex();
        thrift.setIndex_name("ix_a");
        thrift.setIndex_id(indexId);
        thrift.setIndex_type(TIndexType.BITMAP);
        thrift.setColumns(Collections.singletonList("c1"));

        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix), Collections.singletonList(thrift));

        assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, job.getType());
        assertEquals(1, job.getNewIndexes().size());
        assertEquals(1, job.getIndexesToAdd().size());
        assertEquals("ix_a", job.getNewIndexes().get(0).getIndexName());
    }

    @Test
    public void testAddIndexJobCopyForPersist() {
        Index ix = new Index(5L, "ix_b", Collections.singletonList(ColumnId.create("c2")),
                IndexDef.IndexType.NGRAMBF, "", new java.util.HashMap<>());
        TOlapTableIndex thrift = new TOlapTableIndex();
        thrift.setIndex_name("ix_b");
        thrift.setIndex_id(5L);
        thrift.setIndex_type(TIndexType.NGRAMBF);
        thrift.setColumns(Collections.singletonList("c2"));

        LakeTableAddIndexJob job = new LakeTableAddIndexJob(7L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix), Collections.singletonList(thrift));

        AlterJobV2 copy = job.copyForPersist();
        assertInstanceOf(LakeTableAddIndexJob.class, copy);
        LakeTableAddIndexJob lc = (LakeTableAddIndexJob) copy;
        assertEquals(job.getJobId(), lc.getJobId());
        assertEquals(job.getNewIndexes().size(), lc.getNewIndexes().size());
        // Copy should be a distinct list instance.
        assertNotNull(lc.getNewIndexes());
        assertTrue(lc.getNewIndexes() != job.getNewIndexes());
    }

    @Test
    public void testDropIndexJobConstruction() {
        TDropIndexInfo info = new TDropIndexInfo();
        info.setIndex_id(201L);
        info.setCol_unique_id(7);
        info.setIndex_type(TIndexType.BITMAP);

        LakeTableDropIndexJob job = new LakeTableDropIndexJob(10L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(201L), Collections.singletonList(info));

        assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        assertEquals(1, job.getDropIndexIds().size());
        assertEquals(1, job.getDropInfos().size());
        assertEquals(201L, job.getDropIndexIds().get(0));
    }

    @Test
    public void testDropIndexJobCopyForPersist() {
        List<TDropIndexInfo> infos = new ArrayList<>();
        TDropIndexInfo info = new TDropIndexInfo();
        info.setIndex_id(201L);
        info.setCol_unique_id(7);
        info.setIndex_type(TIndexType.NGRAMBF);
        infos.add(info);

        LakeTableDropIndexJob job = new LakeTableDropIndexJob(11L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(201L), infos);

        AlterJobV2 copy = job.copyForPersist();
        assertInstanceOf(LakeTableDropIndexJob.class, copy);
        LakeTableDropIndexJob lc = (LakeTableDropIndexJob) copy;
        assertEquals(job.getJobId(), lc.getJobId());
        assertEquals(job.getDropInfos().size(), lc.getDropInfos().size());
        assertTrue(lc.getDropInfos() != job.getDropInfos());
    }

    @Test
    public void testTransactionIdNotSetBeforeWatershed() {
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.emptyList(), Collections.emptyList());
        // watershedTxnId defaults to -1 before runPendingJob allocates it.
        assertTrue(job.getTransactionId().isEmpty());
    }
}
