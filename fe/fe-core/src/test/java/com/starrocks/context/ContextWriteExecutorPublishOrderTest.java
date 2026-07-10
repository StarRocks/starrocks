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

package com.starrocks.context;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Source-level guard that the single-row and batched upsert paths in
 * {@link ContextWriteExecutor} flush the heads INSERT <em>after</em> versions / fragments /
 * refs / commits. End-to-end testing of the publish order needs a live BE
 * ({@code SimpleExecutor.getRepoExecutor} is a static singleton without an injection point),
 * which the existing semantic-context unit tests deliberately avoid; a source-text guard is a
 * pragmatic alternative that fails loud when a future refactor reorders the SQL flushes.
 *
 * <p>If the source is moved or the executeDML lines are restructured beyond simple shuffling
 * (e.g. method extraction), update this test to point at the new sentinel substrings.
 */
public class ContextWriteExecutorPublishOrderTest {

    private static final String SOURCE_PATH =
            "src/main/java/com/starrocks/context/ContextWriteExecutor.java";

    private static String readSource() throws IOException {
        return new String(Files.readAllBytes(Paths.get(SOURCE_PATH)));
    }

    @Test
    public void singleRowUpsertWritesHeadsLast() throws Exception {
        String src = readSource();
        int versions = src.indexOf("executor.executeDML(versionsInsert)");
        int fragments = src.indexOf("writeFragments(executor,");
        int refs = src.indexOf("writeRefs(executor,");
        int commits = src.indexOf("executor.executeDML(commitsInsert)");
        int heads = src.indexOf("executor.executeDML(headsInsert)");
        Assertions.assertTrue(versions > 0, "versions DML not found");
        Assertions.assertTrue(fragments > 0, "fragments DML not found");
        Assertions.assertTrue(refs > 0, "refs DML not found");
        Assertions.assertTrue(commits > 0, "commits DML not found");
        Assertions.assertTrue(heads > 0, "heads DML not found");
        Assertions.assertTrue(versions < fragments, "versions must precede fragments");
        Assertions.assertTrue(fragments < refs, "fragments must precede refs");
        Assertions.assertTrue(refs < commits, "refs must precede commits");
        Assertions.assertTrue(commits < heads,
                "commits must precede heads — heads is the publish step and must come last "
                        + "so a mid-write failure leaves readers seeing the previous head");
    }

    @Test
    public void batchedUpsertFlushesHeadsLast() throws Exception {
        String src = readSource();
        // The batched flush materializes each table's buffer (`String s = xxxBuf.toString();`) and
        // then issues executor.executeDML(s) inside executeBatchedInserts. Anchor on the per-table
        // buffer materialization, which is unique to the batched path and sits immediately before
        // its flush, so its position still pins the publish order.
        int vBuf = src.indexOf("versionsBuf.toString()");
        // Fragments publish via Stream Load (FE→BE HTTP PUT), not INSERT … VALUES — anchor on the
        // StreamLoader call rather than an executeDML flush.
        int fBuf = src.indexOf("loader.loadBatch(");
        int rBuf = src.indexOf("refsBuf.toString()");
        int cBuf = src.indexOf("commitsBuf.toString()");
        int hBuf = src.indexOf("headsBuf.toString()");
        Assertions.assertTrue(vBuf > 0 && fBuf > 0 && rBuf > 0 && cBuf > 0 && hBuf > 0,
                "could not locate all batched flush DMLs");
        Assertions.assertTrue(vBuf < fBuf && fBuf < rBuf && rBuf < cBuf && cBuf < hBuf,
                "batched flush must publish heads last for the same reason as the single-row path");
    }

    /**
     * A bulk import is one logical commit: every surviving row shares a single
     * {@code snapshot_version} (so an as-of fence includes the whole batch or none of it) and the
     * batch writes exactly one {@code context_commits} row. Guards against a regression to the old
     * per-row allocation, which advanced the global snapshot clock by N and wrote N commit rows
     * (and would now emit N duplicate-PK tuples into the snapshot_version-keyed commits table).
     */
    @Test
    public void batchedUpsertSharesOneSnapshotPerBatch() throws Exception {
        String src = readSource();
        // The batch snapshot is allocated once and assigned to every surviving row...
        Assertions.assertTrue(src.contains("p.snapshotVersion = batchSnapshot"),
                "batched path must assign the shared batchSnapshot to each row");
        // ...not allocated per row (the old behaviour).
        Assertions.assertFalse(src.contains("p.snapshotVersion = snapshotAllocator.next()"),
                "batched path must not allocate a snapshot_version per row");
        // The commits row is built from the shared batch snapshot, once, not from a per-row Prepared.
        Assertions.assertTrue(src.contains("appendCommitsRow(commitsBuf, anyC, batchSnapshot"),
                "commits row must be built once from the shared batchSnapshot");
    }
}
