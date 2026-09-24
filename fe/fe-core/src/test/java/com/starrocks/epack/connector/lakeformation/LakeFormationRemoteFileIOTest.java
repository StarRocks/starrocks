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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.common.FeConstants;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** File systems opened on one table's vended credentials, and the closing that keeps them from accumulating. */
public class LakeFormationRemoteFileIOTest {

    private static final URI BUCKET = URI.create("s3://bucket/db/t/part-0.parquet");
    private static final URI SAME_BUCKET_OTHER_PATH = URI.create("s3://bucket/db/t/dt=1/part-1.parquet");
    private static final URI OTHER_BUCKET = URI.create("s3://other-bucket/db/t/part-0.parquet");

    private boolean wasRunningUnitTest;

    @BeforeEach
    public void turnOffTheUnitTestShortcut() {
        wasRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
    }

    @AfterEach
    public void restore() {
        FeConstants.runningUnitTest = wasRunningUnitTest;
    }

    /**
     * A concrete file system that records its own closing. A local one is used because it is real enough to
     * construct and never touches the network; only close() matters to the subject under test.
     */
    private static class ClosingRecorder extends RawLocalFileSystem {
        private final List<ClosingRecorder> closed;
        private final IOException refuseWith;
        private final RuntimeException refuseWithRuntime;

        ClosingRecorder(List<ClosingRecorder> closed, IOException refuseWith,
                        RuntimeException refuseWithRuntime) {
            this.closed = closed;
            this.refuseWith = refuseWith;
            this.refuseWithRuntime = refuseWithRuntime;
        }

        @Override
        public void close() throws IOException {
            closed.add(this);
            if (refuseWithRuntime != null) {
                throw refuseWithRuntime;
            }
            if (refuseWith != null) {
                throw refuseWith;
            }
        }
    }

    /** Stands in for Hadoop's unshared-instance factory, handing back recorders instead of reaching S3. */
    private static void openReturns(List<ClosingRecorder> closed, AtomicInteger opens,
                                    IOException refuseWith, RuntimeException refuseWithRuntime) {
        new MockUp<FileSystem>() {
            @Mock
            public FileSystem newInstance(URI uri, Configuration configuration) {
                opens.incrementAndGet();
                return new ClosingRecorder(closed, refuseWith, refuseWithRuntime);
            }
        };
    }

    private static LakeFormationRemoteFileIO io() {
        return new LakeFormationRemoteFileIO(new Configuration());
    }

    @Test
    public void testOneFileSystemPerSchemeAndAuthority() throws IOException {
        AtomicInteger opens = new AtomicInteger();
        openReturns(new ArrayList<>(), opens, null, null);

        try (LakeFormationRemoteFileIO fileIO = io()) {
            fileIO.fileSystemFor(BUCKET);
            fileIO.fileSystemFor(SAME_BUCKET_OTHER_PATH);
            assertEquals(1, opens.get(),
                    "a table's files live under one bucket; a file system per path would multiply pools");

            fileIO.fileSystemFor(OTHER_BUCKET);
            assertEquals(2, opens.get(), "a different authority is a different file system");
        }
    }

    /**
     * An unshared instance is still registered in Hadoop's unbounded map, and only close() takes it out.
     * Closing the listing has to reach every one of them, not just the last.
     */
    @Test
    public void testClosingTheListingClosesEveryFileSystemItOpened() throws IOException {
        List<ClosingRecorder> closed = new ArrayList<>();
        openReturns(closed, new AtomicInteger(), null, null);

        LakeFormationRemoteFileIO fileIO = io();
        fileIO.fileSystemFor(BUCKET);
        fileIO.fileSystemFor(OTHER_BUCKET);
        assertTrue(closed.isEmpty(), "nothing is released while the listing is still running");

        fileIO.close();

        assertEquals(2, closed.size());
    }

    /**
     * A socket that refuses to shut down must not fail the query that is finishing, and must not stop the
     * other file systems from being released.
     */
    @Test
    public void testAFileSystemThatRefusesToCloseDoesNotStopTheRest() throws IOException {
        List<ClosingRecorder> closed = new ArrayList<>();
        openReturns(closed, new AtomicInteger(), new IOException("connection reset"), null);

        LakeFormationRemoteFileIO fileIO = io();
        fileIO.fileSystemFor(BUCKET);
        fileIO.fileSystemFor(OTHER_BUCKET);

        fileIO.close();

        assertEquals(2, closed.size(), "the second is released even though the first threw");
    }

    /** Opening is allowed to fail; the failure is the caller's to see, not something to swallow. */
    @Test
    public void testAFailureToOpenReachesTheCaller() {
        new MockUp<FileSystem>() {
            @Mock
            public FileSystem newInstance(URI uri, Configuration configuration) throws IOException {
                throw new IOException("no credentials");
            }
        };

        LakeFormationRemoteFileIO fileIO = io();

        IOException failure = assertThrows(IOException.class, () -> fileIO.fileSystemFor(BUCKET));
        assertEquals("no credentials", failure.getMessage());
    }

}
