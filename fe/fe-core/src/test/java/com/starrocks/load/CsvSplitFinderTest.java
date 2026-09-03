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
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.PCsvFileSplits;
import com.starrocks.proto.PGetCsvSplitsResult;
import com.starrocks.proto.StatusPB;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PGetCsvSplitsRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Covers the frontend half of csv split discovery: what is asked of a backend, and how each shape
 * of reply is judged. The backend is mocked, so what is under test is the contract rather than the
 * framing itself - CSVRecordFramerTest and CSVScannerTest cover that side.
 */
public class CsvSplitFinderTest {
    private static final TNetworkAddress ADDRESS = new TNetworkAddress("127.0.0.1", 8060);
    private static final long SPLIT_SIZE = 64 * 1024 * 1024L;

    /**
     * A scan range carrying the fields thrift insists on, so that the request really does serialise.
     * Without them it would fail on the way out and the failure paths below would pass for the
     * wrong reason.
     */
    private static TBrokerScanRange scanRangeOf(int fileCount) {
        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        params.setColumn_separator((byte) ',');
        params.setRow_delimiter((byte) '\n');
        params.setSrc_tuple_id(0);
        params.setSrc_slot_ids(new ArrayList<>());
        params.setDest_tuple_id(1);
        params.setEnclose((byte) '"');

        TBrokerScanRange scanRange = new TBrokerScanRange();
        scanRange.setParams(params);
        scanRange.addToBroker_addresses(new TNetworkAddress("", 0));
        for (int i = 0; i < fileCount; i++) {
            TBrokerRangeDesc range = new TBrokerRangeDesc();
            range.setFile_type(TFileType.FILE_BROKER);
            range.setFormat_type(TFileFormatType.FORMAT_CSV_PLAIN);
            range.setSplittable(true);
            range.setPath("hdfs://127.0.0.1:9001/f" + i);
            range.setStart_offset(0);
            range.setSize(500000000L);
            scanRange.addToRanges(range);
        }
        return scanRange;
    }

    /**
     * One file's worth of offsets. Written out rather than nested inside Lists.newArrayList, which
     * would bind the element type to Long instead of List&lt;Long&gt;.
     */
    private static List<List<Long>> forOneFile(Long... offsets) {
        List<List<Long>> perFile = new ArrayList<>();
        perFile.add(Lists.newArrayList(offsets));
        return perFile;
    }

    private static PGetCsvSplitsResult okResult(List<List<Long>> perFile) {
        PGetCsvSplitsResult result = new PGetCsvSplitsResult();
        result.status = new StatusPB();
        result.status.statusCode = TStatusCode.OK.getValue();
        result.splits = new ArrayList<>();
        for (List<Long> offsets : perFile) {
            PCsvFileSplits fileSplits = new PCsvFileSplits();
            fileSplits.offsets = offsets;
            result.splits.add(fileSplits);
        }
        return result;
    }

    private static void backendReplies(PGetCsvSplitsResult result) {
        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PGetCsvSplitsResult> getCsvSplits(TNetworkAddress address, PGetCsvSplitsRequest request) {
                return CompletableFuture.completedFuture(result);
            }
        };
    }

    @Test
    public void testNoFilesAsksNobody() throws StarRocksException {
        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PGetCsvSplitsResult> getCsvSplits(TNetworkAddress address, PGetCsvSplitsRequest request) {
                throw new IllegalStateException("no file to split, so no backend should be asked");
            }
        };

        Assertions.assertTrue(CsvSplitFinder.findSplits(ADDRESS, scanRangeOf(0), SPLIT_SIZE).isEmpty());
    }

    @Test
    public void testSplitSizeMustBePositive() {
        TBrokerScanRange scanRange = scanRangeOf(1);
        Assertions.assertThrows(StarRocksException.class, () -> CsvSplitFinder.findSplits(ADDRESS, scanRange, 0));
        Assertions.assertThrows(StarRocksException.class, () -> CsvSplitFinder.findSplits(ADDRESS, scanRange, -1));
    }

    @Test
    public void testReturnsOneOffsetListPerFileInOrder() throws StarRocksException {
        List<Long> first = Lists.newArrayList(0L, 70000000L, 140000000L);
        List<Long> second = Lists.newArrayList(0L, 90000000L);
        backendReplies(okResult(Lists.newArrayList(first, second)));

        List<List<Long>> offsets = CsvSplitFinder.findSplits(ADDRESS, scanRangeOf(2), SPLIT_SIZE);

        Assertions.assertEquals(2, offsets.size());
        Assertions.assertEquals(first, offsets.get(0));
        Assertions.assertEquals(second, offsets.get(1));
    }

    @Test
    public void testFileWithNoOffsetsBecomesAnEmptyList() throws StarRocksException {
        List<List<Long>> perFile = new ArrayList<>();
        perFile.add(null);
        backendReplies(okResult(perFile));

        List<List<Long>> offsets = CsvSplitFinder.findSplits(ADDRESS, scanRangeOf(1), SPLIT_SIZE);

        Assertions.assertEquals(1, offsets.size());
        Assertions.assertTrue(offsets.get(0).isEmpty());
    }

    @Test
    public void testReplyWithoutStatusIsRejected() {
        PGetCsvSplitsResult result = okResult(forOneFile(0L));
        result.status = null;
        backendReplies(result);

        StarRocksException e = Assertions.assertThrows(StarRocksException.class,
                () -> CsvSplitFinder.findSplits(ADDRESS, scanRangeOf(1), SPLIT_SIZE));
        Assertions.assertTrue(e.getMessage().contains("no status"), e.getMessage());
    }

    @Test
    public void testBackendErrorIsRejected() {
        PGetCsvSplitsResult result = okResult(forOneFile(0L));
        result.status.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
        result.status.errorMsgs = Lists.newArrayList("could not read the file");
        backendReplies(result);

        StarRocksException e = Assertions.assertThrows(StarRocksException.class,
                () -> CsvSplitFinder.findSplits(ADDRESS, scanRangeOf(1), SPLIT_SIZE));
        Assertions.assertTrue(e.getMessage().contains("could not read the file"), e.getMessage());
    }

    /**
     * Offsets are matched back to files by position, so a reply of the wrong length cannot be used
     * at all - taking the first few would silently attach one file's boundaries to another.
     */
    @Test
    public void testReplyOfTheWrongLengthIsRejected() {
        backendReplies(okResult(forOneFile(0L)));

        StarRocksException e = Assertions.assertThrows(StarRocksException.class,
                () -> CsvSplitFinder.findSplits(ADDRESS, scanRangeOf(2), SPLIT_SIZE));
        Assertions.assertTrue(e.getMessage().contains("1 results for 2 files"), e.getMessage());
    }

    @Test
    public void testMissingSplitsIsRejected() {
        PGetCsvSplitsResult result = okResult(new ArrayList<>());
        result.splits = null;
        backendReplies(result);

        Assertions.assertThrows(StarRocksException.class,
                () -> CsvSplitFinder.findSplits(ADDRESS, scanRangeOf(1), SPLIT_SIZE));
    }

    /**
     * An older backend has no get_csv_splits at all, so the call fails rather than replying. That
     * has to arrive as an exception: an empty result would read as "this file has no records", and
     * the caller needs to tell the difference so it can fall back to loading the file whole.
     */
    @Test
    public void testRpcFailureIsRejected() {
        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PGetCsvSplitsResult> getCsvSplits(TNetworkAddress address, PGetCsvSplitsRequest request)
                    throws RpcException {
                throw new RpcException("127.0.0.1", "unknown method get_csv_splits");
            }
        };

        StarRocksException e = Assertions.assertThrows(StarRocksException.class,
                () -> CsvSplitFinder.findSplits(ADDRESS, scanRangeOf(1), SPLIT_SIZE));
        Assertions.assertTrue(e.getMessage().contains("failed to get csv splits"), e.getMessage());
    }

    @Test
    public void testInterruptionIsPassedOn() {
        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PGetCsvSplitsResult> getCsvSplits(TNetworkAddress address, PGetCsvSplitsRequest request) {
                return new Future<PGetCsvSplitsResult>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return false;
                    }

                    @Override
                    public PGetCsvSplitsResult get() throws InterruptedException {
                        throw new InterruptedException("interrupted while waiting for csv splits");
                    }

                    @Override
                    public PGetCsvSplitsResult get(long timeout, TimeUnit unit) throws InterruptedException {
                        throw new InterruptedException("interrupted while waiting for csv splits");
                    }
                };
            }
        };

        Assertions.assertThrows(StarRocksException.class,
                () -> CsvSplitFinder.findSplits(ADDRESS, scanRangeOf(1), SPLIT_SIZE));
        // The finder re-asserts the flag it swallowed, so the caller can still see the interruption.
        // Reading it also clears it, leaving the test thread as it was found.
        Assertions.assertTrue(Thread.interrupted());
    }
}
