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

import com.starrocks.common.StarRocksException;
import com.starrocks.proto.PCsvFileSplits;
import com.starrocks.proto.PGetCsvSplitsResult;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PGetCsvSplitsRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TGetCsvSplitsRequest;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Finds the offsets at which records begin in CSV files, so that a file can be cut into ranges that
 * each start on a real record.
 *
 * <p>A row delimiter is not by itself a record boundary: inside an enclosed field, or after an
 * escape character, the CSV parser reads it as ordinary data. Whether a given offset sits inside an
 * enclosed field depends on every byte before it, so it cannot be worked out by looking at the bytes
 * around it - the file has to be read from the start. That read is done once, on a backend, by a
 * scan that only frames records and parses nothing, and its result is shared by every range the
 * file is cut into.
 *
 * <p>Without it, a range beginning at an arbitrary offset can only guess where its first record
 * starts, and a file with multi-line fields is silently mis-split (issue #65245).
 */
public class CsvSplitFinder {
    private static final Logger LOG = LogManager.getLogger(CsvSplitFinder.class);

    private CsvSplitFinder() {
    }

    /**
     * Returns one list of record start offsets per file in {@code scanRange}, in the same order.
     * Each list is ascending, begins with 0, and has consecutive entries at least
     * {@code splitSize} bytes apart.
     *
     * <p>{@code scanRange} must carry the CSV dialect and the filesystem properties needed to read
     * the files, exactly as a scan would; only uncompressed CSV can be split this way.
     */
    public static List<List<Long>> findSplits(TBrokerScanRange scanRange, long splitSize)
            throws StarRocksException {
        if (scanRange.getRangesSize() == 0) {
            return new ArrayList<>();
        }
        if (splitSize <= 0) {
            throw new StarRocksException("split size must be positive, got " + splitSize);
        }

        PGetCsvSplitsResult result;
        TNetworkAddress address = pickNode();
        try {
            TGetCsvSplitsRequest tRequest = new TGetCsvSplitsRequest();
            TScanRange tScanRange = new TScanRange();
            tScanRange.setBroker_scan_range(scanRange);
            tRequest.setScan_range(tScanRange);
            tRequest.setSplit_size(splitSize);

            PGetCsvSplitsRequest pRequest = new PGetCsvSplitsRequest();
            pRequest.setRequest(tRequest);

            Future<PGetCsvSplitsResult> future = BackendServiceClient.getInstance().getCsvSplits(address, pRequest);
            result = future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StarRocksException("failed to get csv splits", e);
        } catch (Exception e) {
            throw new StarRocksException("failed to get csv splits: " + e.getMessage());
        }

        if (TStatusCode.findByValue(result.status.statusCode) != TStatusCode.OK) {
            throw new StarRocksException("failed to get csv splits, error: " + result.status.errorMsgs);
        }
        if (result.splits == null || result.splits.size() != scanRange.getRangesSize()) {
            throw new StarRocksException("csv split discovery returned "
                    + (result.splits == null ? 0 : result.splits.size()) + " results for "
                    + scanRange.getRangesSize() + " files");
        }

        List<List<Long>> offsets = new ArrayList<>(result.splits.size());
        int total = 0;
        for (PCsvFileSplits fileSplits : result.splits) {
            offsets.add(fileSplits.offsets == null ? new ArrayList<>() : fileSplits.offsets);
            total += offsets.get(offsets.size() - 1).size();
        }
        // Worth a line at info: this reads the files end to end, so an operator looking at load
        // timings should be able to see that it happened and for how many files.
        LOG.info("found {} csv record boundaries across {} files on {}, split size {}",
                total, scanRange.getRangesSize(), address.getHostname(), splitSize);
        return offsets;
    }

    private static TNetworkAddress pickNode() throws StarRocksException {
        List<Long> nodeIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
        if (RunMode.isSharedDataMode()) {
            nodeIds.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNodeIds(true));
        }
        if (nodeIds.isEmpty()) {
            throw new StarRocksException(RunMode.isSharedNothingMode()
                    ? "Failed to get csv splits. No alive backends"
                    : "Failed to get csv splits. No alive backends or compute nodes");
        }

        Collections.shuffle(nodeIds);
        ComputeNode node =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeIds.get(0));
        return new TNetworkAddress(node.getHost(), node.getBrpcPort());
    }
}
