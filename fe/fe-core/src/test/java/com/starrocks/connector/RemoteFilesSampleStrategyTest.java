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

package com.starrocks.connector;

import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class RemoteFilesSampleStrategyTest {
    private static class MockedConnectorScanRangeSource extends ConnectorScanRangeSource {
        private boolean hasMoreOutput = true;
        private final List<TScanRangeLocations> scanRangeLocations = new ArrayList<>();

        MockedConnectorScanRangeSource(int fileNum) {
            for (int i = 0; i < fileNum; i++) {
                THdfsScanRange hdfsScanRange = new THdfsScanRange();
                hdfsScanRange.setFile_format(THdfsFileFormat.PARQUET);
                hdfsScanRange.setFull_path("test://test/".concat(Integer.toString(i)));
                TScanRange scanRange = new TScanRange();
                scanRange.setHdfs_scan_range(hdfsScanRange);
                TScanRangeLocations scan = new TScanRangeLocations();
                scan.setScan_range(scanRange);
                scanRangeLocations.add(scan);
            }
        }

        @Override
        public List<TScanRangeLocations> getSourceOutputs(int maxSize) {
            hasMoreOutput = false;
            return scanRangeLocations;
        }

        @Override
        public boolean sourceHasMoreOutput() {
            return hasMoreOutput;
        }
    }

    @Test
    public void testPassThrough() {
        ConnectorScanRangeSource source = new MockedConnectorScanRangeSource(10);
        int count = 0;
        while (source.hasMoreOutput()) {
            count += source.getOutputs(100).size();
        }
        Assert.assertEquals(10, count);
    }

    @Test
    public void testRandomLessWithChecker() {
        ConnectorScanRangeSource source = new MockedConnectorScanRangeSource(4);
        Predicate<THdfsScanRange> formatChecker = x -> x.isSetFile_format() && x.getFile_format().equals(
                THdfsFileFormat.PARQUET);

        source.setSampleStrategy(new RemoteFilesSampleStrategy(5, formatChecker));
        int count = 0;
        while (source.hasMoreOutput()) {
            count += source.getOutputs(100).size();
        }
        Assert.assertEquals(4, count);
    }

    @Test
    public void testRandomMoreWithChecker() {
        ConnectorScanRangeSource source = new MockedConnectorScanRangeSource(10);
        Predicate<THdfsScanRange> formatChecker = x -> x.isSetFile_format() && x.getFile_format().equals(
                THdfsFileFormat.PARQUET);

        source.setSampleStrategy(new RemoteFilesSampleStrategy(5, formatChecker));
        int count = 0;
        while (source.hasMoreOutput()) {
            count += source.getOutputs(100).size();
        }
        Assert.assertEquals(5, count);
    }

    @Test
    public void testCheckerWrongFormat() {
        ConnectorScanRangeSource source = new MockedConnectorScanRangeSource(10);
        Predicate<THdfsScanRange> formatChecker = x -> x.isSetFile_format() && x.getFile_format().equals(
                THdfsFileFormat.ORC);

        source.setSampleStrategy(new RemoteFilesSampleStrategy(5, formatChecker));
        int count = 0;
        while (source.hasMoreOutput()) {
            count += source.getOutputs(100).size();
        }
        Assert.assertEquals(0, count);
    }

    @Test
    public void testSpecificFileName() {
        ConnectorScanRangeSource source = new MockedConnectorScanRangeSource(10);
        source.setSampleStrategy(new RemoteFilesSampleStrategy("test://test/5"));
        List<TScanRangeLocations> scanRanges = new ArrayList<>();
        while (source.hasMoreOutput()) {
            scanRanges.addAll(source.getOutputs(100));
        }
        Assert.assertEquals(scanRanges.size(), 1);
        Assert.assertEquals("test://test/5", scanRanges.get(0).getScan_range().getHdfs_scan_range().getFull_path());
    }

}
