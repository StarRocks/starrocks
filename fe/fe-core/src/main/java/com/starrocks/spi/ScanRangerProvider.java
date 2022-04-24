package com.starrocks.spi;

import com.starrocks.thrift.TScanRangeLocations;

import java.util.List;

public interface ScanRangerProvider {
    List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength);
}
