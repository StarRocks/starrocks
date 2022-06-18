// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.starrocks.connector.ConnectorScanRangeMgr;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ScanRangeMgr {
    private final ConcurrentMap<String, ConnectorScanRangeMgr> scanRangeMgrs = new ConcurrentHashMap<>();

    //TODO: register and remove mgrs
    
    public TScanRangeLocations getScanRanges(String catalog) {
        return scanRangeMgrs.get(catalog).getSplits();
    }
}
