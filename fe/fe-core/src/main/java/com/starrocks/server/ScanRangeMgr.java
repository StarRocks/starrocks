// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.starrocks.connector.ConnectorScanRangeMgr;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;

public class ScanRangeMgr {
    private static final Logger LOG = LogManager.getLogger(ScanRangeMgr.class);

    private final ConcurrentMap<String, ConnectorScanRangeMgr> scanRangeMgrs = new ConcurrentHashMap<>();

    public TScanRangeLocations getScanRanges(String catalog, PhysicalScanOperator scanOperator) {
        return scanRangeMgrs.get(catalog).getScanRanges(scanOperator);
    }

    public void addScanRangeMgr(String catalogName, ConnectorScanRangeMgr scanRangeMgr) {
        checkState(scanRangeMgrs.putIfAbsent(catalogName, scanRangeMgr) == null, "ScanRangeMgr for connector '%s' is already registered", catalogName);
    }

    public void removeScanRangeMgr(String catalogName)
    {
        scanRangeMgrs.remove(catalogName);
    }
}
