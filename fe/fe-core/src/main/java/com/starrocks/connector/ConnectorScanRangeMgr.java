// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector;

import com.starrocks.catalog.Table;
import com.starrocks.thrift.TScanRangeLocations;

/**
 * Each connector needs to provide its own scan range calculation logic by implementing this
 * interface. We will call ScanRangeMgr#getScanRanges in the core engine, the core engine will
 * get scan range locations use ConnectorScanRangeMgr for each connector. For example, if we are
 * using hive connector, then the core engine will call HiveScanRangeMgr to get TScanRangeLocations.
 */
public interface ConnectorScanRangeMgr {

    /**
     * Used to get scan range locations for given connector.
     * @return
     */
    default TScanRangeLocations getScanRanges(Table table, )
    {
        return null;
    }
}
