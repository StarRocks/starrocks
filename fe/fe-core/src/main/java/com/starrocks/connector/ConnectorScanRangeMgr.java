// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector;

import com.starrocks.thrift.TScanRangeLocations;

public interface ConnectorScanRangeMgr {

    /**
     * Used to get scan range locations for given connector.
     * @return
     */
    default TScanRangeLocations getSplits()
    {
        return null;
    }
}
