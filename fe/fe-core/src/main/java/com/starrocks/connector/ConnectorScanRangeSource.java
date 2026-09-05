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

import com.starrocks.catalog.Table;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public abstract class ConnectorScanRangeSource implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(ConnectorScanRangeSource.class);

    RemoteFilesSampleStrategy strategy = new RemoteFilesSampleStrategy();

    protected abstract List<TScanRangeLocations> getSourceOutputs(int maxSize);

    public List<TScanRangeLocations> getOutputs(int maxSize) {
        return strategy.sample(getSourceOutputs(maxSize));
    }

    public List<TScanRangeLocations> getAllOutputs() {
        return getOutputs(Integer.MAX_VALUE);
    }

    protected abstract boolean sourceHasMoreOutput();

    public boolean hasMoreOutput() {
        if (strategy.enough()) {
            return false;
        }
        return sourceHasMoreOutput();
    }

    public void setSampleStrategy(RemoteFilesSampleStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public void close() throws Exception {
        // default no-op
    }

    // Enforces scan_lake_partition_num_limit incrementally as new partitions are discovered during scan-range
    // dispatch.
    protected static void checkPartitionNumLimit(Table table, int selectedPartitionCount) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return;
        }
        int limit = connectContext.getSessionVariable().getScanLakePartitionNumLimit();
        if (limit <= 0 || selectedPartitionCount <= limit) {
            return;
        }
        String msg = "Exceeded the limit of number of " + table.getCatalogName() + "." + table.getCatalogDBName()
                + "." + table.getCatalogTableName() + " partitions to be scanned. "
                + "Number of partitions allowed: " + limit + ", number of partitions to be scanned: "
                + selectedPartitionCount + ". Please adjust the SQL or change the limit by set variable "
                + "scan_lake_partition_num_limit.";
        LOG.warn("{} queryId: {}", msg, DebugUtil.printId(connectContext.getQueryId()));
        throw new StarRocksConnectorException(msg);
    }
}
