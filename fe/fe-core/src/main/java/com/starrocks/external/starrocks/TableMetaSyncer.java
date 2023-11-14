// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.starrocks;

import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.thrift.TGetTableMetaRequest;
import com.starrocks.thrift.TGetTableMetaResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TableMetaSyncer is used to sync olap external
// table meta info from remote dorisdb cluster
public class TableMetaSyncer {
    private static final Logger LOG = LogManager.getLogger(TableMetaSyncer.class);

    public void syncTable(ExternalOlapTable table) throws MetaNotFoundException {
        String host = table.getSourceTableHost();
        int port = table.getSourceTablePort();
        TNetworkAddress addr = new TNetworkAddress(host, port);
        TGetTableMetaRequest request = new TGetTableMetaRequest();
        request.setDb_name(table.getSourceTableDbName());
        request.setTable_name(table.getSourceTableName());
        try {
            TGetTableMetaResponse response = FrontendServiceProxy.call(addr,
                    Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.getTableMeta(request));
            if (response.status.getStatus_code() != TStatusCode.OK) {
                String errMsg;
                if (response.status.getError_msgs() != null) {
                    errMsg = String.join(",", response.status.getError_msgs());
                } else {
                    errMsg = "";
                }
                LOG.warn("get TableMeta failed: {}", errMsg);
                throw new MetaNotFoundException(errMsg);
            } else {
                table.updateMeta(request.getDb_name(), response.getTable_meta(), response.getBackends());
            }
        } catch (Exception e) {
            LOG.warn("call fe {} refreshTable rpc method failed", addr, e);
            throw new MetaNotFoundException("get TableMeta failed from " + addr + ", error: " + e.getMessage());
        }
    }
}