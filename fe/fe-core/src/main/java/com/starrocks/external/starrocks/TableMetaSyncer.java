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


package com.starrocks.external.starrocks;

import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.common.Config;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.sql.common.MetaNotFoundException;
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