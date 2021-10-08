// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.starrocks;

import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.common.ClientPool;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TGetTableMetaRequest;
import com.starrocks.thrift.TGetTableMetaResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

// TableMetaSyncer is used to sync olap external 
// table meta info from remote dorisdb cluster
public class TableMetaSyncer {
    private static final Logger LOG = LogManager.getLogger(TableMetaSyncer.class);

    public void syncTable(ExternalOlapTable table) {
        String host = table.getSourceTableHost();
        int port = table.getSourceTablePort();
        TNetworkAddress addr = new TNetworkAddress(host, port);
        FrontendService.Client client = null;
        try {
            client = ClientPool.frontendPool.borrowObject(addr, 1000);
        } catch (Exception e) {
            LOG.warn("get frontend client from pool failed", e);
            return;
        }

        TGetTableMetaRequest request = new TGetTableMetaRequest();
        request.setDb_name(table.getSourceTableDbName());
        request.setTable_name(table.getSourceTableName());
        TAuthenticateParams authInfo = new TAuthenticateParams();
        authInfo.setUser(table.getSourceTableUser());
        authInfo.setPasswd(table.getSourceTablePassword());
        request.setAuth_info(authInfo);
        boolean returnToPool = false;
        try {
            TGetTableMetaResponse response = client.getTableMeta(request);
            returnToPool = true;
            List<String> errmsgs = response.status.getError_msgs(); 
            if (response.status.getStatus_code() != TStatusCode.OK) {
                LOG.info("errmsg: {}", errmsgs.get(0));
            } else {
                table.updateMeta(request.getDb_name(), response.getTable_meta(), response.getBackends());
            }
        } catch (Exception e) {
            LOG.warn("call fe {} refreshTable rpc method failed", addr, e);
        }
        if (returnToPool) {
            ClientPool.frontendPool.returnObject(addr, client);
        } else {
            ClientPool.frontendPool.invalidateObject(addr, client);
        }
    }
};