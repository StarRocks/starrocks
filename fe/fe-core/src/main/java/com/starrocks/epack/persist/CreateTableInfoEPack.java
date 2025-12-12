// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.epack.authorization.MaskingPolicyContext;
import com.starrocks.epack.authorization.RowAccessPolicyContext;
import com.starrocks.epack.authorization.TableUID;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateTableInfoEPack extends CreateTableInfo {
    @SerializedName(value = "mp")
    private List<ApplyOrRevokeMaskingPolicyLog> applyOrRevokeMaskingPolicyLogs;

    @SerializedName(value = "rp")
    private List<ApplyOrRevokeRowAccessPolicyLog> applyOrRevokeRowAccessPolicyLogs;

    public CreateTableInfoEPack() {
    }

    public CreateTableInfoEPack(String dbName, Table table, String storageVolumeId,
                                Map<String, WithColumnMaskingPolicy> maskingPolicyContextMap,
                                List<WithRowAccessPolicy> withRowAccessPolicyList) {
        super(dbName, table, storageVolumeId);
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        if (table.isTemporaryTable()) {
            context.setSessionId(((OlapTable) table).getSessionId());
        }
        if (maskingPolicyContextMap != null) {
            applyOrRevokeMaskingPolicyLogs = new ArrayList<>();
            for (Map.Entry<String, WithColumnMaskingPolicy> m : maskingPolicyContextMap.entrySet()) {
                applyOrRevokeMaskingPolicyLogs.add(new ApplyOrRevokeMaskingPolicyLog(
                        TableUID.generate(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName, table.getName()),
                        table.getColumn(m.getKey()).getColumnId(),
                        new MaskingPolicyContext(m.getValue().getPolicyId(),
                                MetaUtils.getColumnIdsByColumnNames(table, m.getValue().getUsingColumns())))
                );
            }
        }

        if (withRowAccessPolicyList != null) {
            applyOrRevokeRowAccessPolicyLogs = new ArrayList<>();
            for (WithRowAccessPolicy withRowAccessPolicy : withRowAccessPolicyList) {
                applyOrRevokeRowAccessPolicyLogs.add(new ApplyOrRevokeRowAccessPolicyLog(
                        TableUID.generate(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName, table.getName()),
                        new RowAccessPolicyContext(withRowAccessPolicy.getPolicyId(),
                                MetaUtils.getColumnIdsByColumnNames(table, withRowAccessPolicy.getOnColumns()))));
            }
        }
    }

    public List<ApplyOrRevokeMaskingPolicyLog> getApplyOrRevokeMaskingPolicyLogs() {
        return applyOrRevokeMaskingPolicyLogs;
    }

    public List<ApplyOrRevokeRowAccessPolicyLog> getApplyOrRevokeRowAccessPolicyLogs() {
        return applyOrRevokeRowAccessPolicyLogs;
    }
}
