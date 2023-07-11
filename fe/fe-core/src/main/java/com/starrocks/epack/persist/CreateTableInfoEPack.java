// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.epack.privilege.MaskingPolicyContext;
import com.starrocks.epack.privilege.RowAccessPolicyContext;
import com.starrocks.epack.privilege.TableUID;
import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.persist.CreateTableInfo;

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

        if (maskingPolicyContextMap != null) {
            applyOrRevokeMaskingPolicyLogs = new ArrayList<>();
            for (Map.Entry<String, WithColumnMaskingPolicy> m : maskingPolicyContextMap.entrySet()) {
                applyOrRevokeMaskingPolicyLogs.add(new ApplyOrRevokeMaskingPolicyLog(
                        TableUID.generate(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName, table.getName()),
                        m.getKey(),
                        new MaskingPolicyContext(m.getValue().getPolicyId(), m.getValue().getUsingColumns()))
                );
            }
        }

        if (withRowAccessPolicyList != null) {
            applyOrRevokeRowAccessPolicyLogs = new ArrayList<>();
            for (WithRowAccessPolicy withRowAccessPolicy : withRowAccessPolicyList) {
                applyOrRevokeRowAccessPolicyLogs.add(new ApplyOrRevokeRowAccessPolicyLog(
                        TableUID.generate(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName, table.getName()),
                        new RowAccessPolicyContext(withRowAccessPolicy.getPolicyId(), withRowAccessPolicy.getOnColumns())));
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
