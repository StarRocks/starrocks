// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner.stream;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TIMTDescriptor;
import com.starrocks.thrift.TIMTType;

/**
 * Intermediate materialized table
 */
public class IMTInfo {

    private TIMTType type;
    private boolean needMaintain;
    private OlapTableRouteInfo olapTable;

    public static IMTInfo fromOlapTable(long dbId, OlapTable table, boolean needMaintain) throws UserException {
        IMTInfo res = new IMTInfo();
        res.type = TIMTType.OLAP_TABLE;
        res.needMaintain = needMaintain;
        res.olapTable = OlapTableRouteInfo.create(dbId, table);
        return res;
    }

    public TIMTDescriptor toThrift() {
        TIMTDescriptor desc = new TIMTDescriptor();
        desc.setImt_type(this.type);
        desc.setNeed_maintain(this.needMaintain);
        desc.setOlap_table(this.olapTable.toThrift());
        return desc;
    }

    public OlapTable toOlapTable() {
        Preconditions.checkState(type == TIMTType.OLAP_TABLE);
        return olapTable.getOlapTable();
    }

    @Override
    public String toString() {
        return String.format("%s/%s", type.toString(), olapTable.getTableName());
    }
}
