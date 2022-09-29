// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner.stream;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TIMTDescriptor;
import com.starrocks.thrift.TIMTType;
import com.starrocks.thrift.TUniqueId;

/**
 * Intermediate materialized table
 */
public class IMTInfo {

    private TIMTType type;
    private OlapTableRouteInfo olapTable;

    // If this IMT needs to be maintained by operator
    private boolean needMaintain;
    private TUniqueId loadId;
    private long txnId;

    public static IMTInfo fromOlapTable(long dbId, OlapTable table, boolean needMaintain) throws UserException {
        IMTInfo res = new IMTInfo();
        res.type = TIMTType.OLAP_TABLE;
        res.needMaintain = needMaintain;
        res.olapTable = OlapTableRouteInfo.create(dbId, table);
        return res;
    }

    public String getName() {
        return this.olapTable.getTableName();
    }

    public boolean isNeedMaintain() {
        return needMaintain;
    }

    public void setNeedMaintain(boolean needMaintain) {
        this.needMaintain = needMaintain;
    }

    public void setLoadInfo(TUniqueId loadId, long txnId) {
        this.loadId = loadId;
        this.txnId = txnId;
    }

    public void finalizeTupleDescriptor(DescriptorTable descTable, TupleDescriptor tupleDesc) {
        olapTable.finalizeTupleDescriptor(descTable, tupleDesc);
    }

    public TIMTDescriptor toThrift() {
        TIMTDescriptor desc = new TIMTDescriptor();
        desc.setImt_type(this.type);
        desc.setOlap_table(this.olapTable.toThrift());
        desc.setNeed_maintain(this.needMaintain);
        if (this.needMaintain) {
            desc.setLoad_id(this.loadId);
            desc.setTxn_id(this.txnId);
        }
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
