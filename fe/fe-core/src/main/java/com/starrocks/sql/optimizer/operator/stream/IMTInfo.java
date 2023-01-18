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

package com.starrocks.sql.optimizer.operator.stream;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.UserException;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TIMTDescriptor;
import com.starrocks.thrift.TIMTType;
import com.starrocks.thrift.TUniqueId;

/**
 * Intermediate materialized table
 */
public class IMTInfo {

    private TIMTType type;
    private OlapTableRouteInfo olapTable;

    private TUniqueId loadId;
    private long txnId;

    public static IMTInfo fromTableName(long dbId, TupleDescriptor outputTupleDesc,
                                        TableName tableName) throws UserException {
        Preconditions.checkState(tableName != null);
        OlapTable resultTable = (OlapTable) MetaUtils.getTable(tableName);
        IMTInfo res = new IMTInfo();
        res.type = TIMTType.OLAP_TABLE;
        res.olapTable = OlapTableRouteInfo.create(dbId, resultTable, outputTupleDesc);
        return res;
    }

    public String getName() {
        return this.olapTable.getTableName();
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
        // desc.setLoad_id(this.loadId);
        // desc.setTxn_id(this.txnId);
        return desc;
    }

    public OlapTable toOlapTable() {
        Preconditions.checkState(type == TIMTType.OLAP_TABLE);
        return olapTable.getOlapTable();
    }

    @Override
    public String toString() {
        return String.format("IMT type=%s, table_name=%s", type.toString(), olapTable.getTableName());
    }
}