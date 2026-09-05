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

package com.starrocks.warehouse;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;
import com.starrocks.system.ComputeNode;

import java.util.ArrayList;
import java.util.List;

public class DefaultWarehouse extends Warehouse {

    private static final List<Long> WORKER_GROUP_ID_LIST;

    public DefaultWarehouse(long id, String name) {
        super(id, name, "An internal warehouse init after FE is ready");
    }

    static {
        WORKER_GROUP_ID_LIST = ImmutableList.of(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
    }

    @Override
    public List<Long> getWorkerGroupIds() {
        return WORKER_GROUP_ID_LIST;
    }

    @Override
    public Long getAnyWorkerGroupId() {
        return StarOSAgent.DEFAULT_WORKER_GROUP_ID;
    }

    @Override
    public void addNodeToCNGroup(ComputeNode node, String cnGroupName) throws DdlException {
        if (!Strings.isNullOrEmpty(cnGroupName)) {
            // NOTE: NOT IMPLEMENTED, so the cnGroupName must be empty!
            ErrorReport.reportDdlException(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED);
        }
        node.setWorkerGroupId(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
        node.setWarehouseId(getId());
    }

    @Override
    public void validateRemoveNodeFromCNGroup(ComputeNode node, String cnGroupName) throws DdlException {
        if (!Strings.isNullOrEmpty(cnGroupName)) {
            // NOTE: NOT IMPLEMENTED, so the cnGroupName must be empty!
            ErrorReport.reportDdlException(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED);
        }
    }

    @Override
    public List<String> getWarehouseInfo() {
        return Lists.newArrayList(
                String.valueOf(getId()),
                getName(),
                "AVAILABLE",
                String.valueOf(0L),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(0L),   //TODO: need to be filled after
                String.valueOf(0L),   //TODO: need to be filled after
                "",
                "",
                "",
                "",
                comment);
    }

    @Override
    public List<List<String>> getWarehouseNodesInfo() {
        return new ArrayList<>();
    }

    @Override
    public ProcResult fetchResult() {
        return new BaseProcResult();
    }

    @Override
    public void createCNGroup(CreateCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    @Override
    public void dropCNGroup(DropCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    @Override
    public void enableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    @Override
    public void disableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    @Override
    public void alterCNGroup(AlterCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    @Override
    public void replayInternalOpLog(String payload) {

    }

    @Override
    public long getResumeTime() {
        return -1L;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }
}
