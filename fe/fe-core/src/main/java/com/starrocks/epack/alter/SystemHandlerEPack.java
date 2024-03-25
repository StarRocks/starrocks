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
package com.starrocks.epack.alter;

import com.google.common.base.Preconditions;
import com.starrocks.alter.SystemHandler;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.epack.sql.ast.AddBackendClauseEPack;
import com.starrocks.epack.sql.ast.AddComputeNodeClauseEPack;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.CancelDecommissionDiskClause;
import com.starrocks.epack.sql.ast.CancelDisableDiskClause;
import com.starrocks.epack.sql.ast.DecommissionDiskClause;
import com.starrocks.epack.sql.ast.DisableDiskClause;
import com.starrocks.epack.sql.ast.DropBackendClauseEPack;
import com.starrocks.epack.sql.ast.DropComputeNodeClauseEPack;
import com.starrocks.epack.system.SystemInfoServiceEpack;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterClause;

import java.util.List;

public class SystemHandlerEPack extends SystemHandler {
    public SystemHandlerEPack() {
        super();
    }

    @Override
    // add synchronized to avoid process 2 or more stmts at same time
    public synchronized ShowResultSet process(List<AlterClause> alterClauses, Database dummyDb,
                                              OlapTable dummyTbl) throws UserException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);
        alterClause.accept(SystemHandlerEPack.Visitor.getInstance(), null);
        return null;
    }

    protected static class Visitor extends SystemHandler.Visitor implements AstVisitorEPack<Void, Void> {
        private static final SystemHandlerEPack.Visitor INSTANCE = new SystemHandlerEPack.Visitor();

        public static SystemHandlerEPack.Visitor getInstance() {
            return INSTANCE;
        }

        @Override
        public Void visitAddBackendClause(AddBackendClauseEPack alterClause, Void context) {
            SystemInfoServiceEpack systemInfoServiceEpack =
                    (SystemInfoServiceEpack) GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            try {
                systemInfoServiceEpack.addBackends(
                        alterClause.getAddBackendClause().getHostPortPairs(), alterClause.getWarehouse());
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public Void visitAddComputeNodeClause(AddComputeNodeClauseEPack alterClause, Void context) {
            SystemInfoServiceEpack systemInfoServiceEpack =
                    (SystemInfoServiceEpack) GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            try {
                systemInfoServiceEpack.addComputeNodes(
                        alterClause.getAddComputeNodeClause().getHostPortPairs(), alterClause.getWarehouse());
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public Void visitDropBackendClause(DropBackendClauseEPack alterClause, Void context) {
            SystemInfoServiceEpack systemInfoServiceEpack =
                    (SystemInfoServiceEpack) GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            try {
                systemInfoServiceEpack.dropBackends(alterClause.getDropBackendClause(), alterClause.getWarehouse());
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public Void visitDropComputeNodeClause(DropComputeNodeClauseEPack alterClause, Void context) {
            SystemInfoServiceEpack systemInfoServiceEpack =
                    (SystemInfoServiceEpack) GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            try {
                systemInfoServiceEpack.dropComputeNodes(alterClause.getDropComputeNodeClause().getHostPortPairs(),
                        alterClause.getWarehouse());
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public Void visitDecommissionDiskClause(DecommissionDiskClause clause, Void context) {
            try {

                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                        .decommissionDisks(clause.getBeHostPort(), clause.getDiskList());
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public Void visitCancelDecommissionDiskClause(CancelDecommissionDiskClause clause, Void context) {
            try {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                        .cancelDecommissionDisks(clause.getBeHostPort(), clause.getDiskList());
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public Void visitDisableDiskClause(DisableDiskClause clause, Void context) {
            try {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                        .disableDisks(clause.getBeHostPort(), clause.getDiskList());
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public Void visitCancelDisableDiskClause(CancelDisableDiskClause clause, Void context) {
            try {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                        .cancelDisableDisks(clause.getBeHostPort(), clause.getDiskList());
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
            return null;
        }
    }
}
