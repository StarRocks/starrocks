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
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.epack.sql.ast.AddBackendClauseEPack;
import com.starrocks.epack.sql.ast.AddComputeNodeClauseEPack;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.CancelDecommissionDiskClause;
import com.starrocks.epack.sql.ast.DecommissionDiskClause;
import com.starrocks.epack.sql.ast.DisableDiskClause;
import com.starrocks.epack.sql.ast.DropBackendClauseEPack;
import com.starrocks.epack.sql.ast.DropComputeNodeClauseEPack;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        private static final Logger LOG = LogManager.getLogger(SystemHandlerEPack.class);
        private static final SystemHandlerEPack.Visitor INSTANCE = new SystemHandlerEPack.Visitor();

        public static SystemHandlerEPack.Visitor getInstance() {
            return INSTANCE;
        }

        public void addComputeNodeToWarehouse(ComputeNode computeNode, String warehouseName)
                throws DdlException {
            LocalWarehouse warehouse = (LocalWarehouse) GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getWarehouse(warehouseName);
            // check if the warehouse exist
            if (warehouse == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
            }

            computeNode.setWorkerGroupId(warehouse.getAnyAvailableCluster().getWorkerGroupId());
            computeNode.setWarehouseId(warehouse.getId());
        }

        @Override
        public Void visitAddBackendClause(AddBackendClauseEPack alterClause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                SystemInfoService systemInfoService =
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

                String warehouseName = alterClause.getWarehouse();
                List<Pair<String, Integer>> hostPortPairs = alterClause.getAddBackendClause().getHostPortPairs();

                for (Pair<String, Integer> pair : hostPortPairs) {
                    systemInfoService.checkSameNodeExist(pair.first, pair.second);
                }

                for (Pair<String, Integer> pair : hostPortPairs) {
                    Backend newBackend = new Backend(GlobalStateMgr.getCurrentState().getNextId(), pair.first, pair.second);
                    systemInfoService.setBackendOwner(newBackend);
                    addComputeNodeToWarehouse(newBackend, warehouseName);
                    systemInfoService.addBackend(newBackend);

                    // log
                    GlobalStateMgr.getCurrentState().getEditLog().logAddBackend(newBackend);
                    LOG.info("finished to add {} ", newBackend);

                    // backends are changed, regenerated tablet number metrics
                    MetricRepo.generateBackendsTabletMetrics();
                }
            });
            return null;
        }

        @Override
        public Void visitAddComputeNodeClause(AddComputeNodeClauseEPack alterClause, Void context) {
            try {
                SystemInfoService systemInfoService =
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

                String warehouseName = alterClause.getWarehouse();
                List<Pair<String, Integer>> hostPortPairs = alterClause.getAddComputeNodeClause().getHostPortPairs();

                for (Pair<String, Integer> pair : hostPortPairs) {
                    systemInfoService.checkSameNodeExist(pair.first, pair.second);
                }

                for (Pair<String, Integer> pair : hostPortPairs) {
                    ComputeNode newComputeNode = new ComputeNode(GlobalStateMgr.getCurrentState().getNextId(),
                            pair.first, pair.second);
                    systemInfoService.setComputeNodeOwner(newComputeNode);
                    addComputeNodeToWarehouse(newComputeNode, warehouseName);
                    systemInfoService.addComputeNode(newComputeNode);

                    // log
                    GlobalStateMgr.getCurrentState().getEditLog().logAddComputeNode(newComputeNode);
                    LOG.info("finished to add {} ", newComputeNode);
                }
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public Void visitDropBackendClause(DropBackendClauseEPack alterClause, Void context) {
            try {
                SystemInfoService systemInfoService =
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

                String warehouseName = alterClause.getWarehouse();
                List<Pair<String, Integer>> hostPortPairs = alterClause.getDropBackendClause().getHostPortPairs();

                boolean needCheckUnforce = !alterClause.getDropBackendClause().isForce();

                // check if the warehouse exist
                if (GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName) == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
                }

                for (Pair<String, Integer> pair : hostPortPairs) {
                    Backend be = systemInfoService.getBackendWithHeartbeatPort(pair.first, pair.second);
                    // check is already exist
                    if (be == null) {
                        throw new DdlException("backend does not exists[" + pair.first + ":" + pair.second + "]");
                    }

                    // check if warehouseName is right
                    Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(be.getWarehouseId());
                    if (wh != null && !warehouseName.equalsIgnoreCase(wh.getName())) {
                        LOG.warn("warehouseName in dropBackends is not equal, " +
                                        "warehouseName from dropBackendClause is {}, while actual one is {}",
                                warehouseName, wh.getName());
                        throw new DdlException("backend [" + pair.first + ":" + pair.second +
                                "] does not exist in warehouse " + warehouseName);
                    }
                }

                for (Pair<String, Integer> pair : hostPortPairs) {
                    systemInfoService.dropBackend(pair.first, pair.second, needCheckUnforce);
                }
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public Void visitDropComputeNodeClause(DropComputeNodeClauseEPack alterClause, Void context) {
            try {
                SystemInfoService systemInfoService =
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

                String warehouseName = alterClause.getWarehouse();
                List<Pair<String, Integer>> hostPortPairs = alterClause.getDropComputeNodeClause().getHostPortPairs();

                // check if the warehouse exist
                if (GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName) == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
                }

                for (Pair<String, Integer> pair : hostPortPairs) {
                    // check is already exist
                    ComputeNode cn = systemInfoService.getComputeNodeWithHeartbeatPort(pair.first, pair.second);
                    if (cn == null) {
                        throw new DdlException("compute node does not exists[" + pair.first + ":" + pair.second + "]");
                    }
                    // check if warehouseName is right
                    Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(cn.getWarehouseId());
                    if (wh != null && !warehouseName.equalsIgnoreCase(wh.getName())) {
                        throw new DdlException("compute node [" + pair.first + ":" + pair.second +
                                "] does not exist in warehouse " + warehouseName);
                    }
                }

                for (Pair<String, Integer> pair : hostPortPairs) {
                    systemInfoService.dropComputeNode(pair.first, pair.second);
                }
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
    }
}
