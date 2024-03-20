// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.system;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.DropComputeNodeLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SystemInfoServiceEpack extends SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(SystemInfoServiceEpack.class);

    public SystemInfoServiceEpack() {
        super();
    }

    @Override
    public void addComputeNodes(List<Pair<String, Integer>> hostPortPairs, String warehouseName)
            throws DdlException {

        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) != null) {
                throw new DdlException("Same backend already exists[" + pair.first + ":" + pair.second + "]");
            }
            if (getComputeNodeWithHeartbeatPort(pair.first, pair.second) != null) {
                throw new DdlException("Same compute node already exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addComputeNode(pair.first, pair.second, warehouseName);
        }
    }

    // Final entry of adding compute node
    private void addComputeNode(String host, int heartbeatPort, String warehouseName)
            throws DdlException {
        ComputeNode newComputeNode = new ComputeNode(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        setComputeNodeOwner(newComputeNode);
        addComuteNodeToWarehouse(newComputeNode, warehouseName);
        idToComputeNodeRef.put(newComputeNode.getId(), newComputeNode);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddComputeNode(newComputeNode);
        LOG.info("finished to add {} ", newComputeNode);
    }

    public void addComuteNodeToWarehouse(ComputeNode computeNode, String warehouseName)
            throws DdlException {
        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
        // check if the warehouse exist
        if (warehouse == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
        }

        computeNode.setWorkerGroupId(warehouse.getAnyAvailableCluster().getWorkerGroupId());
        computeNode.setWarehouseId(warehouse.getId());
    }

    /**
     * @param hostPortPairs : backend's host and port
     * @throws DdlException
     */
    @Override
    public void addBackends(List<Pair<String, Integer>> hostPortPairs, String warehouseName)
            throws DdlException {

        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) != null) {
                throw new DdlException("Same backend already exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addBackend(pair.first, pair.second, warehouseName);
        }
    }

    // Final entry of adding backend
    private void addBackend(String host, int heartbeatPort, String warehouseName)
            throws DdlException {
        Backend newBackend = new Backend(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        // add backend to DEFAULT_CLUSTER
        setBackendOwner(newBackend);
        addComuteNodeToWarehouse(newBackend, warehouseName);

        // update idToBackend
        idToBackendRef.put(newBackend.getId(), newBackend);

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddBackend(newBackend);
        LOG.info("finished to add {} ", newBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public void dropComputeNodes(List<Pair<String, Integer>> hostPortPairs, String warehouseName)
            throws DdlException {

        // check if the warehouse exist
        if (GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            ComputeNode cn = getComputeNodeWithHeartbeatPort(pair.first, pair.second);
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
            dropComputeNode(pair.first, pair.second);
        }
    }

    @Override
    public void dropComputeNode(String host, int heartbeatPort)
            throws DdlException {
        ComputeNode dropComputeNode = getComputeNodeWithHeartbeatPort(host, heartbeatPort);
        if (dropComputeNode == null) {
            throw new DdlException("compute node does not exists[" + host + ":" + heartbeatPort + "]");
        }

        // remove worker
        if (RunMode.isSharedDataMode()) {
            long starletPort = dropComputeNode.getStarletPort();
            // only need to remove worker after be reported its staretPort
            if (starletPort != 0) {
                String workerAddr = dropComputeNode.getHost() + ":" + starletPort;
                GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorker(workerAddr, dropComputeNode.getWorkerGroupId());
            }
        }

        // update idToComputeNode
        idToComputeNodeRef.remove(dropComputeNode.getId());

        // log
        GlobalStateMgr.getCurrentState().getEditLog()
                .logDropComputeNode(new DropComputeNodeLog(dropComputeNode.getId()));
        LOG.info("finished to drop {}", dropComputeNode);
    }

    public void dropBackends(DropBackendClause dropBackendClause, String warehouseName) throws DdlException {
        List<Pair<String, Integer>> hostPortPairs = dropBackendClause.getHostPortPairs();
        boolean needCheckUnforce = !dropBackendClause.isForce();

        // check if the warehouse exist
        if (GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            Backend be = getBackendWithHeartbeatPort(pair.first, pair.second);
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
            dropBackend(pair.first, pair.second, needCheckUnforce);
        }
    }

    @Override
    // final entry of dropping backend
    public void dropBackend(String host, int heartbeatPort, boolean needCheckUnforce) throws DdlException {

        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);
        if (droppedBackend == null) {
            throw new DdlException("backend does not exists[" + host + ":" + heartbeatPort + "]");
        }

        if (needCheckUnforce) {
            try {
                checkWhenNotForceDrop(droppedBackend);
            } catch (RuntimeException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // remove worker
        if (RunMode.isSharedDataMode()) {
            long starletPort = droppedBackend.getStarletPort();
            // only need to remove worker after be reported its staretPort
            if (starletPort != 0) {
                String workerAddr = droppedBackend.getHost() + ":" + starletPort;
                GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorker(workerAddr, droppedBackend.getWorkerGroupId());
            }
        }

        // update idToBackend
        idToBackendRef.remove(droppedBackend.getId());

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(droppedBackend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logDropBackend(droppedBackend);
        LOG.info("finished to drop {}", droppedBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    @Override
    public void dropNodes(long warehouseId) throws DdlException {
        List<ComputeNode> nodes = backendAndComputeNodeStream().
                filter(cn -> cn.getWarehouseId() == warehouseId).collect(Collectors.toList());

        for (ComputeNode node : nodes) {
            try {
                if (node instanceof Backend) {
                    dropBackend(node.getHost(), node.getHeartbeatPort(), false);
                } else {
                    dropComputeNode(node.getHost(), node.getHeartbeatPort());
                }
            } catch (DdlException e) {
                if (e.getMessage().contains("compute node does not exists")
                        || e.getMessage().contains("backend does not exists")) {
                    continue;
                } else {
                    throw e;
                }
            }
        }
    }

}
