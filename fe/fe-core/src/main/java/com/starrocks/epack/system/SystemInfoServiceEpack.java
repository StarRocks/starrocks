// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.system;

import com.starrocks.common.DdlException;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

public class SystemInfoServiceEpack extends SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(SystemInfoServiceEpack.class);

    public SystemInfoServiceEpack() {
        super();
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
