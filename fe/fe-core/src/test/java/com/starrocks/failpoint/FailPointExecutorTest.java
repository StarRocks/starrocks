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

package com.starrocks.failpoint;

import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FailPointExecutorTest {
    private SystemInfoService service;

    @BeforeEach
    public void setUp() {
        service = new SystemInfoService();
    }

    @Test
    public void testResolveAllAliveBackendAndComputeNodes() throws Exception {
        Backend be = new Backend(10001, "127.0.0.1", 9050);
        be.setBePort(9060);
        be.setAlive(true);
        service.addBackend(be);

        ComputeNode cn = new ComputeNode(10002, "127.0.0.2", 9050);
        cn.setBePort(9060);
        cn.setAlive(true);
        service.addComputeNode(cn);

        ComputeNode deadCn = new ComputeNode(10003, "127.0.0.3", 9050);
        deadCn.setBePort(9060);
        deadCn.setAlive(false);
        service.addComputeNode(deadCn);

        List<ComputeNode> nodes = FailPointExecutor.resolveNodes(service, null);
        Assertions.assertEquals(2, nodes.size());
        Assertions.assertEquals(
                Arrays.asList(10001L, 10002L),
                nodes.stream().map(ComputeNode::getId).sorted().collect(Collectors.toList()));
    }

    @Test
    public void testResolveExplicitComputeNodeAddr() throws Exception {
        ComputeNode cn = new ComputeNode(10002, "10.0.0.2", 9050);
        cn.setBePort(9060);
        cn.setAlive(true);
        service.addComputeNode(cn);

        List<ComputeNode> nodes = FailPointExecutor.resolveNodes(service,
                Collections.singletonList("10.0.0.2:9060"));
        Assertions.assertEquals(1, nodes.size());
        Assertions.assertEquals(10002L, nodes.get(0).getId());
    }

    @Test
    public void testResolveEmptyClusterThrows() {
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> FailPointExecutor.resolveNodes(service, null));
        Assertions.assertTrue(e.getMessage().contains("No alive backends or compute nodes"));
    }

    @Test
    public void testResolveUnknownAddrThrows() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> FailPointExecutor.resolveNodes(service,
                        Collections.singletonList("10.0.0.9:9060")));
        Assertions.assertTrue(e.getMessage().contains("cannot find backend or compute node"));
    }
}
