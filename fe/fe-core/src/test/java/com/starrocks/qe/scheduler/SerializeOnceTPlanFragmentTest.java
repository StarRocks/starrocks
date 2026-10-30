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

package com.starrocks.qe.scheduler;

import com.starrocks.planner.DataPartition;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.PlanFragment;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.rpc.PExecPlanFragmentRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.InternalServiceVersion;
import com.starrocks.thrift.TBucketProperty;
import com.starrocks.thrift.TDataPartition;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TUniqueId;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.utframe.MockedBackend.MockPBackendService;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SerializeOnceTPlanFragmentTest extends SchedulerTestBase {
    private static final String JOIN_AGG_SQL = "select l_orderkey, count(*), sum(l_extendedprice * (1 - l_discount)) " +
            "from lineitem join orders on l_orderkey = o_orderkey " +
            "where o_orderdate > '1995-01-01' and l_shipmode in ('AIR', 'MAIL') " +
            "group by l_orderkey order by 3 desc limit 10";
    private static final String MULTI_CAST_SQL = "with w as (select l_orderkey, count(*) as c from lineitem " +
            "group by l_orderkey) select /*+SET_VAR(cbo_cte_reuse_rate=0) */ * from w union all select * from w";

    @AfterEach
    public void resetProtocol() throws Exception {
        setPlanProtocol("binary");
    }

    private void setPlanProtocol(String protocol) throws Exception {
        GlobalStateMgr.getCurrentState().getVariableMgr().setSystemVariable(connectContext.getSessionVariable(),
                new SystemVariable(SessionVariable.THRIFT_PLAN_PROTOCOL, new StringLiteral(protocol)), true);
    }

    private static TExecPlanFragmentParams newRequest(TPlanFragment fragment) {
        // Set fields on both sides of the fragment (field 2), so that the compact protocol encodes the fields after
        // it as deltas from the fragment's field id.
        TExecPlanFragmentParams request = new TExecPlanFragmentParams();
        request.setProtocol_version(InternalServiceVersion.V1);
        request.setFragment(fragment);
        request.setParams(new TPlanFragmentExecParams());
        request.params.setQuery_id(new TUniqueId(1, 2));
        request.params.setFragment_instance_id(new TUniqueId(3, 4));
        request.params.setPer_node_scan_ranges(new HashMap<>());
        request.params.setPer_exch_num_senders(Map.of(1, 3));
        request.params.setDestinations(new ArrayList<>());
        request.setCoord(new TNetworkAddress("127.0.0.1", 9020));
        request.setBackend_num(7);
        return request;
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "compact", "json"})
    public void testSameBytesAsRegularSerialization(String protocol) throws Exception {
        ExecPlan execPlan = getExecPlan(JOIN_AGG_SQL);
        assertTrue(execPlan.getFragments().size() > 1);

        TSerializer serializer = ConfigurableSerDesFactory.getTSerializer(protocol);
        TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer(protocol);
        for (PlanFragment fragment : execPlan.getFragments()) {
            TExecPlanFragmentParams regular = newRequest(fragment.toThrift());
            SerializeOnceTPlanFragment shared = fragment.toThrift(new SerializeOnceTPlanFragment());
            assertEquals(regular.getFragment(), shared);

            byte[] expected = serializer.serialize(regular);
            // Serialize several requests sharing the fragment: the first one fills the cache, the rest use it.
            for (int i = 0; i < 3; i++) {
                assertArrayEquals(expected, serializer.serialize(newRequest(shared)));
            }
            // The standalone fragment, whose bytes are the ones cached.
            assertArrayEquals(serializer.serialize(regular.getFragment()), serializer.serialize(shared));

            TExecPlanFragmentParams decoded = new TExecPlanFragmentParams();
            deserializer.deserialize(decoded, serializer.serialize(newRequest(shared)));
            assertEquals(regular, decoded);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "compact"})
    public void testDeployedRequestsMatchRegularSerialization(String protocol) throws Exception {
        setPlanProtocol(protocol);
        TSerializer serializer = ConfigurableSerDesFactory.getTSerializer(protocol);
        TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer(protocol);
        Map<Integer, List<TPlanFragment>> rootNodeIdToFragments = new ConcurrentHashMap<>();
        setBackendService(address -> new MockPBackendService() {
            @Override
            public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                try {
                    byte[] bytes = request.getSerializedRequest();
                    TExecPlanFragmentParams tRequest = new TExecPlanFragmentParams();
                    deserializer.deserialize(tRequest, bytes);
                    // The decoded request has a regular TPlanFragment.
                    assertArrayEquals(serializer.serialize(tRequest), bytes);
                    rootNodeIdToFragments.computeIfAbsent(
                                    tRequest.getFragment().getPlan().getNodes().get(0).getNode_id(),
                                    k -> new ArrayList<>())
                            .add(tRequest.getFragment());
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                return super.execPlanFragmentAsync(request);
            }
        });

        DefaultCoordinator scheduler = startScheduling(JOIN_AGG_SQL);
        assertTrue(scheduler.getExecStatus().ok());

        assertFalse(rootNodeIdToFragments.isEmpty());
        assertTrue(rootNodeIdToFragments.values().stream().anyMatch(fragments -> fragments.size() > 1));
        rootNodeIdToFragments.values().forEach(fragments ->
                fragments.forEach(fragment -> assertEquals(fragments.get(0), fragment)));
    }

    @ParameterizedTest
    @ValueSource(strings = {JOIN_AGG_SQL, MULTI_CAST_SQL})
    public void testSharedFragmentOnlyWithoutPerInstanceSink(String sql) throws Exception {
        DefaultCoordinator scheduler = startScheduling(sql);
        assertTrue(scheduler.getExecStatus().ok());

        TFragmentInstanceFactory factory = new TFragmentInstanceFactory(connectContext, scheduler.getJobSpec(),
                scheduler.getExecutionDAG(), new TNetworkAddress("127.0.0.1", 9020));
        boolean hasMultiCast = false;
        boolean hasShared = false;
        for (ExecutionFragment execFragment : scheduler.getExecutionDAG().getFragmentsInCreatedOrder()) {
            TPlanFragment shared = factory.createSharedFragment(execFragment);
            if (execFragment.getPlanFragment() instanceof MultiCastPlanFragment) {
                hasMultiCast = true;
                assertNull(shared);
            } else if (execFragment.getInstances().size() > 1) {
                hasShared = true;
                assertInstanceOf(SerializeOnceTPlanFragment.class, shared);
            } else {
                assertNull(shared);
            }
        }
        assertEquals(sql.equals(MULTI_CAST_SQL), hasMultiCast);
        assertTrue(hasShared);
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "compact"})
    public void testDetectModificationAfterSerialization(String protocol) throws Exception {
        PlanFragment fragment = getExecPlan(JOIN_AGG_SQL).getFragments().get(0);
        SerializeOnceTPlanFragment shared = fragment.toThrift(new SerializeOnceTPlanFragment());
        TSerializer serializer = ConfigurableSerDesFactory.getTSerializer(protocol);
        serializer.serialize(newRequest(shared));

        shared.setMin_reservation_bytes(1);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> serializer.serialize(newRequest(shared)));
        assertTrue(e.getMessage().contains("modified after it was serialized"), e.getMessage());
    }

    @Test
    public void testDetectInstanceDifferingFromSharedFragment() throws Exception {
        DefaultCoordinator scheduler = startScheduling(JOIN_AGG_SQL);
        TFragmentInstanceFactory factory = new TFragmentInstanceFactory(connectContext, scheduler.getJobSpec(),
                scheduler.getExecutionDAG(), new TNetworkAddress("127.0.0.1", 9020));
        ExecutionFragment execFragment = scheduler.getExecutionDAG().getFragmentsInCreatedOrder().stream()
                .filter(fragment -> fragment.getInstances().size() > 1)
                .findFirst().orElseThrow();
        TPlanFragment shared = factory.createSharedFragment(execFragment);
        TUniqueId queryId = scheduler.getJobSpec().getQueryId();
        assertTrue(Deployer.isSharedFragmentConsistent(execFragment, shared, queryId));

        // Stands for any per-instance change that sharing would lose, from modifying either a request's fragment or
        // the plan fragment.
        shared.setMin_reservation_bytes(1);
        assertFalse(Deployer.isSharedFragmentConsistent(execFragment, shared, queryId));
    }

    @Test
    public void testFallBackToFragmentPerInstance() throws Exception {
        // Every thrift conversion gives a different fragment, so the shared fragment never matches the regular one and
        // every fragment falls back. The instances must still get the regular fragments. JMockit removes the mock
        // after the test.
        AtomicInteger numConversions = new AtomicInteger();
        new MockUp<DataPartition>() {
            @Mock
            public TDataPartition toThrift(Invocation invocation) {
                TDataPartition partition = invocation.proceed();
                partition.addToBucket_properties(new TBucketProperty().setBucket_num(numConversions.incrementAndGet()));
                return partition;
            }
        };
        List<TPlanFragment> deployedFragments = Collections.synchronizedList(new ArrayList<>());
        setBackendService(address -> new MockPBackendService() {
            @Override
            public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                TExecPlanFragmentParams tRequest = new TExecPlanFragmentParams();
                try {
                    request.getRequest(tRequest);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                deployedFragments.add(tRequest.getFragment());
                return super.execPlanFragmentAsync(request);
            }
        });

        DefaultCoordinator scheduler = startScheduling(JOIN_AGG_SQL);
        assertTrue(scheduler.getExecStatus().ok());
        // Each instance converted its own fragment, so no two deployed fragments are equal.
        assertTrue(deployedFragments.size() > 1);
        assertEquals(deployedFragments.size(), deployedFragments.stream().distinct().count());
    }
}
