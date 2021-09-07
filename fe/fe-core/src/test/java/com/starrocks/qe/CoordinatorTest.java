// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/CoordinatorTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.EditLog;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataStreamSink;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.HashJoinNode;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.Planner;
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.Backend;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CoordinatorTest extends Coordinator {
    static Planner planner = new Planner();
    static ConnectContext context = new ConnectContext(null);

    static {
        context.setExecutionId(new TUniqueId(1, 2));
    }

    @Mocked
    static Catalog catalog;
    @Mocked
    static EditLog editLog;
    @Mocked
    static FrontendOptions frontendOptions;
    static Analyzer analyzer = new Analyzer(catalog, null);

    public CoordinatorTest() {
        super(context, analyzer, planner);
    }

    private static Coordinator coor;

    @Test
    public void testComputeColocateJoinInstanceParam() {
        Coordinator coordinator = new Coordinator(context, analyzer, planner);

        // 1. set fragmentToBucketSeqToAddress in coordinator
        Map<Integer, TNetworkAddress> bucketSeqToAddress = new HashMap<>();
        TNetworkAddress address = new TNetworkAddress();
        for (int i = 0; i < 3; i++) {
            bucketSeqToAddress.put(i, address);
        }
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
        Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentToBucketSeqToAddress = new HashMap<>();
        fragmentToBucketSeqToAddress.put(planFragmentId, bucketSeqToAddress);
        Deencapsulation.setField(coordinator, "fragmentIdToSeqToAddressMap", fragmentToBucketSeqToAddress);

        // 2. set bucketSeqToScanRange in coordinator
        BucketSeqToScanRange bucketSeqToScanRange = new BucketSeqToScanRange();
        Map<Integer, List<TScanRangeParams>> ScanRangeMap = new HashMap<>();
        ScanRangeMap.put(1, new ArrayList<>());
        for (int i = 0; i < 3; i++) {
            bucketSeqToScanRange.put(i, ScanRangeMap);
        }

        Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap = Maps.newHashMap();
        fragmentIdBucketSeqToScanRangeMap.put(planFragmentId, bucketSeqToScanRange);
        Deencapsulation.setField(coordinator, "fragmentIdBucketSeqToScanRangeMap", fragmentIdBucketSeqToScanRangeMap);

        FragmentExecParams params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeColocateJoinInstanceParam", planFragmentId, 1, params);
        Assert.assertEquals(1, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeColocateJoinInstanceParam", planFragmentId, 2, params);
        Assert.assertEquals(2, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeColocateJoinInstanceParam", planFragmentId, 3, params);
        Assert.assertEquals(3, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeColocateJoinInstanceParam", planFragmentId, 5, params);
        Assert.assertEquals(3, params.instanceExecParams.size());
    }

    @Test
    public void testIsBucketShuffleJoin() {
        Coordinator coordinator = new Coordinator(context, analyzer, planner);

        PlanNodeId testStarrocksNodeId = new PlanNodeId(-1);
        TupleId testTupleId = new TupleId(-1);
        ArrayList<TupleId> tupleIdArrayList = new ArrayList<>();
        tupleIdArrayList.add(testTupleId);

        ArrayList<Expr> testJoinexprs = new ArrayList<>();
        BinaryPredicate binaryPredicate = new BinaryPredicate();
        testJoinexprs.add(binaryPredicate);

        HashJoinNode hashJoinNode = new HashJoinNode(testStarrocksNodeId, new EmptySetNode(testStarrocksNodeId, tupleIdArrayList),
                new EmptySetNode(testStarrocksNodeId, tupleIdArrayList), new TableRef(), testJoinexprs, new ArrayList<>());
        hashJoinNode.setFragment(new PlanFragment(new PlanFragmentId(-1), hashJoinNode,
                new DataPartition(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED, testJoinexprs)));

        // hash join node is not bucket shuffle join
        Assert.assertEquals(false,
                Deencapsulation.invoke(coordinator, "isBucketShuffleJoin", -1, hashJoinNode));

        // the fragment id is different from hash join node
        hashJoinNode.setFragment(new PlanFragment(new PlanFragmentId(-2), hashJoinNode,
                new DataPartition(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED, testJoinexprs)));
        hashJoinNode.setDistributionMode(HashJoinNode.DistributionMode.BUCKET_SHUFFLE);
        Assert.assertEquals(false,
                Deencapsulation.invoke(coordinator, "isBucketShuffleJoin", -1, hashJoinNode));

        hashJoinNode.setFragment(new PlanFragment(new PlanFragmentId(-1), hashJoinNode,
                new DataPartition(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED, testJoinexprs)));
        Deencapsulation.setField(hashJoinNode, "isLocalBucketShuffle", true);
        Assert.assertEquals(true,
                Deencapsulation.invoke(coordinator, "isBucketShuffleJoin", -1, hashJoinNode));

        // the fragment id is in cache, so not do check node again
        Assert.assertEquals(true,
                Deencapsulation.invoke(coordinator, "isBucketShuffleJoin", -1));

    }

    @Test
    public void testComputeBucketShuffleJoinInstanceParam() {
        Coordinator coordinator = new Coordinator(context, analyzer, planner);

        // 1. set fragmentToBucketSeqToAddress in bucketShuffleJoinController
        Map<Integer, TNetworkAddress> bucketSeqToAddress = new HashMap<>();
        TNetworkAddress address = new TNetworkAddress();
        for (int i = 0; i < 3; i++) {
            bucketSeqToAddress.put(i, address);
        }
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
        Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentToBucketSeqToAddress = new HashMap<>();
        fragmentToBucketSeqToAddress.put(planFragmentId, bucketSeqToAddress);
        Deencapsulation.setField(coordinator, "fragmentIdToSeqToAddressMap", fragmentToBucketSeqToAddress);

        // 2. set bucketSeqToScanRange in bucketShuffleJoinController
        Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap = new HashMap<>();
        BucketSeqToScanRange bucketSeqToScanRange = new BucketSeqToScanRange();
        Map<Integer, List<TScanRangeParams>> ScanRangeMap = new HashMap<>();
        ScanRangeMap.put(1, new ArrayList<>());
        for (int i = 0; i < 3; i++) {
            bucketSeqToScanRange.put(i, ScanRangeMap);
        }
        fragmentIdBucketSeqToScanRangeMap.put(planFragmentId, bucketSeqToScanRange);
        Deencapsulation.setField(coordinator, "fragmentIdBucketSeqToScanRangeMap", fragmentIdBucketSeqToScanRangeMap);

        FragmentExecParams params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeBucketShuffleJoinInstanceParam", planFragmentId, 1, params);
        Assert.assertEquals(1, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeBucketShuffleJoinInstanceParam", planFragmentId, 2, params);
        Assert.assertEquals(2, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeBucketShuffleJoinInstanceParam", planFragmentId, 3, params);
        Assert.assertEquals(3, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeBucketShuffleJoinInstanceParam", planFragmentId, 5, params);
        Assert.assertEquals(3, params.instanceExecParams.size());
    }

    @Test
    public void testPruneBucketForBucketShuffle(@Mocked Catalog catalog) {
        Coordinator coordinator = new Coordinator(context, analyzer, planner);

        Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = Maps.newHashMap();

        Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap = Maps.newHashMap();

        PlanFragment destPlanFragment = new PlanFragment(new PlanFragmentId(0), null, null);
        FragmentExecParams destFragmentParam = new FragmentExecParams(destPlanFragment);
        // dest planFragment only has one instanceExecParams
        FInstanceExecParam instanceExecParam =
                new FInstanceExecParam(null, new TNetworkAddress("127.0.0.1", 9999), 0, destFragmentParam);
        // This instanceExecParams only has one bucket
        instanceExecParam.addBucketSeq(0);
        destFragmentParam.instanceExecParams.add(instanceExecParam);

        Set<Integer> bucketShuffleFragmentIds = new HashSet<>();
        bucketShuffleFragmentIds.add(destPlanFragment.getFragmentId().asInt());
        Deencapsulation.setField(coordinator, "bucketShuffleFragmentIds", bucketShuffleFragmentIds);
        // dest plan fragment have 3 buckets
        fragmentIdToBucketNumMap.put(destPlanFragment.getFragmentId(), 3);
        Deencapsulation.setField(coordinator, "fragmentIdToBucketNumMap", fragmentIdToBucketNumMap);

        PlanFragment shufflePlanFragment = new PlanFragment(new PlanFragmentId(1), null, null);
        OlapScanNode scanNode = new OlapScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), null);
        scanNode.setFragment(shufflePlanFragment);
        ExchangeNode dest = new ExchangeNode(new PlanNodeId(1), scanNode);
        DataStreamSink dataStreamSink = new DataStreamSink(dest.getId());
        dataStreamSink.setPartition(new DataPartition(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED,
                Lists.newArrayList(new SlotRef(new TableName(), "xxx"))));
        shufflePlanFragment.setSink(dataStreamSink);
        dest.setFragment(destPlanFragment);
        shufflePlanFragment.setDestination(dest);
        // Shuffle plan fragment will shuffle data to dest plan fragment
        FragmentExecParams shuffleFragmentParam = new FragmentExecParams(shufflePlanFragment);
        shuffleFragmentParam.instanceExecParams
                .add(new FInstanceExecParam(null, new TNetworkAddress("127.0.0.2", 9999), 0, shuffleFragmentParam));
        shuffleFragmentParam.instanceExecParams
                .add(new FInstanceExecParam(null, new TNetworkAddress("127.0.0.3", 9999), 0, shuffleFragmentParam));
        shuffleFragmentParam.instanceExecParams
                .add(new FInstanceExecParam(null, new TNetworkAddress("127.0.0.4", 9999), 0, shuffleFragmentParam));

        fragmentExecParamsMap.put(new PlanFragmentId(0), destFragmentParam);
        fragmentExecParamsMap.put(new PlanFragmentId(1), shuffleFragmentParam);

        Deencapsulation.setField(coordinator, "fragmentExecParamsMap", fragmentExecParamsMap);

        Backend be = new Backend(10001, "127.0.0.1", 0);
        be.setBeRpcPort(9999);
        new Expectations() {
            {
                catalog.getCurrentSystemInfo().getBackendWithBePort("127.0.0.1", 9999);
                result = be;
                minTimes = 0;
            }
        };

        Deencapsulation.invoke(coordinator, "computeFragmentExecParams");
        Assert.assertEquals(3, shuffleFragmentParam.destinations.size());
        Assert.assertNotEquals(-1, shuffleFragmentParam.destinations.get(0).fragment_instance_id.lo);
        Assert.assertEquals("127.0.0.1", shuffleFragmentParam.destinations.get(0).getServer().hostname);
        Assert.assertEquals(9999, shuffleFragmentParam.destinations.get(0).getServer().port);

        Assert.assertEquals(-1, shuffleFragmentParam.destinations.get(1).fragment_instance_id.lo);
        Assert.assertEquals("0.0.0.0", shuffleFragmentParam.destinations.get(1).getServer().hostname);
        Assert.assertEquals(0, shuffleFragmentParam.destinations.get(1).getServer().port);

        Assert.assertEquals(-1, shuffleFragmentParam.destinations.get(2).fragment_instance_id.lo);
        Assert.assertEquals("0.0.0.0", shuffleFragmentParam.destinations.get(2).getServer().hostname);
        Assert.assertEquals(0, shuffleFragmentParam.destinations.get(2).getServer().port);
    }

    private TScanRangeLocations createScanRangeLocations(String fileName, List<String> hosts, long scanRangeBytes) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path(fileName);
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(scanRangeBytes);
        hdfsScanRange.setPartition_id(0);
        hdfsScanRange.setFile_length(10);
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        for (String host : hosts) {
            TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(host, -1));
            scanRangeLocations.addToLocations(scanRangeLocation);
        }
        return scanRangeLocations;
    }

    @Test
    public void testHybridBackendSelectorForHybridDeployment(@Injectable HdfsScanNode scanNode) {
        Coordinator coordinator = new Coordinator(context, analyzer, planner);
        // set scan nodes
        Deencapsulation.setField(coordinator, "scanNodes", Lists.newArrayList(scanNode));

        // set fragmentExecParamsMap
        PlanFragmentId fragmentId = new PlanFragmentId(0);
        Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = Deencapsulation.getField(
                coordinator, "fragmentExecParamsMap");
        fragmentExecParamsMap.put(fragmentId, new FragmentExecParams(null));

        // set idToBackend
        Map<Long, Backend> idToBackend = Maps.newHashMap();
        String host0 = "127.0.0.0";
        Backend be0 = new Backend(0, host0, 9050);
        be0.setAlive(true);
        be0.setBePort(9060);
        String host1 = "127.0.0.1";
        Backend be1 = new Backend(1, host1, 9050);
        be1.setAlive(true);
        be1.setBePort(9060);
        String host2 = "127.0.0.2";
        Backend be2 = new Backend(2, host2, 9050);
        be2.setAlive(true);
        be2.setBePort(9060);
        String host3 = "127.0.0.3";
        Backend be3 = new Backend(3, host3, 9050);
        be3.setAlive(true);
        be3.setBePort(9060);
        idToBackend.put(be0.getId(), be0);
        idToBackend.put(be1.getId(), be1);
        idToBackend.put(be2.getId(), be2);
        idToBackend.put(be3.getId(), be3);
        Deencapsulation.setField(coordinator, "idToBackend", ImmutableMap.copyOf(idToBackend));

        // set scan range locations
        //
        // file locations:
        // each file has 1 block and 2 replicas
        // only host 0 - 2 have hdfs files
        //
        // 0000_0:  host0  host1
        // 0000_1:  host1  host2
        // 0000_2:  host0  host2
        // 0000_3:  host0  host1
        // 0000_4:  host1  host2
        // 0000_5:  host0  host2
        Map<String, List<String>> fileToHosts = Maps.newHashMap();
        String file0 = "0000_0";
        String file1 = "0000_1";
        String file2 = "0000_2";
        String file3 = "0000_3";
        String file4 = "0000_4";
        String file5 = "0000_5";
        fileToHosts.put(file0, Lists.newArrayList(host0, host1));
        fileToHosts.put(file1, Lists.newArrayList(host1, host2));
        fileToHosts.put(file2, Lists.newArrayList(host0, host2));
        fileToHosts.put(file3, Lists.newArrayList(host0, host1));
        fileToHosts.put(file4, Lists.newArrayList(host1, host2));
        fileToHosts.put(file5, Lists.newArrayList(host0, host2));
        List<TScanRangeLocations> locations = Lists.newArrayList();
        locations.add(createScanRangeLocations(file0, fileToHosts.get(file0), 200));
        locations.add(createScanRangeLocations(file1, fileToHosts.get(file1), 400));
        locations.add(createScanRangeLocations(file2, fileToHosts.get(file2), 500));
        locations.add(createScanRangeLocations(file3, fileToHosts.get(file3), 300));
        locations.add(createScanRangeLocations(file4, fileToHosts.get(file4), 200));
        locations.add(createScanRangeLocations(file5, fileToHosts.get(file5), 400));

        new Expectations() {
            {
                scanNode.getScanRangeLocations(0);
                result = locations;
                scanNode.getFragmentId();
                result = fragmentId;
                scanNode.getId();
                result = new PlanNodeId(0);
            }
        };

        // set forceScheduleLocal false
        Deencapsulation.setField(coordinator, "forceScheduleLocal", false);

        // compute
        Deencapsulation.invoke(coordinator, "computeScanRangeAssignment");

        // check result
        FragmentScanRangeAssignment assignment =
                fragmentExecParamsMap.get(scanNode.getFragmentId()).scanRangeAssignment;
        System.out.println("Hybrid deployment, force local: false, assignment: " + assignment);
        // each be has 1 scan range at least
        Assert.assertEquals(4, assignment.size());
        // 5 scan ranges are scheduled to local be
        int hostMatches = 0;
        for (Map.Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> entry : assignment.entrySet()) {
            Map<Integer, List<TScanRangeParams>> scanNodeToParams = entry.getValue();
            int paramsSize = scanNodeToParams.get(scanNode.getId().asInt()).size();
            Assert.assertTrue(paramsSize == 1 || paramsSize == 2);

            String host = entry.getKey().hostname;
            for (List<TScanRangeParams> params : scanNodeToParams.values()) {
                for (TScanRangeParams param : params) {
                    String file = param.scan_range.hdfs_scan_range.relative_path;
                    if (fileToHosts.get(file).contains(host)) {
                        hostMatches += 1;
                    }
                }
            }
        }
        Assert.assertEquals(5, hostMatches);

        // set forceScheduleLocal true
        Deencapsulation.setField(coordinator, "forceScheduleLocal", true);

        // compute
        assignment.clear();
        Deencapsulation.invoke(coordinator, "computeScanRangeAssignment");
        System.out.println("Hybrid deployment, force local: true, assignment: " + assignment);

        // check result
        // one remote be (host3) has no scan range
        Assert.assertEquals(3, assignment.size());
        Assert.assertTrue(!assignment.containsKey(new TNetworkAddress(be3.getHost(), be3.getBePort())));
        // all 6 scan ranges are scheduled to local be
        hostMatches = 0;
        for (Map.Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> entry : assignment.entrySet()) {
            Map<Integer, List<TScanRangeParams>> scanNodeToParams = entry.getValue();
            int paramsSize = scanNodeToParams.get(scanNode.getId().asInt()).size();
            Assert.assertTrue(paramsSize == 1 || paramsSize == 2 || paramsSize == 3);

            String host = entry.getKey().hostname;
            for (List<TScanRangeParams> params : scanNodeToParams.values()) {
                for (TScanRangeParams param : params) {
                    String file = param.scan_range.hdfs_scan_range.relative_path;
                    if (fileToHosts.get(file).contains(host)) {
                        hostMatches += 1;
                    }
                }
            }
        }
        Assert.assertEquals(6, hostMatches);
    }

    @Test
    public void testHybridBackendSelectorForIndependentDeployment(@Injectable HdfsScanNode scanNode) {
        Coordinator coordinator = new Coordinator(context, analyzer, planner);
        // set scan nodes
        Deencapsulation.setField(coordinator, "scanNodes", Lists.newArrayList(scanNode));

        // set fragmentExecParamsMap
        PlanFragmentId fragmentId = new PlanFragmentId(0);
        Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = Deencapsulation.getField(
                coordinator, "fragmentExecParamsMap");
        fragmentExecParamsMap.put(fragmentId, new FragmentExecParams(null));

        // set idToBackend
        Map<Long, Backend> idToBackend = Maps.newHashMap();
        // host 3 - 4 are be
        String host3 = "127.0.0.3";
        Backend be3 = new Backend(3, host3, 9050);
        be3.setAlive(true);
        be3.setBePort(9060);
        String host4 = "127.0.0.4";
        Backend be4 = new Backend(4, host4, 9050);
        be4.setAlive(true);
        be4.setBePort(9060);
        idToBackend.put(be3.getId(), be3);
        idToBackend.put(be4.getId(), be4);
        Deencapsulation.setField(coordinator, "idToBackend", ImmutableMap.copyOf(idToBackend));

        // set scan range locations
        //
        // file locations:
        // each file has 1 block and 2 replicas
        // only host 0 - 2 have hdfs files
        //
        // 0000_0:  host0  host1
        // 0000_1:  host1  host2
        // 0000_2:  host0  host2
        // 0000_3:  host0  host1

        // host 0 - 2 are for hdfs files
        String host0 = "127.0.0.0";
        String host1 = "127.0.0.1";
        String host2 = "127.0.0.2";
        Map<String, List<String>> fileToHosts = Maps.newHashMap();
        String file0 = "0000_0";
        String file1 = "0000_1";
        String file2 = "0000_2";
        String file3 = "0000_3";
        fileToHosts.put(file0, Lists.newArrayList(host0, host1));
        fileToHosts.put(file1, Lists.newArrayList(host1, host2));
        fileToHosts.put(file2, Lists.newArrayList(host0, host2));
        fileToHosts.put(file3, Lists.newArrayList(host0, host1));
        List<TScanRangeLocations> locations = Lists.newArrayList();
        locations.add(createScanRangeLocations(file0, fileToHosts.get(file0), 200));
        locations.add(createScanRangeLocations(file1, fileToHosts.get(file1), 400));
        locations.add(createScanRangeLocations(file2, fileToHosts.get(file2), 500));
        locations.add(createScanRangeLocations(file3, fileToHosts.get(file3), 300));

        new Expectations() {
            {
                scanNode.getScanRangeLocations(0);
                result = locations;
                scanNode.getFragmentId();
                result = fragmentId;
                scanNode.getId();
                result = new PlanNodeId(0);
            }
        };

        // set forceScheduleLocal true, no matter
        Deencapsulation.setField(coordinator, "forceScheduleLocal", true);

        // compute
        Deencapsulation.invoke(coordinator, "computeScanRangeAssignment");

        // check result
        FragmentScanRangeAssignment assignment =
                fragmentExecParamsMap.get(scanNode.getFragmentId()).scanRangeAssignment;
        System.out.println("Independent deployment, assignment: " + assignment);
        // each be has 2 scan ranges
        Assert.assertEquals(2, assignment.size());
        // no scan ranges are scheduled to local be
        int hostMatches = 0;
        for (Map.Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> entry : assignment.entrySet()) {
            Map<Integer, List<TScanRangeParams>> scanNodeToParams = entry.getValue();
            int paramsSize = scanNodeToParams.get(scanNode.getId().asInt()).size();
            Assert.assertTrue(paramsSize == 2);

            String host = entry.getKey().hostname;
            for (List<TScanRangeParams> params : scanNodeToParams.values()) {
                for (TScanRangeParams param : params) {
                    String file = param.scan_range.hdfs_scan_range.relative_path;
                    if (fileToHosts.get(file).contains(host)) {
                        hostMatches += 1;
                    }
                }
            }
        }
        Assert.assertEquals(0, hostMatches);
    }
}