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

import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IncrementalDeployHiveTest extends SchedulerConnectorTestBase {

    @Test
    public void testSchedule() throws Exception {
        // test different settings.
        connectContext.getSessionVariable().setEnableConnectorIncrementalScanRanges(true);
        {
            connectContext.getSessionVariable().setConnectorIncrementalScanRangeNumber(20);
            runSchedule();
        }
        {
            connectContext.getSessionVariable().setConnectorIncrementalScanRangeNumber(1000);
            runSchedule();
        }
        {
            connectContext.getSessionVariable().setConnectorIncrementalScanRangeNumber(20);
            connectContext.getSessionVariable().setEnablePhasedScheduler(true);
            runSchedule();
        }
    }

    public void runSchedule() throws Exception {
        String sql = "select * from hive0.file_split_db.file_split_tbl";
        List<TExecPlanFragmentParams> requests = new ArrayList<>();
        int maxScanRangeNumber = connectContext.getSessionVariable().getConnectorIncrementalScanRangeNumber();
        // deploy
        new MockUp<Deployer>() {
            @Mock
            public void deployFragments(DeployState deployState) {
                System.out.println("----- deploy fragments ------");
                final List<List<FragmentInstanceExecState>> state =
                        deployState.getThreeStageExecutionsToDeploy();
                int scanRangeNumber = 0;
                for (List<FragmentInstanceExecState> execStates : state) {
                    for (FragmentInstanceExecState execState : execStates) {
                        {
                            TPlanFragmentExecParams params = execState.getRequestToDeploy().params;
                            // there is a placeholder
                            if (!params.getPer_node_scan_ranges().isEmpty()) {
                                scanRangeNumber += params.getPer_node_scan_rangesSize() - 1;
                            }
                            for (List<TScanRangeParams> v : params.getPer_node_scan_ranges().values()) {
                                for (TScanRangeParams p : v) {
                                    System.out.println(p + ", " + System.identityHashCode(p));
                                }
                            }
                        }
                        // instance.node2ScanRanges is shared during incremental deployment.
                        requests.add(execState.getRequestToDeploy().deepCopy());
                    }
                }
                Assert.assertTrue(scanRangeNumber <= maxScanRangeNumber);
            }
        };
        final DefaultCoordinator coordinator = startScheduling(sql);
        Assert.assertTrue(coordinator.getJobSpec().isIncrementalScanRanges());
        Map<TUniqueId, List<TScanRangeParams>> workload = new HashMap<>();
        for (TExecPlanFragmentParams r : requests) {
            TPlanFragmentExecParams params = r.params;
            TUniqueId fragmentInstanceId = params.getFragment_instance_id();
            if (params.getPer_node_scan_ranges().isEmpty()) {
                continue;
            }
            List<TScanRangeParams> scanRanges = workload.computeIfAbsent(fragmentInstanceId, key -> new
                    ArrayList<>());
            for (List<TScanRangeParams> v : params.getPer_node_scan_ranges().values()) {
                scanRanges.addAll(v);
            }
        }
        // 3 nodes, each node has 1 instance.
        Assert.assertEquals(workload.size(), 3);
        Map<String, List<THdfsScanRange>> fileRangesMap = new HashMap<>();
        for (Map.Entry<TUniqueId, List<TScanRangeParams>> kv : workload.entrySet()) {
            System.out.println("----- checking fragment: " + kv.getKey() + "-----");
            List<TScanRangeParams> v = kv.getValue();
            for (int index = 0; index < v.size(); index++) {
                TScanRangeParams p = v.get(index);
                System.out.println(p + ", " + System.identityHashCode(p));
                if (p.isEmpty()) {
                    if (!p.has_more) {
                        Assert.assertTrue((index + 1) == v.size());
                    }
                } else {
                    THdfsScanRange sc = p.scan_range.hdfs_scan_range;
                    String file = sc.relative_path;
                    List<THdfsScanRange> ranges = fileRangesMap.computeIfAbsent(file, x -> new ArrayList<>());
                    ranges.add(sc);
                }
            }
        }

        for (Map.Entry<String, List<THdfsScanRange>> kv : fileRangesMap.entrySet()) {
            System.out.println("----- checking file: " + kv.getKey() + "-----");
            List<THdfsScanRange> fileRangess = kv.getValue();
            fileRangess.sort(new Comparator<THdfsScanRange>() {
                @Override
                public int compare(THdfsScanRange o1, THdfsScanRange o2) {
                    return (int) (o1.offset - o2.offset);
                }
            });
            for (int i = 0; i < fileRangess.size(); i++) {
                THdfsScanRange f = fileRangess.get(i);
                if (i == 0) {
                    Assert.assertEquals(f.offset, 0);
                } else if ((i + 1) == fileRangess.size()) {
                    Assert.assertEquals(f.offset + f.length, f.file_length);
                } else {
                    THdfsScanRange nf = fileRangess.get(i + 1);
                    Assert.assertEquals((f.offset + f.length), nf.offset);
                }
            }
        }
    }
}
