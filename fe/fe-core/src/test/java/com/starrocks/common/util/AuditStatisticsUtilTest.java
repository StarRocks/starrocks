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

package com.starrocks.common.util;

import com.starrocks.proto.PQueryStatistics;
import com.starrocks.thrift.TAuditStatistics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AuditStatisticsUtilTest {
    @Test
    public void test() {
        TAuditStatistics ts = new TAuditStatistics();
        ts.setScan_rows(1);
        ts.setScan_bytes(2);
        ts.setReturned_rows(3);
        ts.setCpu_cost_ns(4);
        ts.setMem_cost_bytes(5);
        ts.setSpill_bytes(6);
        ts.setTransmitted_bytes(7);
        ts.setRead_local_cnt(8);
        ts.setRead_remote_cnt(9);
        PQueryStatistics ps = AuditStatisticsUtil.toProtobuf(ts);
        Assertions.assertEquals(ps.scanRows, 1);
        Assertions.assertEquals(ps.scanBytes, 2);
        Assertions.assertEquals(ps.returnedRows, 3);
        Assertions.assertEquals(ps.cpuCostNs, 4);
        Assertions.assertEquals(ps.memCostBytes, 5);
        Assertions.assertEquals(ps.spillBytes, 6);
        Assertions.assertEquals(ps.transmittedBytes, 7);
        Assertions.assertEquals(ps.readLocalCnt, 8);
        Assertions.assertEquals(ps.readRemoteCnt, 9);

        PQueryStatistics ps2 = new PQueryStatistics();
        AuditStatisticsUtil.mergeProtobuf(ps, ps2);
        Assertions.assertEquals(ps2.scanRows, 1);
        Assertions.assertEquals(ps2.scanBytes, 2);
        Assertions.assertEquals(ps2.returnedRows, 3);
        Assertions.assertEquals(ps2.cpuCostNs, 4);
        Assertions.assertEquals(ps2.memCostBytes, 5);
        Assertions.assertEquals(ps2.spillBytes, 6);
        Assertions.assertEquals(ps2.transmittedBytes, 7);
        Assertions.assertEquals(ps2.readLocalCnt, 8);
        Assertions.assertEquals(ps2.readRemoteCnt, 9);

        TAuditStatistics ts2 = AuditStatisticsUtil.toThrift(ps);
        Assertions.assertEquals(ts2.getScan_rows(), 1);
        Assertions.assertEquals(ts2.getScan_bytes(), 2);
        Assertions.assertEquals(ts2.getReturned_rows(), 3);
        Assertions.assertEquals(ts2.getCpu_cost_ns(), 4);
        Assertions.assertEquals(ts2.getMem_cost_bytes(), 5);
        Assertions.assertEquals(ts2.getSpill_bytes(), 6);
        Assertions.assertEquals(ts2.getTransmitted_bytes(), 7);
        Assertions.assertEquals(ts2.getRead_local_cnt(), 8);
        Assertions.assertEquals(ts2.getRead_remote_cnt(), 9);
    }
}
