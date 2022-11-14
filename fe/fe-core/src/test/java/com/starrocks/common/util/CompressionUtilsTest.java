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

package com.starrocks.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class CompressionUtilsTest {
    @Test
    public void testStringCompression() throws IOException {
        // This is part of a typical profile string from daily test,
        // its length is about 10KB, the length of the full string is 500+KB.
        String origStr = "[\"Query:\n" +
                "  Summary:\n" +
                "     - Query ID: 74009722-362c-11ed-88aa-00163e0e2cf9\n" +
                "     - Start Time: 2022-09-17 10:00:07\n" +
                "     - End Time: 2022-09-17 10:00:07\n" +
                "     - Total: 35ms\n" +
                "     - Query Type: Query\n" +
                "     - Query State: EOF\n" +
                "     - StarRocks Version: MAIN-RELEASE-bd208e2\n" +
                "     - User: root\n" +
                "     - Default Db: persistent_idx_sqlset_5eed36ae_362c_11ed_a45c_00163e0e489a\n" +
                "     - Sql Statement:  select sum(l_extendedprice* (1 - l_discount)) as revenue from lineitem, " +
                "part where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in" +
                " ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and" +
                " p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct =" +
                " 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container " +
                "in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10" +
                " and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct =" +
                " 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container" +
                " in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20" +
                " + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG')" +
                " and l_shipinstruct = 'DELIVER IN PERSON' );\n" +
                "     - QueryCpuCost: 37ms\n" +
                "     - QueryMemCost: 26.767MB\n" +
                "     - Variables: parallel_fragment_exec_instance_num=8,pipeline_dop=8\n" +
                "     - Collect Profile Time: 11ms\n" +
                "  Planner:\n" +
                "     - Total: 3ms / 1\n" +
                "  Execution Profile 74009722-362c-11ed-88aa-00163e0e2cf9:(Active: " +
                "32.941ms[32941234ns], % non-child: 100.00%)\n" +
                "    Fragment 0:\n" +
                "      Instance 74009722-362c-11ed-88aa-00163e0e2d00 " +
                "(host=TNetworkAddress(hostname:172.26.92.152, port:9060)):\n" +
                "         - PeakMemoryUsage: 122.62 KB\n" +
                "         - QueryMemoryLimit: 40.10 GB\n" +
                "        Pipeline (id=2):\n" +
                "           - DegreeOfParallelism: 1\n" +
                "           - TotalDegreeOfParallelism: 1\n" +
                "          PipelineDriver (id=9):\n" +
                "             - ActiveTime: 66.613us\n" +
                "             - BlockByInputEmpty: 0\n" +
                "             - BlockByOutputFull: 0\n" +
                "             - BlockByPrecondition: 0\n" +
                "             - DriverTotalTime: 27.505ms\n" +
                "             - OverheadTime: 66.613us\n" +
                "             - PendingTime: 27.420ms\n" +
                "               - InputEmptyTime: 27.419ms\n" +
                "                 - FirstInputEmptyTime: 27.419ms\n" +
                "                 - FollowupInputEmptyTime: 0ns\n" +
                "               - OutputFullTime: 0ns\n" +
                "               - PendingFinishTime: 0ns\n" +
                "               - PreconditionBlockTime: 0ns\n" +
                "             - ScheduleCount: 1\n" +
                "             - ScheduleTime: 18.618us\n" +
                "             - YieldByTimeLimit: 0\n" +
                "            RESULT_SINK:\n" +
                "              CommonMetrics:\n" +
                "                 - CloseTime: 2.834us\n" +
                "                 - OperatorTotalTime: 57.209us\n" +
                "                 - PeakMemoryUsage: 0.00 \n" +
                "                 - PrepareTime: 9.77us\n" +
                "                 - PullChunkNum: 0\n" +
                "                 - PullRowNum: 0\n" +
                "                 - PullTotalTime: 0ns\n" +
                "                 - PushChunkNum: 1\n" +
                "                 - PushRowNum: 1\n" +
                "                 - PushTotalTime: 54.269us\n" +
                "                 - SetFinishedTime: 47ns\n" +
                "                 - SetFinishingTime: 59ns\n" +
                "              UniqueMetrics:\n" +
                "            AGGREGATE_BLOCKING_SOURCE (plan_node_id=8):\n" +
                "              CommonMetrics:\n" +
                "                 - CloseTime: 2.598us\n" +
                "                 - JoinRuntimeFilterEvaluate: 0\n" +
                "                 - JoinRuntimeFilterHashTime: 0ns\n" +
                "                 - JoinRuntimeFilterInputRows: 0\n" +
                "                 - JoinRuntimeFilterOutputRows: 0\n" +
                "                 - JoinRuntimeFilterTime: 0ns\n" +
                "                 - OperatorTotalTime: 12.594us\n" +
                "                 - PeakMemoryUsage: 0.00 \n" +
                "                 - PrepareTime: 5.366us\n" +
                "                 - PullChunkNum: 1\n" +
                "                 - PullRowNum: 1\n" +
                "                 - PullTotalTime: 9.882us\n" +
                "                 - PushChunkNum: 0\n" +
                "                 - PushRowNum: 0\n" +
                "                 - PushTotalTime: 0ns\n" +
                "                 - RuntimeBloomFilterNum: 0\n" +
                "                 - RuntimeInFilterNum: 0\n" +
                "                 - SetFinishedTime: 50ns\n" +
                "                 - SetFinishingTime: 64ns\n" +
                "              UniqueMetrics:\n" +
                "        Pipeline (id=1):\n" +
                "           - DegreeOfParallelism: 1\n" +
                "           - TotalDegreeOfParallelism: 1\n" +
                "          PipelineDriver (id=8):\n" +
                "             - ActiveTime: 99.78us\n" +
                "             - BlockByInputEmpty: 2\n" +
                "             - BlockByOutputFull: 0\n" +
                "             - BlockByPrecondition: 0\n" +
                "             - DriverTotalTime: 27.485ms\n" +
                "             - OverheadTime: 99.78us\n" +
                "             - PendingTime: 27.283ms\n" +
                "               - InputEmptyTime: 27.287ms\n" +
                "                 - FirstInputEmptyTime: 26.792ms\n" +
                "                 - FollowupInputEmptyTime: 495.532us\n" +
                "               - OutputFullTime: 0ns\n" +
                "               - PendingFinishTime: 0ns\n" +
                "               - PreconditionBlockTime: 0ns\n" +
                "             - ScheduleCount: 3\n" +
                "             - ScheduleTime: 103.182us\n" +
                "             - YieldByTimeLimit: 0\n" +
                "            AGGREGATE_BLOCKING_SINK (plan_node_id=8):\n" +
                "              CommonMetrics:\n" +
                "                 - CloseTime: 3.9us\n" +
                "                 - OperatorTotalTime: 31.636us\n" +
                "                 - PeakMemoryUsage: 0.00 \n" +
                "                 - PrepareTime: 48.270us\n" +
                "                 - PullChunkNum: 0\n" +
                "                 - PullRowNum: 0\n" +
                "                 - PullTotalTime: 0ns\n" +
                "                 - PushChunkNum: 23\n" +
                "                 - PushRowNum: 23\n" +
                "                 - PushTotalTime: 28.277us\n" +
                "                 - RuntimeBloomFilterNum: 0\n" +
                "                 - RuntimeInFilterNum: 0\n" +
                "                 - SetFinishedTime: 30ns\n" +
                "                 - SetFinishingTime: 320ns\n" +
                "              UniqueMetrics:\n" +
                "                 - AggregateFunctions: sum(27: sum)\n" +
                "                 - AggComputeTime: 7.238us\n" +
                "                 - ExprComputeTime: 7.438us\n" +
                "                 - ExprReleaseTime: 8.591us\n" +
                "                 - GetResultsTime: 2.628us\n" +
                "                 - HashTableSize: 0\n" +
                "                 - InputRowCount: 23\n" +
                "                 - PassThroughRowCount: 0\n" +
                "                 - ResultAggAppendTime: 0ns\n" +
                "                 - ResultGroupByAppendTime: 0ns\n" +
                "                 - ResultIteratorTime: 0ns\n" +
                "                 - RowsReturned: 0\n" +
                "                 - StreamingTime: 0ns\n" +
                "            LOCAL_EXCHANGE_SOURCE (pseudo_plan_node_id=-100):\n" +
                "              CommonMetrics:\n" +
                "                 - CloseTime: 196ns\n" +
                "                 - OperatorTotalTime: 19.193us\n" +
                "                 - PeakMemoryUsage: 0.00 \n" +
                "                 - PrepareTime: 6.438us\n" +
                "                 - PullChunkNum: 23\n" +
                "                 - PullRowNum: 23\n" +
                "                 - PullTotalTime: 18.43us\n" +
                "                 - PushChunkNum: 0\n" +
                "                 - PushRowNum: 0\n" +
                "                 - PushTotalTime: 0ns\n" +
                "                 - SetFinishedTime: 852ns\n" +
                "                 - SetFinishingTime: 102ns\n" +
                "              UniqueMetrics:\n" +
                "        Pipeline (id=0):\n" +
                "           - DegreeOfParallelism: 8\n" +
                "           - TotalDegreeOfParallelism: 8\n" +
                "          PipelineDriver (id=0):\n" +
                "             - ActiveTime: 47.548us\n" +
                "             - BlockByInputEmpty: 1\n" +
                "             - BlockByOutputFull: 0\n" +
                "             - BlockByPrecondition: 0\n" +
                "             - DriverTotalTime: 27.641ms\n" +
                "             - OverheadTime: 47.548us\n" +
                "             - PendingTime: 27.283ms\n" +
                "               - InputEmptyTime: 27.284ms\n" +
                "                 - FirstInputEmptyTime: 26.778ms\n" +
                "                 - FollowupInputEmptyTime: 506.462us\n" +
                "               - OutputFullTime: 0ns\n" +
                "               - PendingFinishTime: 0ns\n" +
                "               - PreconditionBlockTime: 0ns\n" +
                "             - ScheduleCount: 2\n" +
                "             - ScheduleTime: 310.820us\n" +
                "             - YieldByTimeLimit: 0\n" +
                "            LOCAL_EXCHANGE_SINK (pseudo_plan_node_id=-100):\n" +
                "              CommonMetrics:\n" +
                "                 - CloseTime: 106ns\n" +
                "                 - OperatorTotalTime: 5.639us\n" +
                "                 - PeakMemoryUsage: 0.00 \n" +
                "                 - PrepareTime: 16.31us\n" +
                "                 - PullChunkNum: 0\n" +
                "                 - PullRowNum: 0\n" +
                "                 - PullTotalTime: 0ns\n" +
                "                 - PushChunkNum: 3\n" +
                "                 - PushRowNum: 3\n" +
                "                 - PushTotalTime: 5.344us\n" +
                "                 - SetFinishedTime: 40ns\n" +
                "                 - SetFinishingTime: 149ns\n" +
                "              UniqueMetrics:\n" +
                "                 - Type: Passthrough\n" +
                "            EXCHANGE_SOURCE (plan_node_id=7):\n" +
                "              CommonMetrics:\n" +
                "                 - CloseTime: 325ns\n" +
                "                 - JoinRuntimeFilterEvaluate: 0\n" +
                "                 - JoinRuntimeFilterHashTime: 0ns\n" +
                "                 - JoinRuntimeFilterInputRows: 0\n" +
                "                 - JoinRuntimeFilterOutputRows: 0\n" +
                "                 - JoinRuntimeFilterTime: 0ns\n" +
                "                 - OperatorTotalTime: 29.310us\n" +
                "                 - PeakMemoryUsage: 0.00 \n" +
                "                 - PrepareTime: 89.237us\n" +
                "                 - PullChunkNum: 3\n" +
                "                 - PullRowNum: 3\n" +
                "                 - PullTotalTime: 28.573us\n" +
                "                 - PushChunkNum: 0\n" +
                "                 - PushRowNum: 0\n" +
                "                 - PushTotalTime: 0ns\n" +
                "                 - RuntimeBloomFilterNum: 0\n" +
                "                 - RuntimeInFilterNum: 0\n" +
                "                 - SetFinishedTime: 61ns\n" +
                "                 - SetFinishingTime: 351ns\n" +
                "              UniqueMetrics:\n" +
                "                 - BufferUnplugCount: 1\n" +
                "                 - BytesPassThrough: 759.00 B\n" +
                "                 - BytesReceived: 0.00 \n" +
                "                 - ClosureBlockCount: 0\n" +
                "                 - ClosureBlockTime: 0ns\n" +
                "                 - DecompressChunkTime: 0ns\n" +
                "                 - DeserializeChunkTime: 55.448us\n" +
                "                 - ReceiverProcessTotalTime: 58.211us\n" +
                "                 - RequestReceived: 23\n" +
                "                 - SenderTotalTime: 55.396us\n" +
                "                 - SenderWaitLockTime: 3.915us\n" +
                "          PipelineDriver (id=1):\n" +
                "             - ActiveTime: 42.564us\n" +
                "             - BlockByInputEmpty: 2\n" +
                "             - BlockByOutputFull: 0\n" +
                "             - BlockByPrecondition: 0\n" +
                "             - DriverTotalTime: 27.638ms\n" +
                "             - OverheadTime: 42.564us\n" +
                "             - PendingTime: 27.286ms\n" +
                "               - InputEmptyTime: 27.288ms\n" +
                "                 - FirstInputEmptyTime: 26.773ms\n" +
                "                 - FollowupInputEmptyTime: 514.234us\n" +
                "               - OutputFullTime: 0ns\n" +
                "               - PendingFinishTime: 0ns\n" +
                "               - PreconditionBlockTime: 0ns\n" +
                "             - ScheduleCount: 3\n" +
                "             - ScheduleTime: 309.484us\n" +
                "             - YieldByTimeLimi...\"]";
        System.out.println("Length of the string before compression is: " + origStr.length());
        byte[] compressedStr = CompressionUtils.gzipCompressString(origStr);
        System.out.println("Length of the string after compression is: " + compressedStr.length);
        String decompressedStr = CompressionUtils.gzipDecompressString(compressedStr);
        System.out.println("Length of the string after decompression is: " + decompressedStr.length());
        Assert.assertEquals(origStr, decompressedStr);
    }
}
