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

import com.starrocks.common.util.Counter;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.TCounterAggregateType;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TRuntimeProfileTree;
import com.starrocks.thrift.TUnit;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

public class QueryRuntimeProfileTest {

    private ConnectContext connectContext;

    @Mocked
    private JobSpec jobSpec;

    @Before
    public void setup() {
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));

        new Expectations() {
            {
                jobSpec.getQueryId();
                result = connectContext.getExecutionId();
                minTimes = 0;

                jobSpec.isEnablePipeline();
                result = true;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testMergeLoadChannelProfile() {
        new Expectations() {
            {
                jobSpec.hasOlapTableSink();
                result = true;
                minTimes = 0;
            }
        };

        QueryRuntimeProfile profile = new QueryRuntimeProfile(connectContext, jobSpec, false);
        profile.initFragmentProfiles(1);
        TReportExecStatusParams reportExecStatusParams = buildReportStatus();
        profile.updateLoadChannelProfile(reportExecStatusParams);
        Optional<RuntimeProfile> optional = profile.mergeLoadChannelProfile();
        Assert.assertTrue(optional.isPresent());
        verifyMergedLoadChannelProfile(optional.get());
    }

    @Test
    public void testBuildNonPipelineQueryProfile() {
        new Expectations() {
            {
                jobSpec.hasOlapTableSink();
                result = true;
                minTimes = 0;
                jobSpec.isEnablePipeline();
                result = false;
                minTimes = 0;
            }
        };

        QueryRuntimeProfile profile = new QueryRuntimeProfile(connectContext, jobSpec, false);
        profile.initFragmentProfiles(1);
        TReportExecStatusParams reportExecStatusParams = buildReportStatus();
        profile.updateLoadChannelProfile(reportExecStatusParams);
        RuntimeProfile runtimeProfile = profile.buildQueryProfile(true);
        Assert.assertNotNull(runtimeProfile);
        Assert.assertEquals(2, runtimeProfile.getChildMap().size());
        Assert.assertSame(profile.getFragmentProfiles().get(0), runtimeProfile.getChild("Fragment 0"));
        RuntimeProfile loadChannelProfile = runtimeProfile.getChild("LoadChannel");
        Assert.assertNotNull(loadChannelProfile);
        verifyMergedLoadChannelProfile(loadChannelProfile);
    }

    private void verifyMergedLoadChannelProfile(RuntimeProfile mergedProfile) {
        Assert.assertEquals("LoadChannel", mergedProfile.getName());
        Assert.assertEquals("288fb1df-f955-472f-a377-cb1e10e4d993", mergedProfile.getInfoString("LoadId"));
        Assert.assertEquals("40", mergedProfile.getInfoString("TxnId"));
        Assert.assertEquals("127.0.0.1,127.0.0.2", mergedProfile.getInfoString("BackendAddresses"));
        Assert.assertEquals(2, mergedProfile.getCounter("ChannelNum").getValue());
        Assert.assertEquals(537395200, mergedProfile.getCounter("PeakMemoryUsage").getValue());
        Assert.assertEquals(1073741824, mergedProfile.getCounter("__MAX_OF_PeakMemoryUsage").getValue());
        Assert.assertEquals(1048576, mergedProfile.getCounter("__MIN_OF_PeakMemoryUsage").getValue());
        Assert.assertEquals(2, mergedProfile.getChildMap().size());

        RuntimeProfile indexProfile1 = mergedProfile.getChild("Index (id=10176)");
        Assert.assertEquals(162, indexProfile1.getCounter("AddChunkCount").getValue());
        Assert.assertEquals(82, indexProfile1.getCounter("__MAX_OF_AddChunkCount").getValue());
        Assert.assertEquals(80, indexProfile1.getCounter("__MIN_OF_AddChunkCount").getValue());
        Assert.assertEquals(15000000000L, indexProfile1.getCounter("AddChunkTime").getValue());
        Assert.assertEquals(20000000000L, indexProfile1.getCounter("__MAX_OF_AddChunkTime").getValue());
        Assert.assertEquals(10000000000L, indexProfile1.getCounter("__MIN_OF_AddChunkTime").getValue());

        RuntimeProfile indexProfile2 = mergedProfile.getChild("Index (id=10298)");
        Assert.assertEquals(162, indexProfile2.getCounter("AddChunkCount").getValue());
        Assert.assertEquals(82, indexProfile2.getCounter("__MAX_OF_AddChunkCount").getValue());
        Assert.assertEquals(80, indexProfile2.getCounter("__MIN_OF_AddChunkCount").getValue());
        Assert.assertEquals(1500000000L, indexProfile2.getCounter("AddChunkTime").getValue());
        Assert.assertEquals(2000000000L, indexProfile2.getCounter("__MAX_OF_AddChunkTime").getValue());
        Assert.assertEquals(1000000000L, indexProfile2.getCounter("__MIN_OF_AddChunkTime").getValue());
    }

    private TReportExecStatusParams buildReportStatus() {
        RuntimeProfile profile = new RuntimeProfile("LoadChannel");
        profile.addInfoString("LoadId", "288fb1df-f955-472f-a377-cb1e10e4d993");
        profile.addInfoString("TxnId", "40");

        RuntimeProfile channelProfile1 = new RuntimeProfile("Channel (host=127.0.0.1)");
        profile.addChild(channelProfile1);
        Counter peakMemoryCounter1 = channelProfile1.addCounter("PeakMemoryUsage", TUnit.BYTES,
                Counter.createStrategy(TCounterAggregateType.AVG));
        peakMemoryCounter1.setValue(1024 * 1024 * 1024);

        RuntimeProfile indexProfile1 = new RuntimeProfile("Index (id=10176)");
        channelProfile1.addChild(indexProfile1);
        Counter addChunkCounter1 = indexProfile1.addCounter("AddChunkCount", TUnit.UNIT,
                Counter.createStrategy(TUnit.UNIT));
        addChunkCounter1.setValue(82);
        Counter addChunkTime1 = indexProfile1.addCounter("AddChunkTime", TUnit.TIME_NS,
                Counter.createStrategy(TCounterAggregateType.AVG));
        addChunkTime1.setValue(20000000000L);

        RuntimeProfile indexProfile2 = new RuntimeProfile("Index (id=10298)");
        channelProfile1.addChild(indexProfile2);
        Counter addChunkCounter2 = indexProfile2.addCounter("AddChunkCount", TUnit.UNIT,
                Counter.createStrategy(TUnit.UNIT));
        addChunkCounter2.setValue(82);
        Counter addChunkTime2 = indexProfile2.addCounter("AddChunkTime", TUnit.TIME_NS,
                Counter.createStrategy(TCounterAggregateType.AVG));
        addChunkTime2.setValue(1000000000L);

        RuntimeProfile channelProfile2 = new RuntimeProfile("Channel (host=127.0.0.2)");
        profile.addChild(channelProfile2);
        Counter peakMemoryCounter2 = channelProfile2.addCounter("PeakMemoryUsage", TUnit.BYTES,
                Counter.createStrategy(TCounterAggregateType.AVG));
        peakMemoryCounter2.setValue(1024 * 1024);

        RuntimeProfile indexProfile3 = new RuntimeProfile("Index (id=10176)");
        channelProfile2.addChild(indexProfile3);
        Counter addChunkCounter3 = indexProfile3.addCounter("AddChunkCount", TUnit.UNIT,
                Counter.createStrategy(TUnit.UNIT));
        addChunkCounter3.setValue(80);
        Counter addChunkTime3 = indexProfile3.addCounter("AddChunkTime", TUnit.TIME_NS,
                Counter.createStrategy(TCounterAggregateType.AVG));
        addChunkTime3.setValue(10000000000L);

        RuntimeProfile indexProfile4 = new RuntimeProfile("Index (id=10298)");
        channelProfile2.addChild(indexProfile4);
        Counter addChunkCounter4 = indexProfile4.addCounter("AddChunkCount", TUnit.UNIT,
                Counter.createStrategy(TUnit.UNIT));
        addChunkCounter4.setValue(80);
        Counter addChunkTime4 = indexProfile4.addCounter("AddChunkTime", TUnit.TIME_NS,
                Counter.createStrategy(TCounterAggregateType.AVG));
        addChunkTime4.setValue(2000000000L);

        TReportExecStatusParams params = new TReportExecStatusParams(FrontendServiceVersion.V1);
        TRuntimeProfileTree profileTree = profile.toThrift();
        params.setLoad_channel_profile(profileTree);

        return params;
    }
}
