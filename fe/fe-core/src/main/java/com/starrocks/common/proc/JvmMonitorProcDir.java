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

package com.starrocks.common.proc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.monitor.jvm.JvmInfo;
import com.starrocks.monitor.jvm.JvmStatCollector;
import com.starrocks.monitor.jvm.JvmStats;
import com.starrocks.monitor.jvm.JvmStats.BufferPool;
import com.starrocks.monitor.jvm.JvmStats.GarbageCollector;
import com.starrocks.monitor.jvm.JvmStats.MemoryPool;
import com.starrocks.monitor.jvm.JvmStats.Threads;

import java.util.List;

public class JvmMonitorProcDir implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("Value")
            .build();

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        JvmStatCollector jvmStatCollector = new JvmStatCollector();

        // 1. jvm info
        JvmInfo jvmInfo = jvmStatCollector.info();
        result.addRow(genRow("jvm start time", TimeUtils.longToTimeString(jvmInfo.getStartTime())));
        result.addRow(genRow("jvm version info", Joiner.on(" ").join(jvmInfo.getVersion(),
                jvmInfo.getVmName(),
                jvmInfo.getVmVendor(),
                jvmInfo.getVmVersion())));

        result.addRow(genRow("configured init heap size", jvmInfo.getConfiguredInitialHeapSize()));
        result.addRow(genRow("configured max heap size", jvmInfo.getConfiguredMaxHeapSize()));
        result.addRow(genRow("frontend pid", jvmInfo.getPid()));

        // 2. jvm stats
        JvmStats jvmStats = jvmStatCollector.stats();
        result.addRow(genRow("classes loaded", jvmStats.getClasses().getLoadedClassCount()));
        result.addRow(genRow("classes total loaded", jvmStats.getClasses().getTotalLoadedClassCount()));
        result.addRow(genRow("classes unloaded", jvmStats.getClasses().getUnloadedClassCount()));

        result.addRow(genRow("mem heap committed", jvmStats.getMem().getHeapCommitted()));
        result.addRow(genRow("mem heap used", jvmStats.getMem().getHeapUsed()));
        result.addRow(genRow("mem non heap committed", jvmStats.getMem().getNonHeapCommitted()));
        result.addRow(genRow("mem non heap used", jvmStats.getMem().getNonHeapUsed()));

        for (MemoryPool memPool : jvmStats.getMem()) {
            result.addRow(genRow("mem pool " + memPool.getName() + " committed", memPool.getCommitted()));
            result.addRow(genRow("mem pool " + memPool.getName() + " used", memPool.getUsed()));
            result.addRow(genRow("mem pool " + memPool.getName() + " max", memPool.getMax()));
            result.addRow(genRow("mem pool " + memPool.getName() + " peak used", memPool.getPeakUsed()));
            result.addRow(genRow("mem pool " + memPool.getName() + " peak max", memPool.getPeakMax()));
        }

        for (BufferPool bp : jvmStats.getBufferPools()) {
            result.addRow(genRow("buffer pool " + bp.getName() + " count", bp.getCount()));
            result.addRow(genRow("buffer pool " + bp.getName() + " used", bp.getUsed()));
            result.addRow(genRow("buffer pool " + bp.getName() + " capacity", bp.getTotalCapacity()));
        }

        for (GarbageCollector gc : jvmStats.getGc()) {
            result.addRow(genRow("gc " + gc.getName() + " collection count", gc.getCollectionCount()));
            result.addRow(genRow("gc " + gc.getName() + " collection time", gc.getCollectionTime().getMillis()));
        }

        Threads threads = jvmStats.getThreads();
        result.addRow(genRow("threads count", threads.getCount()));
        result.addRow(genRow("threads peak count", threads.getPeakCount()));

        return result;
    }

    private List<String> genRow(String key, Object value) {
        List<String> row = Lists.newArrayList();
        row.add(key);
        row.add(String.valueOf(value));
        return row;
    }
}
