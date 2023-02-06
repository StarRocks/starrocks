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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/EtlStatus.java

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

package com.starrocks.load;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.load.loadv2.dpp.DppResult;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TEtlState;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.collections.map.HashedMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

public class EtlStatus implements Writable {
    public static final String DEFAULT_TRACKING_URL = FeConstants.NULL_STRING;

    private TEtlState state;
    private String trackingUrl;

    /**
     * This field is useless in RUNTIME
     * It only used for persisting LoadStatistic in consideration of compatibility
     * It has only one k-v pair:
     *   the key is LOAD_STATISTIC; the value is json string of loadStatistic
     */
    private Map<String, String> stats = new HashedMap();
    private LoadStatistic loadStatistic = new LoadStatistic();
    private static final String LOAD_STATISTIC = "STARROCKS_LOAD_STATISTIC";

    private Map<String, String> counters;
    private Map<Long, Map<String, Long>> tableCounters;
    // not persist
    private Map<String, Long> fileMap;

    // for spark not persist
    // 0 - 100
    private int progress;
    private String failMsg;
    private DppResult dppResult;

    public EtlStatus() {
        this.state = TEtlState.RUNNING;
        this.trackingUrl = DEFAULT_TRACKING_URL;
        this.counters = Maps.newHashMap();
        this.tableCounters = Maps.newHashMap();
        this.fileMap = Maps.newHashMap();
        this.progress = 0;
        this.failMsg = "";
        this.dppResult = null;
    }

    public TEtlState getState() {
        return state;
    }

    public boolean setState(TEtlState state) {
        // running -> finished or cancelled
        if (this.state != TEtlState.RUNNING) {
            return false;
        }
        this.state = state;
        return true;
    }

    public LoadStatistic getLoadStatistic() {
        return loadStatistic;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = Strings.nullToEmpty(trackingUrl);
    }

    public Map<String, String> getCounters() {
        return counters;
    }

    public void replaceCounter(String key, String value) {
        counters.put(key, value);
    }

    public void setCounters(Map<String, String> counters) {
        this.counters = counters;
    }

    public Map<String, Long> getFileMap() {
        return fileMap;
    }

    public void setFileMap(Map<String, Long> fileMap) {
        this.fileMap = fileMap;
    }

    public void addAllFileMap(Map<String, Long> fileMap) {
        this.fileMap.putAll(fileMap);
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public String getFailMsg() {
        return failMsg;
    }

    public void setFailMsg(String failMsg) {
        this.failMsg = failMsg;
    }

    public DppResult getDppResult() {
        return dppResult;
    }

    public void setDppResult(DppResult dppResult) {
        this.dppResult = dppResult;
    }

    public synchronized void increaseTableCounter(long tableId, String key, long value) {
        Map<String, Long> cts = tableCounters.computeIfAbsent(tableId, tid -> Maps.newHashMap());
        Long originVal = cts.computeIfAbsent(key, k -> 0L);
        cts.put(key, originVal + value);
    }

    public synchronized void travelTableCounters(Consumer<Map.Entry<Long, Map<String, Long>>> cb) {
        for (Map.Entry<Long, Map<String, Long>> entry : tableCounters.entrySet()) {
            cb.accept(entry);
        }
    }

    public void reset() {
        this.stats.clear();
        this.counters.clear();
        this.tableCounters.clear();
        this.fileMap.clear();
        this.progress = 0;
        this.failMsg = "";
        this.dppResult = null;
    }

    @Override
    public String toString() {
        return "EtlStatus{" +
                "state=" + state +
                ", trackingUrl='" + trackingUrl + '\'' +
                ", stats=" + stats +
                ", counters=" + counters +
                ", tableCounters=" + tableCounters +
                ", fileMap=" + fileMap +
                ", progress=" + progress +
                ", failMsg='" + failMsg + '\'' +
                ", dppResult='" + dppResult + '\'' +
                '}';
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, state.name());
        Text.writeString(out, trackingUrl);

        // persist load statics in stat counter
        stats.put(LOAD_STATISTIC, loadStatistic.toJson());
        int statsCount = stats.size();
        out.writeInt(statsCount);
        for (Map.Entry<String, String> entry : stats.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }

        int countersCount = (counters == null) ? 0 : counters.size();
        out.writeInt(countersCount);
        if (counters != null) {
            for (Map.Entry<String, String> entry : counters.entrySet()) {
                Text.writeString(out, entry.getKey());
                Text.writeString(out, entry.getValue());
            }
        }
        // TODO: Persist `tableCounters`
        // Text.writeString(out, GsonUtils.GSON.toJson(tableCounters));
    }

    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        state = TEtlState.valueOf(Text.readString(in));
        trackingUrl = Text.readString(in);

        int statsCount = in.readInt();
        for (int i = 0; i < statsCount; ++i) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            stats.put(key, value);
        }
        // restore load statics from stat counter
        if (stats.containsKey(LOAD_STATISTIC)) {
            loadStatistic = LoadStatistic.fromJson(stats.get(LOAD_STATISTIC));
        }

        int countersCount = in.readInt();
        for (int i = 0; i < countersCount; ++i) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            counters.put(key, value);
        }
        // TODO: Persist `tableCounters`
        // if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_93) {
        //     tableCounters = GsonUtils.GSON.fromJson(Text.readString(in), tableCounters.getClass());
        // }
    }

    public void setLoadFileInfo(int filenum, long filesize) {
        this.loadStatistic.fileNum = filenum;
        this.loadStatistic.totalFileSizeB = filesize;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(trackingUrl);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof EtlStatus)) {
            return false;
        }

        EtlStatus etlTaskStatus = (EtlStatus) obj;

        // Check stats
        if (etlTaskStatus.stats == null) {
            return false;
        }
        if (stats.size() != etlTaskStatus.stats.size()) {
            return false;
        }
        for (Entry<String, String> entry : stats.entrySet()) {
            String key = entry.getKey();
            if (!etlTaskStatus.stats.containsKey(key)) {
                return false;
            }
            if (!entry.getValue().equals(etlTaskStatus.stats.get(key))) {
                return false;
            }
        }

        // Check counters
        if (etlTaskStatus.counters == null) {
            return false;
        }
        if (counters.size() != etlTaskStatus.counters.size()) {
            return false;
        }
        for (Entry<String, String> entry : counters.entrySet()) {
            String key = entry.getKey();
            if (!etlTaskStatus.counters.containsKey(key)) {
                return false;
            }
            if (!entry.getValue().equals(etlTaskStatus.counters.get(key))) {
                return false;
            }
        }

        return state.equals(etlTaskStatus.state) && trackingUrl.equals(etlTaskStatus.trackingUrl);
    }

    public static class LoadStatistic {
        @SerializedName("counterTbl")
        // number of rows processed on BE, this number will be updated periodically by query report.
        // A load job may has several load tasks(queries), and each task has several fragments.
        // each fragment will report independently.
        // load task id -> fragment id -> rows count
        private Table<String, String, Long> counterTbl = HashBasedTable.create();

        // load task id -> unfinished backend id list
        @SerializedName("unfinishedBackendIds")
        private Map<String, List<Long>> unfinishedBackendIds = Maps.newHashMap();
        // load task id -> all backend id list
        @SerializedName("allBackendIds")
        private Map<String, List<Long>> allBackendIds = Maps.newHashMap();

        // number of file to be loaded
        @SerializedName("fileNum")
        public int fileNum = 0;
        @SerializedName("totalFileSizeB")
        public long totalFileSizeB = 0;

        @SerializedName("sinkBytesCounterTbl")
        private Table<String, String, Long> sinkBytesCounterTbl = HashBasedTable.create();

        @SerializedName("sourceRowsCounterTbl")
        private Table<String, String, Long> sourceRowsCounterTbl = HashBasedTable.create();

        @SerializedName("sourceBytesCounterTbl")
        private Table<String, String, Long> sourceBytesCounterTbl = HashBasedTable.create();

        @SerializedName("loadFinish")
        private boolean loadFinish = false;

        // init the statistic of specified load task
        public synchronized void initLoad(TUniqueId loadId, Set<TUniqueId> fragmentIds, List<Long> relatedBackendIds) {
            String loadStr = DebugUtil.printId(loadId);
            counterTbl.rowMap().remove(loadStr);
            sinkBytesCounterTbl.rowMap().remove(loadStr);
            sourceRowsCounterTbl.rowMap().remove(loadStr);
            sourceBytesCounterTbl.rowMap().remove(loadStr);

            for (TUniqueId fragId : fragmentIds) {
                counterTbl.put(loadStr, DebugUtil.printId(fragId), 0L);
                sinkBytesCounterTbl.put(loadStr, DebugUtil.printId(fragId), 0L);
                sourceRowsCounterTbl.put(loadStr, DebugUtil.printId(fragId), 0L);
                sourceBytesCounterTbl.put(loadStr, DebugUtil.printId(fragId), 0L);
            }
            
            allBackendIds.put(loadStr, relatedBackendIds);
            // need to get a copy of relatedBackendIds, so that when we modify the "relatedBackendIds" in
            // allBackendIds, the list in unfinishedBackendIds will not be changed.
            unfinishedBackendIds.put(loadStr, Lists.newArrayList(relatedBackendIds));
        }

        public synchronized void removeLoad(TUniqueId loadId) {
            String loadStr = DebugUtil.printId(loadId);
            counterTbl.rowMap().remove(loadStr);
            sinkBytesCounterTbl.rowMap().remove(loadStr);
            sourceRowsCounterTbl.rowMap().remove(loadStr);
            sourceBytesCounterTbl.rowMap().remove(loadStr);
            
            unfinishedBackendIds.remove(loadStr);
            allBackendIds.remove(loadStr);
        }

        public synchronized long totalFileSize() {
            return totalFileSizeB;
        }

        public synchronized long totalSourceLoadBytes() {
            long totalSourceBytes = 0;
            if (loadFinish) {
                totalSourceBytes = totalFileSizeB;
            } else {
                for (long bytes : sourceBytesCounterTbl.values()) {
                    totalSourceBytes += bytes;
                }
            }
            return totalSourceBytes;
        }

        public synchronized long totalSourceLoadRows() {
            long totalSourceRows = 0;
            for (long rows : sourceRowsCounterTbl.values()) {
                totalSourceRows += rows;
            }
            return totalSourceRows;
        }

        public synchronized long totalSinkLoadRows() {
            long totalSourceRows = 0;
            for (long rows : counterTbl.values()) {
                totalSourceRows += rows;
            }
            return totalSourceRows;
        }

        public synchronized long totalRows() {
            long totalRows = 0;
            for (long row : counterTbl.values()) {
                totalRows += row;
            }
            return totalRows;
        }

        public synchronized void setLoadFinish() {
            loadFinish = true;
        }

        public synchronized boolean getLoadFinish() {
            return loadFinish;
        }

        public synchronized void updateLoadProgress(long backendId, TUniqueId loadId, TUniqueId fragmentId,
                                                    long sinkRows, long sinkBytes, long sourceRows, 
                                                    long sourceBytes, boolean isDone) {
            String loadStr = DebugUtil.printId(loadId);
            String fragmentStr = DebugUtil.printId(fragmentId);
            if (counterTbl.contains(loadStr, fragmentStr)) {
                counterTbl.put(loadStr, fragmentStr, sinkRows);
                sinkBytesCounterTbl.put(loadStr, fragmentStr, sinkBytes);
                sourceRowsCounterTbl.put(loadStr, fragmentStr, sourceRows);
                sourceBytesCounterTbl.put(loadStr, fragmentStr, sourceBytes);
            }

            if (isDone && unfinishedBackendIds.containsKey(loadStr)) {
                unfinishedBackendIds.get(loadStr).remove(backendId);
            }
        }

        public synchronized void updateLoadProgress(long backendId, TUniqueId loadId, TUniqueId fragmentId,
                                                    long rows, boolean isDone) {
            String loadStr = DebugUtil.printId(loadId);
            String fragmentStr = DebugUtil.printId(fragmentId);
            if (counterTbl.contains(loadStr, fragmentStr)) {
                counterTbl.put(loadStr, fragmentStr, rows);
                sinkBytesCounterTbl.put(loadStr, fragmentStr, 0L);
                sourceRowsCounterTbl.put(loadStr, fragmentStr, 0L);
                sourceBytesCounterTbl.put(loadStr, fragmentStr, 0L);
            }

            if (isDone && unfinishedBackendIds.containsKey(loadStr)) {
                unfinishedBackendIds.get(loadStr).remove(backendId);
            }
        }

        // used for `show load`
        public synchronized String toShowInfoStr() {
            long totalSinkRows = 0;
            for (long rows : counterTbl.values()) {
                totalSinkRows += rows;
            }

            long totalSourceRows = 0;
            for (long rows : sourceRowsCounterTbl.values()) {
                totalSourceRows += rows;
            }

            long totalSinkBytes = 0;
            for (long bytes : sinkBytesCounterTbl.values()) {
                totalSinkBytes += bytes;
            }

            long totalSourceBytes = 0;
            if (loadFinish) {
                totalSourceBytes = totalFileSizeB;
            } else {
                for (long bytes : sourceBytesCounterTbl.values()) {
                    totalSourceBytes += bytes;
                }
            }

            TreeMap<String, Object> details = Maps.newTreeMap();
            details.put("ScanRows", totalSourceRows);
            details.put("ScanBytes", totalSourceBytes);
            details.put("InternalTableLoadRows", totalSinkRows);
            details.put("InternalTableLoadBytes", totalSinkBytes);
            details.put("FileNumber", fileNum);
            details.put("FileSize", totalFileSizeB);
            details.put("TaskNumber", counterTbl.rowMap().size());
            details.put("Unfinished backends", unfinishedBackendIds);
            details.put("All backends", allBackendIds);
            Gson gson = new Gson();
            return gson.toJson(details);
        }

        public String toJson() throws IOException {
            return GsonUtils.GSON.toJson(this);
        }

        public static LoadStatistic fromJson(String json) {
            LoadStatistic loadStatistic = GsonUtils.GSON.fromJson(json, LoadStatistic.class);
            if (!json.contains("sinkBytesCounterTbl")) {
                loadStatistic.sinkBytesCounterTbl = HashBasedTable.create();
            }
            if (!json.contains("sourceRowsCounterTbl")) {
                loadStatistic.sourceRowsCounterTbl = HashBasedTable.create();
            }
            if (!json.contains("sourceBytesCounterTbl")) {
                loadStatistic.sourceBytesCounterTbl = HashBasedTable.create();
            }
            if (!json.contains("loadFinish")) {
                loadStatistic.loadFinish = false;
            }
            return loadStatistic;
        }

    }

}
