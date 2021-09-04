// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class AnalyzeManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(AnalyzeManager.class);

    private final Map<Long, AnalyzeJob> analyzeJobMap;

    public AnalyzeManager() {
        analyzeJobMap = Maps.newConcurrentMap();
    }

    public void addAnalyzeJob(AnalyzeJob job) {
        long id = Catalog.getCurrentCatalog().getNextId();
        job.setId(id);
        analyzeJobMap.put(id, job);
        Catalog.getCurrentCatalog().getEditLog().logAddAnalyzeJob(job);
    }

    public void updateAnalyzeJobWithoutLog(AnalyzeJob job) {
        analyzeJobMap.put(job.getId(), job);
    }

    public void updateAnalyzeJobWithLog(AnalyzeJob job) {
        analyzeJobMap.put(job.getId(), job);
        Catalog.getCurrentCatalog().getEditLog().logAddAnalyzeJob(job);
    }

    public void removeAnalyzeJob(long id) {
        if (analyzeJobMap.containsKey(id)) {
            Catalog.getCurrentCatalog().getEditLog().logRemoveAnalyzeJob(analyzeJobMap.remove(id));
        }
    }

    public List<AnalyzeJob> getAllAnalyzeJobList() {
        return Lists.newLinkedList(analyzeJobMap.values());
    }

    // expire finish job
    public void expireAnalyzeJob() {
        List<AnalyzeJob> expireList = Lists.newArrayList();

        for (AnalyzeJob job : analyzeJobMap.values()) {
            if (AnalyzeJob.DEFAULT_ALL_ID != job.getDbId()) {
                // check db/table
                Database db = Catalog.getCurrentCatalog().getDb(job.getDbId());
                if (null == db) {
                    expireList.add(job);
                    continue;
                }

                if (AnalyzeJob.DEFAULT_ALL_ID != job.getTableId()) {
                    if (null == db.getTable(job.getTableId())) {
                        expireList.add(job);
                        continue;
                    }
                }
            }

            if (Constants.ScheduleStatus.FINISH != job.getStatus()) {
                continue;
            }

            // Job will keep two collection cycle
            LocalDateTime expireTimePoint = LocalDateTime.now().minusSeconds(job.getExpireSec());
            if (job.getWorkTime().isBefore(expireTimePoint)) {
                expireList.add(job);
            }
        }

        expireList.forEach(d -> analyzeJobMap.remove(d.getId()));
        for (AnalyzeJob job : expireList) {
            Catalog.getCurrentCatalog().getEditLog().logRemoveAnalyzeJob(job);
        }
    }

    public void replayAddAnalyzeJob(AnalyzeJob job) {
        analyzeJobMap.put(job.getId(), job);
    }

    public void replayRemoveAnalyzeJob(AnalyzeJob job) {
        analyzeJobMap.remove(job.getId());
    }

    public void readFields(DataInputStream dis) throws IOException {
        // read job
        String s = Text.readString(dis);
        SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);

        if (null != data && null != data.jobs) {
            for (AnalyzeJob job : data.jobs) {
                replayAddAnalyzeJob(job);
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // save history
        List<AnalyzeJob> historyList = getAllAnalyzeJobList();
        SerializeData data = new SerializeData();
        data.jobs = historyList;

        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(out, s);
    }

    private static class SerializeData {
        @SerializedName("analyzeJobs")
        public List<AnalyzeJob> jobs;
    }
}
