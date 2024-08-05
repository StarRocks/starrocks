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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import com.starrocks.load.FailMsg;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SegmentLoadPendingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(SegmentLoadPendingTask.class);

    private static final String SPARK_OUTPUT_DATA_FILE_SUFFIX = "dat";
    private static final String SPARK_OUTPUT_PB_FILE_SUFFIX = "pb";
    private static final String SPARK_OUTPUT_SCHEMA_FILE_SUFFIX = "schema";
    private static final String SPARK_OUTPUT_TABLET_DATA_DIR = "/data";

    private final Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups;
    private final BrokerDesc brokerDesc;

    public SegmentLoadPendingTask(SegmentLoadJob loadTaskCallback,
                                  Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups,
                                  BrokerDesc brokerDesc) {
        super(loadTaskCallback, TaskType.PENDING, 0);
        this.attachment = new SegmentPendingTaskAttachment(signature);
        this.aggKeyToBrokerFileGroups = aggKeyToBrokerFileGroups;
        this.brokerDesc = brokerDesc;
        this.failMsg = new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL);
    }

    @Override
    void executeTask() throws UserException {
        LOG.info("begin to execute segment pending task. job: {}", callback.getCallbackId());
        parseAllFilePath();
    }

    private void parseAllFilePath() throws UserException {
        long start = System.currentTimeMillis();

        // Segment Load currently only supports defining one data_desc and one file group,
        // and no partitions need to be specified
        Long tableId = null;
        BrokerFileGroup fileGroup = null;
        for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : aggKeyToBrokerFileGroups.entrySet()) {
            tableId = entry.getKey().getTableId();

            if (entry.getValue().size() > 1) {
                throw new UserException("Segment Load currently only supports defining one file group. job: " +
                        callback.getCallbackId());
            }
            fileGroup = entry.getValue().get(0);
        }

        if (tableId == null || fileGroup == null) {
            throw new UserException("Segment Load Data Description definition error, please check load statement. job: "
                    + callback.getCallbackId());
        }

        long totalDataFileSize = 0;
        int totalDataFileNum = 0;
        SegmentPendingTaskAttachment segmentAttachment = (SegmentPendingTaskAttachment) attachment;
        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
        String path = fileGroup.getFilePaths().get(0);
        Set<String> allPbFilePrefix = new HashSet<>();

        if (brokerDesc.hasBroker()) {
            BrokerUtil.parseFile(path, brokerDesc, fileStatuses, false, true);
        } else {
            HdfsUtil.parseFile(path, brokerDesc, fileStatuses, false, false, true);
        }

        for (TBrokerFileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDir) {
                continue;
            }
            String filePath = fileStatus.getPath();
            // tableId/partitionId/indexId/tabletId/data/uuid_0.dat
            // tableId/partitionId/indexId/tabletId/tablet.schema
            String relativePath = filePath.substring(filePath.indexOf(String.valueOf(tableId)));

            if (filePath.contains(SPARK_OUTPUT_TABLET_DATA_DIR) && filePath.endsWith(SPARK_OUTPUT_DATA_FILE_SUFFIX)) {
                String tabletMeta = relativePath.split(SPARK_OUTPUT_TABLET_DATA_DIR)[0];
                segmentAttachment.addDataFileInfo(tabletMeta, Pair.create(filePath, fileStatus.size));
                totalDataFileNum++;
                totalDataFileSize += fileStatus.getSize();
            } else if (filePath.contains(SPARK_OUTPUT_TABLET_DATA_DIR)
                    && filePath.endsWith(SPARK_OUTPUT_PB_FILE_SUFFIX)) {
                String tabletMeta = relativePath.split(SPARK_OUTPUT_TABLET_DATA_DIR)[0];
                allPbFilePrefix.add(tabletMeta);
            } else if (filePath.endsWith(SPARK_OUTPUT_SCHEMA_FILE_SUFFIX)) {
                String tabletMeta = relativePath.substring(0, relativePath.lastIndexOf("/"));
                segmentAttachment.addSchemaFilePath(tabletMeta, filePath);
            }
        }

        if (!segmentAttachment.getTabletMetaToSchemaFilePath().keySet().containsAll(
                segmentAttachment.getTabletMetaToDataFileInfo().keySet())) {
            throw new UserException("Segment Load file path is invalid, some tablet schema files may be missing. job: "
                    + callback.getCallbackId());
        }

        if (allPbFilePrefix.size() != segmentAttachment.getTabletMetaToDataFileInfo().keySet().size()
                || !allPbFilePrefix.containsAll(segmentAttachment.getTabletMetaToDataFileInfo().keySet())) {
            throw new UserException("Segment Load file path is invalid, segment pb files does not match data files. " +
                    "job: " + callback.getCallbackId());
        }

        LOG.info("get {} files to be loaded. total size: {}. cost: {} ms, job: {}",
                 totalDataFileNum, totalDataFileSize, (System.currentTimeMillis() - start), callback.getCallbackId());

        ((SegmentLoadJob) callback).setLoadFileInfo(totalDataFileNum, totalDataFileSize);
    }
}
