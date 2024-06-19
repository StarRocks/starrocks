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

package com.starrocks.connector.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.view.FileSystemViewManager.createInMemoryFileSystemViewWithTimeline;

public class HudiRemoteFileIO implements RemoteFileIO {
    private static final Logger LOG = LogManager.getLogger(HudiRemoteFileIO.class);
    private final Configuration configuration;

    // table location -> HoodieTableMetaClient
    private final Map<String, HoodieTableMetaClient> hudiClients = new ConcurrentHashMap<>();

    public HudiRemoteFileIO(Configuration configuration) {
        this.configuration = configuration;
    }

    private void createHudiContext(RemotePathKey.HudiContext ctx, String hudiTableLocation) {
        if (ctx.init.get()) {
            return;
        }
        try {
            ctx.lock.lock();
            if (ctx.init.get()) {
                return;
            }
            HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(configuration);
            HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
            HoodieTableMetaClient metaClient =
                    HoodieTableMetaClient.builder().setConf(configuration).setBasePath(hudiTableLocation).build();
            // metaClient.reloadActiveTimeline();
            HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
            Option<HoodieInstant> lastInstant = timeline.lastInstant();
            if (lastInstant.isPresent()) {
                ctx.fsView = createInMemoryFileSystemViewWithTimeline(engineContext, metaClient, metadataConfig, timeline);
                ctx.lastInstant = lastInstant.get();
                ctx.timeline = timeline;
            }
            ctx.init.set(true);
        } finally {
            ctx.lock.unlock();
        }
    }

    @Override
    public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey) {
        String tableLocation = pathKey.getHudiTableLocation().orElseThrow(() ->
                new StarRocksConnectorException("Missing hudi table base location on %s", pathKey));

        String partitionPath = pathKey.getPath();
        String partitionName = FSUtils.getRelativePartitionPath(new Path(tableLocation), new Path(partitionPath));

        ImmutableMap.Builder<RemotePathKey, List<RemoteFileDesc>> resultPartitions = ImmutableMap.builder();
        List<RemoteFileDesc> fileDescs = Lists.newArrayList();

        RemotePathKey.HudiContext hudiContext = pathKey.getHudiContext();
        createHudiContext(hudiContext, tableLocation);
        if (hudiContext.lastInstant == null) {
            return resultPartitions.put(pathKey, fileDescs).build();
        }

        try {
            Iterator<FileSlice> hoodieFileSliceIterator = hudiContext.fsView
                    .getLatestMergedFileSlicesBeforeOrOn(partitionName, hudiContext.lastInstant.getTimestamp()).iterator();
            while (hoodieFileSliceIterator.hasNext()) {
                FileSlice fileSlice = hoodieFileSliceIterator.next();
                Optional<HoodieBaseFile> baseFile = fileSlice.getBaseFile().toJavaOptional();
                String fileName = baseFile.map(BaseFile::getFileName).orElse("");
                long fileLength = baseFile.map(BaseFile::getFileLen).orElse(-1L);
                List<String> logs = fileSlice.getLogFiles().map(HoodieLogFile::getFileName).collect(Collectors.toList());
                // The file name of HoodieBaseFile contains "instantTime", so we set the `modificationTime` to 0.
                RemoteFileDesc res = new RemoteFileDesc(fileName, "", fileLength, 0,
                        ImmutableList.of(), ImmutableList.copyOf(logs));
                res.setHudiInstant(hudiContext.lastInstant);
                fileDescs.add(res);
            }
        } catch (Exception e) {
            LOG.error("Failed to get hudi remote file's metadata on path: {}", partitionPath, e);
            throw new StarRocksConnectorException("Failed to get hudi remote file's metadata on path: %s. msg: %s",
                    pathKey, e.getMessage());
        }
        return resultPartitions.put(pathKey, fileDescs).build();
    }

    @Override
    public FileStatus[] getFileStatus(Path... files) {
        throw new UnsupportedOperationException("getFileStatus");
    }
}
