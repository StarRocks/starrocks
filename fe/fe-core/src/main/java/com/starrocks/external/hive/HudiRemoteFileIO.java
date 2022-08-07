package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.starrocks.external.RemoteFileBlockDesc;
import com.starrocks.external.RemoteFileIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HudiRemoteFileIO extends RemoteFileIO {
    private final Configuration configuration;
    private final HoodieTableMetaClient metaClient;
    private final String basePath;

    public HudiRemoteFileIO(Configuration configuration, HoodieTableMetaClient metaClient, String basePath) {
        this.configuration = configuration;
        this.metaClient = metaClient;
        this.basePath = basePath;
    }

    @Override
    protected Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey) {
        String partitionName =  FSUtils.getRelativePartitionPath(new Path(basePath), new Path(pathKey.getPath()));
        ImmutableMap.Builder<RemotePathKey, List<RemoteFileDesc>> resultPartitions = ImmutableMap.builder();

        List<RemoteFileDesc> fileDescs = Lists.newArrayList();
        FileSystem fileSystem = metaClient.getRawFs();
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(configuration);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
        HoodieTableFileSystemView fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext,
                metaClient, metadataConfig);
        HoodieTimeline activeInstants = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
        Option<HoodieInstant> latestInstant = activeInstants.lastInstant();
        String queryInstant = latestInstant.get().getTimestamp();
        Iterator<HoodieBaseFile> hoodieBaseFileIterator = fileSystemView
                .getLatestBaseFilesBeforeOrOn(partitionName, queryInstant).iterator();

        while (hoodieBaseFileIterator.hasNext()) {
            HoodieBaseFile baseFile = hoodieBaseFileIterator.next();
            BlockLocation[] blockLocations;
            try {
                FileStatus fileStatus = HoodieInputFormatUtils.getFileStatus(baseFile);
                if (fileStatus instanceof LocatedFileStatus) {
                    blockLocations = ((LocatedFileStatus) fileStatus).getBlockLocations();
                } else {
                    blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
                }
                List<RemoteFileBlockDesc> fileBlockDescs = getRemoteFileBlockDesc(blockLocations);
                fileDescs.add(new RemoteFileDesc(baseFile.getFileName(), "", fileStatus.getLen(),
                        ImmutableList.copyOf(fileBlockDescs)));
            } catch (Exception e) {
                throw new StarRocksConnectorException("xxx");
            }
        }
        return resultPartitions.put(pathKey, fileDescs).build();
    }
}
