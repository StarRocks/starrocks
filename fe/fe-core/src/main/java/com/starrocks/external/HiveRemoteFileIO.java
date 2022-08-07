package com.starrocks.external;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.google.common.collect.Maps;
import com.starrocks.external.hive.HivePartitionName;
import com.starrocks.external.hive.Partition;
import com.starrocks.external.hive.RemoteFileDesc;
import com.starrocks.external.hive.RemotePathKey;
import com.starrocks.external.hive.StarRocksConnectorException;
import com.starrocks.external.hive.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class HiveRemoteFileIO extends RemoteFileIO {
    private final Configuration configuration;

    public HiveRemoteFileIO(Configuration configuration) {
        this.configuration = configuration;
    }

    public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey) {
        ImmutableMap.Builder<RemotePathKey, List<RemoteFileDesc>> resultPartitions = ImmutableMap.builder();
        String path = ObjectStorageUtils.formatObjectStoragePath(pathKey.getPath());
        List<RemoteFileDesc> fileDescs = Lists.newArrayList();
        try {
            URI uri = new URI(path.replace(" ", "%20"));
            org.apache.hadoop.fs.FileSystem fileSystem = FileSystem.get(uri, configuration);

            // fileSystem.listLocatedStatus is an api to list all statuses and
            // block locations of the files in the given path in one operation.
            // The performance is better than getting status and block location one by one.
            RemoteIterator<LocatedFileStatus> blockIterator = fileSystem.listLocatedStatus(new Path(path));
            while (blockIterator.hasNext()) {
                LocatedFileStatus locatedFileStatus = blockIterator.next();
                if (!isValidDataFile(locatedFileStatus)) {
                    continue;
                }
                String fileName = Utils.getSuffixName(pathKey.getPath(), locatedFileStatus.getPath().toString());
                BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
                List<RemoteFileBlockDesc> fileBlockDescs = getRemoteFileBlockDesc(blockLocations);
                fileDescs.add(new RemoteFileDesc(fileName, "", locatedFileStatus.getLen(),
                        ImmutableList.copyOf(fileBlockDescs)));
            }

        } catch (Exception e) {
            throw new StarRocksConnectorException("xxxx");
        }
        return resultPartitions.put(pathKey, fileDescs).build();
    }

    private boolean isValidDataFile(FileStatus fileStatus) {
        if (fileStatus.isDirectory()) {
            return false;
        }

        String lcFileName = fileStatus.getPath().getName().toLowerCase();
        return !(lcFileName.startsWith(".") || lcFileName.startsWith("_") ||
                lcFileName.endsWith(".copying") || lcFileName.endsWith(".tmp"));
    }
}
