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

package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.thrift.THiveFileInfo;
import org.apache.hadoop.fs.Path;

import java.util.Collection;
import java.util.List;

import static com.starrocks.connector.PartitionUtil.getPathWithSlash;

public class PartitionUpdate {
    private final String name;
    private UpdateMode updateMode;
    private final Path writePath;
    private final Path targetPath;
    private final List<String> fileNames;
    private final long rowCount;
    private final long totalSizeInBytes;

    public PartitionUpdate(String name, Path writePath, Path targetPath,
                           List<String> fileNames, long rowCount, long totalSizeInBytes) {
        this.name = name;
        this.writePath = writePath;
        this.targetPath = targetPath;
        this.fileNames = fileNames;
        this.rowCount = rowCount;
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public String getName() {
        return name;
    }

    public UpdateMode getUpdateMode() {
        return updateMode;
    }

    public Path getWritePath() {
        return writePath;
    }

    public Path getTargetPath() {
        return targetPath;
    }

    public List<String> getFileNames() {
        return fileNames;
    }

    public long getFileCount() {
        if (fileNames == null) {
            return 0;
        }
        return fileNames.size();
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public boolean isS3Url() {
        return HiveWriteUtils.isS3Url(targetPath.toString());
    }

    public PartitionUpdate setUpdateMode(UpdateMode updateMode) {
        this.updateMode = updateMode;
        return this;
    }

    public enum UpdateMode {
        NEW,
        APPEND,
        OVERWRITE,
    }

    public static PartitionUpdate get(THiveFileInfo fileInfo, String stagingDir,
                                      String tableLocation, List<String> partitionColNames) {
        Preconditions.checkState(fileInfo.isSetPartition_path() && !Strings.isNullOrEmpty(fileInfo.getPartition_path()),
                "Missing partition path");

        String partitionNameFromPath = PartitionUtil.getPartitionName(stagingDir, fileInfo.getPartition_path());
        List<String> partitionValues = PartitionUtil.toPartitionValues(partitionNameFromPath);
        String partitionName = PartitionUtil.toHivePartitionName(partitionColNames, partitionValues);

        String tableLocationWithSlash = getPathWithSlash(tableLocation);
        String stagingDirWithSlash = getPathWithSlash(stagingDir);

        String writePath = stagingDirWithSlash + partitionNameFromPath + "/";
        String targetPath = tableLocationWithSlash + partitionNameFromPath + "/";

        return new PartitionUpdate(partitionName, new Path(writePath), new Path(targetPath),
                Lists.newArrayList(fileInfo.getFile_name()), fileInfo.getRecord_count(), fileInfo.getFile_size_in_bytes());
    }

    public static List<PartitionUpdate> merge(List<PartitionUpdate> unMergedUpdates) {
        ImmutableList.Builder<PartitionUpdate> partitionUpdates = ImmutableList.builder();
        for (Collection<PartitionUpdate> partitionGroup : Multimaps.index(unMergedUpdates, PartitionUpdate::getName)
                .asMap().values()) {
            PartitionUpdate firstPartition = partitionGroup.iterator().next();

            ImmutableList.Builder<String> allFileNames = ImmutableList.builder();
            long totalRowCount = 0;
            long fileSizeInBytes = 0;
            for (PartitionUpdate partition : partitionGroup) {
                allFileNames.addAll(partition.getFileNames());
                totalRowCount += partition.getRowCount();
                fileSizeInBytes += partition.getTotalSizeInBytes();
            }

            partitionUpdates.add(new PartitionUpdate(firstPartition.getName(),
                    firstPartition.getWritePath(),
                    firstPartition.getTargetPath(),
                    allFileNames.build(),
                    totalRowCount,
                    fileSizeInBytes));
        }
        return partitionUpdates.build();
    }
}
