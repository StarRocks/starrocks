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

package com.starrocks.connector.iceberg;

import com.google.common.collect.Maps;
import com.starrocks.connector.PartitionInfo;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Conversions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IcebergUtil {

    public static final String TRANSFORM_DAY = "day";
    public static final String TRANSFORM_MONTH = "month";
    public static final String TRANSFORM_YEAR = "year";

    public static Map<String, PartitionInfo> getPartition(org.apache.iceberg.Table icebergTable) {
        Map<String, PartitionInfo> partitionNameWithPartition = Maps.newHashMap();
        Map<String, List<PartitionInfo>> columnNameWithPartitionName = Maps.newHashMap();
        PartitionSpec partitionSpec = icebergTable.spec();
        Schema schema = icebergTable.schema();
        for (ManifestFile manifestFile : IcebergUtil.readManifestFiles(icebergTable.currentSnapshot(), icebergTable.io())) {
            // Iceberg no partition, return table name with latest's snapshot time.
            Snapshot snapshot = icebergTable.snapshot(manifestFile.snapshotId());
            if (partitionSpec.isUnpartitioned()) {
                String tableName = icebergTable.name();
                PartitionInfo partitionInfo = partitionNameWithPartition.get(tableName);
                long snapshotTime = snapshot == null ? 0L : snapshot.timestampMillis();
                if (partitionInfo == null || partitionInfo.getModifiedTime() < snapshotTime) {
                    partitionNameWithPartition.put(tableName, new Partition(tableName, snapshotTime));
                }
                continue;
            }
            List<ManifestFile.PartitionFieldSummary> partitionFieldSummaries = manifestFile.partitions();
            for (int index = 0; index < partitionFieldSummaries.size(); index++) {
                PartitionField partitionField = partitionSpec.fields().get(index);
                // remove transform name
                String columnName = schema.findColumnName(partitionField.sourceId());
                org.apache.iceberg.types.Type sourceType = schema.findType(partitionField.sourceId());
                org.apache.iceberg.types.Type result = partitionField.transform().getResultType(sourceType);
                // NOTE: now only support time column
                if (partitionField.transform().dedupName().equals("time")) {
                    // get partition info in manifest list is range
                    List<PartitionInfo> partitionNames = getRangePartitionNames(result,
                            columnName,
                            partitionField,
                            snapshot,
                            partitionFieldSummaries.get(index));
                    List<PartitionInfo> partitionInfos = columnNameWithPartitionName.get(columnName);
                    if (partitionInfos == null) {
                        partitionInfos = new ArrayList<>();
                    }
                    partitionInfos.addAll(partitionNames);
                    columnNameWithPartitionName.put(columnName, partitionInfos);
                }
            }
        }
        for (Map.Entry<String, List<PartitionInfo>> entry : columnNameWithPartitionName.entrySet()) {
            for (PartitionInfo partitionInfo : entry.getValue()) {
                Partition partition = (Partition) partitionInfo;
                Partition oldPartition = (Partition) partitionNameWithPartition.getOrDefault(partition.getPartitionName(), null);
                if (oldPartition == null || oldPartition.getModifiedTime() < partition.getModifiedTime()) {
                    partitionNameWithPartition.put(partition.getPartitionName(), partition);
                }
            }
        }
        return partitionNameWithPartition;
    }

    public static List<PartitionInfo> getRangePartitionNames(org.apache.iceberg.types.Type type,
                                                             String columnName,
                                                             PartitionField partitionField, Snapshot snapshot,
                                                             ManifestFile.PartitionFieldSummary  partitionFieldSummary) {
        List<PartitionInfo> partitionNames = new ArrayList<>();
        Object lowerBound = Conversions.fromByteBuffer(type, partitionFieldSummary.lowerBound());
        Object upperBound = Conversions.fromByteBuffer(type, partitionFieldSummary.upperBound());
        // because time in iceberg is utc timezone for case:
        // case1 : time = 2022-10-05 12:12:00 --> partition is 2022-10-05 in iceberg/fe
        // case2 : time = 2022-10-05 22:12:00 --> partition is 2022-10-05 in iceberg, in fe is 2022-10-06
        for (int value = (int) lowerBound; value <= (int) upperBound + 1; value++) {
            StringBuilder sb = new StringBuilder();
            sb.append(columnName);
            sb.append("=");
            switch (partitionField.transform().toString()) {
                case TRANSFORM_DAY:
                    sb.append(TransformUtil.humanDay(value));
                    break;
                case TRANSFORM_MONTH:
                    sb.append(TransformUtil.humanMonth(value));
                    break;
                case TRANSFORM_YEAR:
                    sb.append(TransformUtil.humanYear(value));
                    break;
            }
            partitionNames.add(new Partition(sb.toString(), snapshot == null ? 0L : snapshot.timestampMillis()));
        }
        return partitionNames;
    }

    public static CloseableIterable<ManifestFile> readManifestFiles(Snapshot snapshot, FileIO fileIO) {
        return Avro.read(fileIO.newInputFile(snapshot.manifestListLocation()))
                .rename("manifest_file", GenericManifestFile.class.getName())
                .rename("partitions", GenericPartitionFieldSummary.class.getName())
                .rename("r508", GenericPartitionFieldSummary.class.getName())
                .classLoader(GenericManifestFile.class.getClassLoader())
                .project(ManifestFile.schema())
                .reuseContainers(true)
                .build();
    }
}
