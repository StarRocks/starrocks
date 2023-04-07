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


package com.starrocks.connector.iceberg.cost;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Long.parseLong;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class IcebergTableStatisticsReader {
    private static final Logger LOG = LogManager.getLogger(IcebergStatisticProvider.class);

    public static final String APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY = "ndv";

    private IcebergTableStatisticsReader() {}

    public static Map<Integer, Long> readNdvs(Table icebergTable, long snapshotId, Set<Integer> columnIds,
                                              boolean extendedStatisticsEnabled) {
        if (!extendedStatisticsEnabled) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<Integer, Long> ndvByColumnId = ImmutableMap.builder();
        Set<Integer> remainingColumnIds = new HashSet<>(columnIds);

        Iterator<StatisticsFile> statisticsFiles = getStatisticsFiles(icebergTable, snapshotId);
        while (!remainingColumnIds.isEmpty() && statisticsFiles.hasNext()) {
            StatisticsFile statisticsFile = statisticsFiles.next();

            Map<Integer, BlobMetadata> thetaBlobsByFieldId = statisticsFile.blobMetadata().stream()
                    .filter(blobMetadata -> blobMetadata.type().equals(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1))
                    .filter(blobMetadata -> blobMetadata.fields().size() == 1)
                    .filter(blobMetadata -> remainingColumnIds.contains(getOnlyElement(blobMetadata.fields())))
                    // Fail loud upon duplicates (there must be none)
                    .collect(toImmutableMap(blobMetadata -> getOnlyElement(blobMetadata.fields()), identity()));

            for (Map.Entry<Integer, BlobMetadata> entry : thetaBlobsByFieldId.entrySet()) {
                int fieldId = entry.getKey();
                BlobMetadata blobMetadata = entry.getValue();
                String ndv = blobMetadata.properties().get(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY);
                if (ndv == null) {
                    LOG.debug("Blob %s is missing %s property", blobMetadata.type(),
                            APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY);
                    remainingColumnIds.remove(fieldId);
                } else {
                    remainingColumnIds.remove(fieldId);
                    ndvByColumnId.put(fieldId, parseLong(ndv));
                }
            }
        }
        return ndvByColumnId.build();
    }

    public static Iterator<StatisticsFile> getStatisticsFiles(Table icebergTable, long snapshotId) {
        return new AbstractIterator<StatisticsFile>() {
            private final Map<Long, StatisticsFile> statsFileBySnapshot = icebergTable.statisticsFiles().stream()
                    .collect(toMap(
                            StatisticsFile::snapshotId,
                            identity(),
                            (a, b) -> {
                                throw new IllegalStateException(String.format(
                                        "Unexpected duplicate statistics files %s, %s", a, b));
                            },
                            HashMap::new));

            @Override
            protected StatisticsFile computeNext() {
                if (statsFileBySnapshot.isEmpty()) {
                    // Already found all statistics files
                    return endOfData();
                }

                StatisticsFile statisticsFile = statsFileBySnapshot.remove(snapshotId);
                if (statisticsFile != null) {
                        return statisticsFile;
                }
                return endOfData();
            }
        };
    }

}
