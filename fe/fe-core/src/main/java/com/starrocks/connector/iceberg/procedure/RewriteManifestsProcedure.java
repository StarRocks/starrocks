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

package com.starrocks.connector.iceberg.procedure;

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;

public class RewriteManifestsProcedure extends IcebergTableProcedure {
    private static final String PROCEDURE_NAME = "rewrite_manifests";
    private static final int MAX_MANIFEST_CLUSTERS = 100;

    private static final RewriteManifestsProcedure INSTANCE = new RewriteManifestsProcedure();

    public static RewriteManifestsProcedure getInstance() {
        return INSTANCE;
    }

    private RewriteManifestsProcedure() {
        super(
                PROCEDURE_NAME,
                Collections.emptyList(),
                IcebergTableOperation.REWRITE_MANIFESTS
        );
    }

    @Override
    public ShowResultSet execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (!args.isEmpty()) {
            throw new StarRocksConnectorException(
                    "invalid args. rewrite_manifests operation does not support any arguments");
        }

        IcebergMaintenanceTaskStats stats = context.stats();
        stats.setOperation(IcebergTableOperation.REWRITE_MANIFESTS);

        Table icebergTable = context.table();
        Snapshot currentSnapshot = icebergTable.currentSnapshot();
        if (currentSnapshot == null) {
            return null;
        }

        // stream the manifest list instead of materializing it via allManifests():
        // only the count and lengths are needed to decide whether to rewrite
        long manifestCount = 0;
        long totalManifestsSize = 0;
        try (CloseableIterable<ManifestFile> manifests =
                IcebergUtil.readManifests(currentSnapshot, icebergTable.io())) {
            for (ManifestFile manifest : manifests) {
                manifestCount++;
                totalManifestsSize += manifest.length();
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException(
                    "Unable to read manifests for snapshot " + currentSnapshot.snapshotId(), e);
        }
        if (manifestCount == 0) {
            return null;
        }

        long manifestTargetSizeBytes = PropertyUtil.propertyAsLong(
                icebergTable.properties(), MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        if (manifestTargetSizeBytes <= 0) {
            manifestTargetSizeBytes = MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
        }
        stats.setManifestCountInput(manifestCount);
        stats.setManifestTargetSizeBytes(manifestTargetSizeBytes);
        stats.setManifestBytesInput(totalManifestsSize);

        if (manifestCount == 1 && totalManifestsSize < manifestTargetSizeBytes) {
            return null;
        }
        // Having too many open manifest writers can potentially cause OOM on the coordinator
        // Floor to at least 1 to avoid modulo-by-zero when totalManifestsSize is 0 (e.g., manifests report length 0)
        long targetManifestClusters = Math.max(1, Math.min(
                ((totalManifestsSize + manifestTargetSizeBytes - 1) / manifestTargetSizeBytes),
                MAX_MANIFEST_CLUSTERS));

        RewriteManifests rewriteManifests = context.transaction().rewriteManifests();
        if (context.executorService() != null) {
            rewriteManifests = rewriteManifests.scanManifestsWith(context.executorService());
        }
        Types.StructType structType = icebergTable.spec().partitionType();

        rewriteManifests
                .clusterBy(file -> {
                    // Cluster by partitions for better locality when reading data files
                    StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(structType).set(file.partition());
                    // Limit the number of clustering buckets to avoid creating too many small manifest files
                    return Integer.toUnsignedLong(Objects.hash(partitionWrapper)) % targetManifestClusters;
                })
                .commit();
        stats.setExecuted(true);
        return null;
    }
}
