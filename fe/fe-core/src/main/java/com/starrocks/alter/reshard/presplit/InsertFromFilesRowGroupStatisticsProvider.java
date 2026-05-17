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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Production Tier 1 {@link RowGroupStatisticsProvider} for the INSERT-from-FILES
 * load path. Enumerates the {@link TableFunctionTable}'s already-resolved file
 * list, opens each Parquet file's footer via
 * {@link ParquetRowGroupStatisticsReader}, and concatenates per-row-group
 * statistics projected onto the request's sort key.
 *
 * <p>The Hadoop {@link Configuration} is built by copying every entry in the
 * source table's properties map and explicitly disabling Hadoop's per-scheme
 * filesystem cache so concurrent loads with different credentials cannot
 * silently share an under-credentialed {@code FileSystem} instance. The raw
 * property copy is sufficient for HDFS deployments and for cloud-storage
 * setups where the user has already supplied the right Hadoop keys (e.g.
 * {@code fs.s3a.access.key}). For cloud providers that require key
 * translation (Iceberg-style {@code IcebergCachingFileIO}-class remapping),
 * the Parquet open will fail with {@code IOException}; the pipeline records
 * {@code SAMPLE_FAILED} and the load proceeds against the original single
 * tablet — never an outage. Cloud-aware key translation is a deliberate
 * follow-up.
 */
final class InsertFromFilesRowGroupStatisticsProvider implements RowGroupStatisticsProvider {

    /**
     * Schemes whose Hadoop {@code FileSystem.CACHE} entry we explicitly
     * disable per call. The cache keys handles by {@code (scheme, authority,
     * UGI)} and ignores the supplied {@link Configuration} on a hit, so two
     * loads with different credentials for the same {@code (scheme, authority,
     * UGI)} tuple would otherwise silently share the first load's filesystem.
     * The list covers every cloud/HDFS scheme this provider is expected to
     * meet; schemes outside the list (e.g. {@code file:}) keep the default
     * cached behavior, which is safe because they carry no per-request
     * credentials.
     */
    static final Set<String> SCHEMES_TO_BUILD_FRESH_FILESYSTEM = Set.of(
            "hdfs", "viewfs",
            "s3", "s3a", "s3n", "ks3",
            "oss", "gs", "cosn", "tos", "obs",
            "wasb", "wasbs", "abfs", "abfss", "adl",
            "alluxio", "jfs");

    @Override
    public List<RowGroupStatistics> fetch(SampleRequest request) throws StarRocksException {
        InsertFromFilesScanContext context = requireInsertFromFilesContext(request);
        TableFunctionTable sourceTable = context.sourceTable();
        rejectNonParquetFormat(sourceTable);
        // ParquetMetadataSampler.rejectCompositeSortKey runs upstream in tryPlan
        // before this provider is invoked, so a single-element sort key is the
        // contract by the time we get here.
        Column sortKeyColumn = request.getSortKey().get(0);

        Configuration hadoopConfig = buildHadoopConfiguration(sourceTable.getProperties());

        // Read every non-directory file's footer. The pipeline picks K (tablet
        // count) from total file bytes, and ParquetMetadataSampler computes
        // K-1 row-quantile cuts from the full per-row-group stats list — a
        // partial enumeration would bias the cuts toward the prefix.
        List<RowGroupStatistics> aggregated = new ArrayList<>();
        for (TBrokerFileStatus brokerFileStatus : sourceTable.loadFileList()) {
            if (brokerFileStatus.isDir) {
                continue;
            }
            FileStatus hadoopFileStatus = toHadoopFileStatus(brokerFileStatus);
            aggregated.addAll(
                    ParquetRowGroupStatisticsReader.read(hadoopFileStatus, hadoopConfig, sortKeyColumn));
        }
        return aggregated;
    }

    private static InsertFromFilesScanContext requireInsertFromFilesContext(SampleRequest request)
            throws Tier1UnavailableException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof InsertFromFilesScanContext insertFromFilesContext)) {
            throw new Tier1UnavailableException(
                    "InsertFromFilesRowGroupStatisticsProvider received a " + scanContext.getClass().getSimpleName()
                            + " — wire only the INSERT-from-FILES load kind here");
        }
        return insertFromFilesContext;
    }

    private static void rejectNonParquetFormat(TableFunctionTable sourceTable) throws Tier1UnavailableException {
        String format = sourceTable.getFormat();
        if (format == null || !"parquet".equalsIgnoreCase(format)) {
            throw new Tier1UnavailableException(
                    "Tier 1 supports Parquet sources only; FILES() reported format \"" + format + "\"");
        }
    }

    static Configuration buildHadoopConfiguration(Map<String, String> sourceTableProperties) {
        Configuration hadoopConfig = new Configuration();
        if (sourceTableProperties != null) {
            sourceTableProperties.forEach(hadoopConfig::set);
        }
        for (String scheme : SCHEMES_TO_BUILD_FRESH_FILESYSTEM) {
            hadoopConfig.setBoolean("fs." + scheme + ".impl.disable.cache", true);
        }
        return hadoopConfig;
    }

    private static FileStatus toHadoopFileStatus(TBrokerFileStatus brokerFileStatus) {
        // ParquetFileReader.open(HadoopInputFile.fromStatus(...)) reads the
        // path to open the file; len lets parquet-mr seek the footer offset
        // without an extra stat() RPC. blockReplication / blockSize /
        // modificationTime are unread on this path, so 0s are safe.
        return new FileStatus(
                brokerFileStatus.size,
                brokerFileStatus.isDir,
                /*blockReplication=*/ 0,
                /*blockSize=*/ 0,
                /*modificationTime=*/ 0L,
                new Path(brokerFileStatus.path));
    }
}
