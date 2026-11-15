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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Shared Hadoop-side wiring for the meta-tier row-group statistics providers
 * (one per load kind). Builds a Hadoop {@link Configuration} that disables
 * the per-scheme filesystem cache for every credentialed scheme StarRocks
 * exposes, translates {@link TBrokerFileStatus} entries into Hadoop
 * {@link FileStatus} instances suitable for parquet-mr's
 * {@code HadoopInputFile.fromStatus}, and runs the resulting per-file footer
 * reads.
 *
 * <p>The Hadoop {@link Configuration} retains every raw caller-supplied property (for direct HDFS
 * and legacy {@code fs.*} load properties), then applies the same {@link CloudConfigurationFactory}
 * credential translation used by the rest of FE storage access. This makes canonical properties
 * such as {@code aws.s3.use_instance_profile} usable by FE-local footer reads without maintaining a
 * second provider-specific credential mapping here. Legacy Broker properties keep their native
 * {@code oss://} / {@code cosn://} Hadoop implementations when the cloud factory does not select a
 * canonical provider. Per-scheme filesystem caching is disabled last so concurrent loads with
 * different credentials cannot silently share an under-credentialed {@code FileSystem} instance.
 */
final class PreSplitHadoopAccess {

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
    private static final Map<String, String> LEGACY_SCHEME_IMPLEMENTATIONS = Map.of(
            "fs.oss.", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem",
            "fs.cosn.", "org.apache.hadoop.fs.CosFileSystem");

    private PreSplitHadoopAccess() {
    }

    static Configuration buildHadoopConfiguration(Map<String, String> properties) {
        Configuration hadoopConfig = new Configuration();
        Map<String, String> effectiveProperties = properties == null ? Collections.emptyMap() : properties;
        effectiveProperties.forEach(hadoopConfig::set);
        CloudConfigurationFactory.buildCloudConfigurationForStorage(effectiveProperties)
                .applyToConfiguration(hadoopConfig);
        registerLegacySchemeImplementations(effectiveProperties, hadoopConfig);
        for (String scheme : SCHEMES_TO_BUILD_FRESH_FILESYSTEM) {
            hadoopConfig.setBoolean("fs." + scheme + ".impl.disable.cache", true);
        }
        return hadoopConfig;
    }

    private static void registerLegacySchemeImplementations(
            Map<String, String> properties, Configuration hadoopConfig) {
        LEGACY_SCHEME_IMPLEMENTATIONS.forEach((propertyPrefix, implementation) -> {
            String implementationKey = propertyPrefix + "impl";
            boolean hasLegacyProperties = properties.keySet().stream()
                    .anyMatch(property -> property.startsWith(propertyPrefix));
            if (hasLegacyProperties && hadoopConfig.get(implementationKey) == null) {
                hadoopConfig.set(implementationKey, implementation);
            }
        });
    }

    static FileStatus toHadoopFileStatus(TBrokerFileStatus brokerFileStatus) {
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

    /** One file's footer read: the file plus the reader its already-resolved format selects. */
    record FooterRead(MetaTierFormat format, FileStatus file) {
    }

    /**
     * Reads every file's footer and concatenates the per-row-group / per-stripe statistics in
     * request order. Footer reads are independent per file and the samplers sort the aggregated
     * statistics, so they run concurrently up to
     * {@link Config#tablet_pre_split_meta_tier_footer_read_parallelism} — each footer is a remote
     * round-trip, and a serial pass over a many-file source otherwise dominates the pre-split hook,
     * which runs on the triggering load's critical path.
     *
     * <p>A per-file {@link StarRocksException} (e.g. a {@link MetaTierUnavailableException} for
     * truncated / unmappable statistics) propagates unchanged so the pipeline falls back to the data
     * tier exactly as a serial reader would.
     */
    static List<RowGroupStatistics> readFooters(
            List<FooterRead> footerReads, Configuration hadoopConfig, List<Column> sortKeyColumns,
            String loadTimeZone) throws StarRocksException {
        int parallelism = Math.max(1,
                Math.min(Config.tablet_pre_split_meta_tier_footer_read_parallelism, footerReads.size()));
        if (parallelism == 1) {
            List<RowGroupStatistics> aggregated = new ArrayList<>();
            for (FooterRead footerRead : footerReads) {
                aggregated.addAll(footerRead.format().read(
                        footerRead.file(), hadoopConfig, sortKeyColumns, loadTimeZone));
            }
            return aggregated;
        }
        ExecutorService footerReadPool = Executors.newFixedThreadPool(parallelism,
                new ThreadFactoryBuilder().setNameFormat("presplit-footer-reader-%d").setDaemon(true).build());
        try {
            List<Future<List<RowGroupStatistics>>> futures = new ArrayList<>(footerReads.size());
            for (FooterRead footerRead : footerReads) {
                futures.add(footerReadPool.submit(() -> footerRead.format().read(
                        footerRead.file(), hadoopConfig, sortKeyColumns, loadTimeZone)));
            }
            List<RowGroupStatistics> aggregated = new ArrayList<>();
            for (Future<List<RowGroupStatistics>> future : futures) {
                aggregated.addAll(joinFooterRead(future));
            }
            return aggregated;
        } finally {
            footerReadPool.shutdownNow();
        }
    }

    /**
     * Awaits one footer-read task. A per-file {@link StarRocksException} is rethrown unchanged; any
     * other failure is wrapped as a checked {@link StarRocksException}.
     */
    private static List<RowGroupStatistics> joinFooterRead(Future<List<RowGroupStatistics>> future)
            throws StarRocksException {
        try {
            return future.get();
        } catch (ExecutionException executionFailure) {
            Throwable cause = executionFailure.getCause();
            if (cause instanceof StarRocksException starRocksException) {
                throw starRocksException;
            }
            throw new StarRocksException("Parquet/ORC footer read failed during pre-split sampling: "
                    + (cause == null ? executionFailure.getMessage() : cause.getMessage()), cause);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new StarRocksException("Interrupted while reading footers for pre-split sampling", interrupted);
        }
    }
}
