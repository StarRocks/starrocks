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

import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Shared Hadoop-side wiring for the meta-tier row-group statistics providers
 * (one per load kind). Builds a Hadoop {@link Configuration} that disables
 * the per-scheme filesystem cache for every credentialed scheme StarRocks
 * exposes, and translates {@link TBrokerFileStatus} entries into Hadoop
 * {@link FileStatus} instances suitable for parquet-mr's
 * {@code HadoopInputFile.fromStatus}.
 *
 * <p>The Hadoop {@link Configuration} retains every raw caller-supplied property (for direct HDFS
 * and legacy {@code fs.*} load properties), then applies the same {@link CloudConfigurationFactory}
 * credential translation used by the rest of FE storage access. This makes canonical properties
 * such as {@code aws.s3.use_instance_profile} usable by FE-local footer reads without maintaining a
 * second provider-specific mapping here. Per-scheme filesystem caching is disabled last so
 * concurrent loads with different credentials cannot silently share an under-credentialed
 * {@code FileSystem} instance.
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

    private PreSplitHadoopAccess() {
    }

    static Configuration buildHadoopConfiguration(Map<String, String> properties) {
        Configuration hadoopConfig = new Configuration();
        Map<String, String> effectiveProperties = properties == null ? Collections.emptyMap() : properties;
        effectiveProperties.forEach(hadoopConfig::set);
        CloudConfigurationFactory.buildCloudConfigurationForStorage(effectiveProperties)
                .applyToConfiguration(hadoopConfig);
        for (String scheme : SCHEMES_TO_BUILD_FRESH_FILESYSTEM) {
            hadoopConfig.setBoolean("fs." + scheme + ".impl.disable.cache", true);
        }
        return hadoopConfig;
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
}
