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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.credential.CloudConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * The listing stack for one table on its vended credentials. The catalog-level file cache is bypassed: its key
 * says nothing about who may read the path.
 */
final class LakeFormationFileListing implements AutoCloseable {

    private static final String PER_BUCKET_PREFIX = "fs.s3a.bucket.";
    private static final List<String> PER_BUCKET_CREDENTIAL_SUFFIXES = List.of(
            ".access.key", ".secret.key", ".session.token", ".aws.credentials.provider",
            ".security.credential.provider.path");

    private final LakeFormationRemoteFileIO fileIO;
    private final RemoteFileOperations operations;

    private LakeFormationFileListing(LakeFormationRemoteFileIO fileIO, RemoteFileOperations operations) {
        this.fileIO = fileIO;
        this.operations = operations;
    }

    /**
     * @param cloudConfiguration the vended credentials; applied to a copy of the catalog's configuration so
     *                           the catalog's own stays untouched
     * @param baseConfiguration  the catalog's configuration, read for everything that is not a credential
     */
    static LakeFormationFileListing open(CloudConfiguration cloudConfiguration,
                                         Configuration baseConfiguration,
                                         ExecutorService pullRemoteFileExecutor,
                                         boolean recursiveListing) {
        Configuration configuration = withoutPerBucketCredentials(new Configuration(baseConfiguration));
        cloudConfiguration.applyToConfiguration(configuration);

        LakeFormationRemoteFileIO fileIO = new LakeFormationRemoteFileIO(configuration);
        RemoteFileOperations operations = new RemoteFileOperations(
                // Query level only; the catalog cache is not keyed by who may read an entry.
                CachingRemoteFileIO.createQueryLevelInstance(fileIO, 0),
                pullRemoteFileExecutor,
                pullRemoteFileExecutor,
                recursiveListing,
                // The catalog level cache is deliberately not consulted: its entries are not keyed by who
                // was authorized to read them.
                false,
                configuration);
        return new LakeFormationFileListing(fileIO, operations);
    }

    /**
     * S3A applies fs.s3a.bucket.&lt;name&gt;.* over the global keys, so a per-bucket credential inherited from the
     * catalog would win over the vended one. Matched by suffix: bucket names may contain dots.
     */
    static Configuration withoutPerBucketCredentials(Configuration configuration) {
        List<String> drop = new ArrayList<>();
        for (Map.Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            if (key.startsWith(PER_BUCKET_PREFIX) && (key.contains(".assumed.role.")
                    || PER_BUCKET_CREDENTIAL_SUFFIXES.stream().anyMatch(key::endsWith))) {
                drop.add(key);
            }
        }
        drop.forEach(configuration::unset);
        return configuration;
    }

    RemoteFileOperations operations() {
        return operations;
    }

    @Override
    public void close() {
        fileIO.close();
    }
}
