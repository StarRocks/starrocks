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

import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;

import java.util.HashMap;
import java.util.Map;

/**
 * Turns the credentials Lake Formation vended into the CloudConfiguration that FE listing and BE scanning
 * consume. The only place that conversion happens.
 *
 * Built from nothing rather than by copying the catalog's properties and overwriting three of them. The
 * catalog almost certainly configures how to obtain the cluster's own identity - an instance profile, an
 * assumed role - and AwsCloudCredential.applyToConfiguration prefers those over static keys. Carrying them
 * along would produce a configuration that silently reads with the cluster's identity while appearing to
 * hold the vended one, which is the exact failure a Lake Formation deployment exists to prevent.
 *
 * Shaped after CloudConfigurationFactory.buildCloudConfigurationForAWSVendedCredentials, which does the
 * same job for Iceberg's vended credentials.
 */
final class LakeFormationCredentials {

    private LakeFormationCredentials() {
    }

    /**
     * @param catalogProperties the catalog's own properties; only the addressing options are read from it,
     *                          never anything that selects a credentials provider
     * @throws LakeFormationTableAccessException if the result is not usable AWS credentials
     */
    static CloudConfiguration toCloudConfiguration(LakeFormationTableAccess access,
                                                   LakeFormationCatalogProperties lakeFormationProperties,
                                                   Map<String, String> catalogProperties) {
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, access.accessKeyId());
        properties.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, access.secretAccessKey());
        properties.put(CloudConfigurationConstants.AWS_S3_SESSION_TOKEN, access.sessionToken());
        properties.put(CloudConfigurationConstants.AWS_S3_REGION, lakeFormationProperties.region());

        // These say where the bucket is, not who is asking, so they have to survive or a catalog pointed at
        // a non-default endpoint - or at a bucket outside the Data Catalog's region - would stop working the
        // moment Lake Formation is switched on. The region put above is only the default for the usual case
        // where the catalog does not say.
        copyIfPresent(catalogProperties, properties, CloudConfigurationConstants.AWS_S3_REGION);
        copyIfPresent(catalogProperties, properties, CloudConfigurationConstants.AWS_S3_ENDPOINT);
        copyIfPresent(catalogProperties, properties,
                CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS);

        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        // Not a redundant check: the factory ends its chain with a plain CloudConfiguration whose type is
        // DEFAULT, so a validation failure arrives as a non-null object rather than as an error. Everything
        // downstream that only tests for null would take it for a working configuration and scan with
        // whatever ambient credentials the process happens to have.
        if (cloudConfiguration == null || cloudConfiguration.getCloudType() != CloudType.AWS) {
            throw new LakeFormationTableAccessException("The credentials Lake Formation vended for "
                    + access.identity() + " did not produce an AWS cloud configuration, so this table"
                    + " cannot be scanned.");
        }
        return cloudConfiguration;
    }

    private static void copyIfPresent(Map<String, String> from, Map<String, String> to, String key) {
        if (from == null) {
            return;
        }
        String value = from.get(key);
        if (value != null && !value.isEmpty()) {
            to.put(key, value);
        }
    }
}
