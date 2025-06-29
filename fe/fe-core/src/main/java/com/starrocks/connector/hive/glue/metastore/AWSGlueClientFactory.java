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

package com.starrocks.connector.hive.glue.metastore;

import com.google.common.base.Preconditions;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.aws.AwsCloudCredential;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

import java.net.URI;

public final class AWSGlueClientFactory implements GlueClientFactory {

    private static final Logger LOGGER = LogManager.getLogger(AWSGlueClientFactory.class);

    private final HiveConf conf;

    public AWSGlueClientFactory(HiveConf conf) {
        Preconditions.checkNotNull(conf, "HiveConf cannot be null");
        this.conf = conf;
    }

    @Override
    public GlueClient newClient() throws MetaException {
        AwsCloudCredential glueCloudCredential = CloudConfigurationFactory.buildGlueCloudCredential(conf);
        try {
            GlueClientBuilder glueClientBuilder = GlueClient.builder();
            if (glueCloudCredential != null) {
                AwsCredentialsProvider awsCredentialsProvider = glueCloudCredential.generateAWSCredentialsProvider();
                glueClientBuilder = glueClientBuilder.credentialsProvider(awsCredentialsProvider);

                glueClientBuilder.region(glueCloudCredential.tryToResolveRegion());

                if (!glueCloudCredential.getEndpoint().isEmpty()) {
                    glueClientBuilder.endpointOverride(URI.create(glueCloudCredential.getEndpoint()));
                }
            }
            return glueClientBuilder.build();
        } catch (Exception e) {
            String message = "Unable to build GlueClient: " + e;
            LOGGER.error(message, e);
            throw new MetaException(message);
        }
    }
}
