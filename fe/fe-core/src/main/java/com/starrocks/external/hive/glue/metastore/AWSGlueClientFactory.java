// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.glue.metastore;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import static com.starrocks.external.hive.glue.util.AWSGlueConfig.AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.AWS_GLUE_CONNECTION_TIMEOUT;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.AWS_GLUE_ENDPOINT;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.AWS_GLUE_MAX_CONNECTIONS;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.AWS_GLUE_MAX_RETRY;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.AWS_GLUE_SOCKET_TIMEOUT;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.AWS_REGION;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.DEFAULT_CONNECTION_TIMEOUT;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.DEFAULT_MAX_CONNECTIONS;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.DEFAULT_MAX_RETRY;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.DEFAULT_SOCKET_TIMEOUT;

public final class AWSGlueClientFactory implements GlueClientFactory {

    private static final Logger LOGGER = Logger.getLogger(AWSGlueClientFactory.class);

    private final HiveConf conf;

    public AWSGlueClientFactory(HiveConf conf) {
        Preconditions.checkNotNull(conf, "HiveConf cannot be null");
        this.conf = conf;
    }

    @Override
    public AWSGlue newClient() throws MetaException {
        try {
            AWSGlueClientBuilder glueClientBuilder = AWSGlueClientBuilder.standard()
                    .withCredentials(getAWSCredentialsProvider(conf));

            String regionStr = getProperty(AWS_REGION, conf);
            String glueEndpoint = getProperty(AWS_GLUE_ENDPOINT, conf);

            // ClientBuilder only allows one of EndpointConfiguration or Region to be set
            if (StringUtils.isNotBlank(glueEndpoint)) {
                LOGGER.info("Setting glue service endpoint to " + glueEndpoint);
                glueClientBuilder
                        .setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(glueEndpoint, null));
            } else if (StringUtils.isNotBlank(regionStr)) {
                LOGGER.info("Setting region to : " + regionStr);
                glueClientBuilder.setRegion(regionStr);
            } else {
                Region currentRegion = Regions.getCurrentRegion();
                if (currentRegion != null) {
                    LOGGER.info("Using region from ec2 metadata : " + currentRegion.getName());
                    glueClientBuilder.setRegion(currentRegion.getName());
                } else {
                    LOGGER.info("No region info found, using SDK default region: us-east-1");
                }
            }

            glueClientBuilder.setClientConfiguration(buildClientConfiguration(conf));
            return glueClientBuilder.build();
        } catch (Exception e) {
            String message = "Unable to build AWSGlueClient: " + e;
            LOGGER.error(message);
            throw new MetaException(message);
        }
    }

    private AWSCredentialsProvider getAWSCredentialsProvider(HiveConf conf) {
        Class<? extends AWSCredentialsProviderFactory> providerFactoryClass = conf
                .getClass(AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS,
                        AKSKCredentialsProviderFactory.class).asSubclass(
                        AWSCredentialsProviderFactory.class);
        AWSCredentialsProviderFactory provider = ReflectionUtils.newInstance(
                providerFactoryClass, conf);
        return provider.buildAWSCredentialsProvider(conf);
    }

    private ClientConfiguration buildClientConfiguration(HiveConf hiveConf) {
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxErrorRetry(hiveConf.getInt(AWS_GLUE_MAX_RETRY, DEFAULT_MAX_RETRY))
                .withMaxConnections(hiveConf.getInt(AWS_GLUE_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS))
                .withConnectionTimeout(hiveConf.getInt(AWS_GLUE_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT))
                .withSocketTimeout(hiveConf.getInt(AWS_GLUE_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT));
        return clientConfiguration;
    }

    private static String getProperty(String propertyName, HiveConf conf) {
        return Strings.isNullOrEmpty(System.getProperty(propertyName)) ?
                conf.get(propertyName) : System.getProperty(propertyName);
    }
}
