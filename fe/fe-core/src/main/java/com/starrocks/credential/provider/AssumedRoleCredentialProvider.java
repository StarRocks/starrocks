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

// This file is based on code available under the Apache license here:
// https://github.com/apache/hadoop/blob/fdcbc8b072ccdb48baeaff843d81ef240e4477e6/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/auth/AssumedRoleCredentialProvider.java

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.credential.provider;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.CredentialInitializationException;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3ARetryPolicy;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.STSClientFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static org.apache.hadoop.fs.s3a.S3AUtils.buildAWSProviderList;

/**
 * This file is copied from hadoop, we only make a slight change, let this
 * credential provider support custom external-id.
 * <p>
 * Support IAM Assumed roles by instantiating an instance of
 * {@code STSAssumeRoleSessionCredentialsProvider} from configuration
 * properties, including wiring up the inner authenticator, and,
 * unless overridden, creating a session name from the current user.
 * <p>
 * Classname is used in configuration files; do not move.
 */
public class AssumedRoleCredentialProvider implements AWSCredentialsProvider,
        Closeable {

    // Custom configuration
    public static final String CUSTOM_CONSTANT_HADOOP_EXTERNAL_ID = "starrocks.fs.s3a.external-id";

    private static final Logger LOG =
            LoggerFactory.getLogger(AssumedRoleCredentialProvider.class);
    public static final String NAME
            = "com.starrocks.credential.provider.AssumedRoleCredentialProvider";

    public static final String E_NO_ROLE = "Unset property "
            + Constants.ASSUMED_ROLE_ARN;

    private final STSAssumeRoleSessionCredentialsProvider stsProvider;

    private final String sessionName;

    private final long duration;

    private final String arn;

    private final AWSCredentialProviderList credentialsToSTS;

    private final Invoker invoker;

    /**
     * Instantiate.
     * This calls {@link #getCredentials()} to fail fast on the inner
     * role credential retrieval.
     *
     * @param fsUri possibly null URI of the filesystem.
     * @param conf  configuration
     * @throws IOException                      on IO problems and some parameter checking
     * @throws IllegalArgumentException         invalid parameters
     * @throws AWSSecurityTokenServiceException problems getting credentials
     */
    public AssumedRoleCredentialProvider(@Nullable URI fsUri, Configuration conf)
            throws IOException {

        arn = conf.getTrimmed(Constants.ASSUMED_ROLE_ARN, "");
        if (StringUtils.isEmpty(arn)) {
            throw new IOException(E_NO_ROLE);
        }

        // build up the base provider
        credentialsToSTS = buildAWSProviderList(fsUri, conf,
                Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER,
                Arrays.asList(
                        SimpleAWSCredentialsProvider.class,
                        EnvironmentVariableCredentialsProvider.class),
                Sets.newHashSet(this.getClass()));
        LOG.debug("Credentials to obtain role credentials: {}", credentialsToSTS);

        // then the STS binding
        sessionName = conf.getTrimmed(Constants.ASSUMED_ROLE_SESSION_NAME,
                buildSessionName());
        duration = conf.getTimeDuration(Constants.ASSUMED_ROLE_SESSION_DURATION,
                Constants.ASSUMED_ROLE_SESSION_DURATION_DEFAULT, TimeUnit.SECONDS);
        String policy = conf.getTrimmed(Constants.ASSUMED_ROLE_POLICY, "");

        LOG.debug("{}", this);
        STSAssumeRoleSessionCredentialsProvider.Builder builder
                = new STSAssumeRoleSessionCredentialsProvider.Builder(arn, sessionName);
        builder.withRoleSessionDurationSeconds((int) duration);
        if (StringUtils.isNotEmpty(policy)) {
            LOG.debug("Scope down policy {}", policy);
            builder.withScopeDownPolicy(policy);
        }
        String endpoint = conf.getTrimmed(Constants.ASSUMED_ROLE_STS_ENDPOINT, "");
        String region = conf.getTrimmed(Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION,
                Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION_DEFAULT);
        String externalId = conf.getTrimmed(CUSTOM_CONSTANT_HADOOP_EXTERNAL_ID, "");
        AWSSecurityTokenServiceClientBuilder stsbuilder =
                STSClientFactory.builder(
                        conf,
                        fsUri != null ? fsUri.getHost() : "",
                        credentialsToSTS,
                        endpoint,
                        region);
        // the STS client is not tracked for a shutdown in close(), because it
        // (currently) throws an UnsupportedOperationException in shutdown().
        builder.withStsClient(stsbuilder.build());
        if (!externalId.isEmpty()) {
            builder.withExternalId(externalId);
        }

        //now build the provider
        stsProvider = builder.build();

        // to handle STS throttling by the AWS account, we
        // need to retry
        invoker = new Invoker(new S3ARetryPolicy(conf), this::operationRetried);

        // and force in a fail-fast check just to keep the stack traces less
        // convoluted
        getCredentials();
    }

    /**
     * Get credentials.
     *
     * @return the credentials
     * @throws AWSSecurityTokenServiceException if none could be obtained.
     */
    @Override
    @Retries.RetryRaw
    public AWSCredentials getCredentials() {
        try {
            return invoker.retryUntranslated("getCredentials",
                    true,
                    stsProvider::getCredentials);
        } catch (IOException e) {
            // this is in the signature of retryUntranslated;
            // its hard to see how this could be raised, but for
            // completeness, it is wrapped as an Amazon Client Exception
            // and rethrown.
            throw new CredentialInitializationException(
                    "getCredentials failed: " + e,
                    e);
        } catch (AWSSecurityTokenServiceException e) {
            LOG.error("Failed to get credentials for role {}",
                    arn, e);
            throw e;
        }
    }

    @Override
    public void refresh() {
        stsProvider.refresh();
    }

    /**
     * Propagate the close() call to the inner stsProvider.
     */
    @Override
    public void close() {
        S3AUtils.closeAutocloseables(LOG, stsProvider, credentialsToSTS);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(
                "AssumedRoleCredentialProvider{");
        sb.append("role='").append(arn).append('\'');
        sb.append(", session'").append(sessionName).append('\'');
        sb.append(", duration=").append(duration);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Build the session name from the current user's shortname.
     *
     * @return a string for the session name.
     * @throws IOException failure to get the current user
     */
    static String buildSessionName() throws IOException {
        return sanitize(UserGroupInformation.getCurrentUser()
                .getShortUserName());
    }

    /**
     * Build a session name from the string, sanitizing it for the permitted
     * characters.
     *
     * @param session source session
     * @return a string for use in role requests.
     */
    @VisibleForTesting
    static String sanitize(String session) {
        StringBuilder r = new StringBuilder(session.length());
        for (char c : session.toCharArray()) {
            if ("abcdefghijklmnopqrstuvwxyz0123456789,.@-".contains(
                    Character.toString(c).toLowerCase(Locale.ENGLISH))) {
                r.append(c);
            } else {
                r.append('-');
            }
        }
        return r.toString();
    }

    /**
     * Callback from {@link Invoker} when an operation is retried.
     *
     * @param text       text of the operation
     * @param ex         exception
     * @param retries    number of retries
     * @param idempotent is the method idempotent
     */
    public void operationRetried(
            String text,
            Exception ex,
            int retries,
            boolean idempotent) {
        if (retries == 0) {
            // log on the first retry attempt of the credential access.
            // At worst, this means one log entry every intermittent renewal
            // time.
            LOG.info("Retried {}", text);
        }
    }
}

