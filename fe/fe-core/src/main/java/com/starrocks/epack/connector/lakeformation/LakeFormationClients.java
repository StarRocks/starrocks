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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import java.time.Duration;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * The Glue and Lake Formation clients, both built on the tagged session.
 *
 * They share one credentials provider on purpose: Lake Formation matches the caller against the
 * session tag, so a client built on the untagged base credentials would be refused - and it would be
 * refused with a message that names neither the tag nor the identity, which is why this is a single
 * factory rather than two call sites that each remember to tag.
 *
 * Closeable because the three SDK clients hold connection pools, and a dropped catalog has to let go of
 * them. The tagged provider itself needs no closing: StsAssumeRoleCredentialsProvider only starts a
 * background refresh thread when asyncCredentialUpdateEnabled is set, which this factory does not set, so
 * the session is refreshed lazily on the calling thread and stops being refreshed as soon as nobody asks.
 */
public final class LakeFormationClients implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(LakeFormationClients.class);

    private final StsClient stsClient;
    private final GlueClient glueClient;
    private final LakeFormationClient lakeFormationClient;

    private LakeFormationClients(StsClient stsClient, GlueClient glueClient,
                                 LakeFormationClient lakeFormationClient) {
        this.stsClient = stsClient;
        this.glueClient = glueClient;
        this.lakeFormationClient = lakeFormationClient;
    }

    /**
     * @param baseCredentials the credentials the process already runs with; the tagged session is
     *                        assumed on top of them, never instead of them
     */
    public static LakeFormationClients create(LakeFormationCatalogProperties properties,
                                              AwsCredentialsProvider baseCredentials) {
        requireNonNull(properties, "properties is null");
        requireNonNull(baseCredentials, "baseCredentials is null");

        Region region = Region.of(properties.region());
        StsClient stsClient = StsClient.builder()
                .credentialsProvider(baseCredentials)
                .region(region)
                .build();
        GlueClient glueClient = null;
        try {
            AwsCredentialsProvider tagged = LakeFormationSession.taggedCredentialsProvider(
                    stsClient, properties.iamRoleArn(), properties.sessionTagValue());
            probeTaggedSession(tagged, properties);
            glueClient = GlueClient.builder()
                    .credentialsProvider(tagged)
                    .region(region)
                    .build();
            LakeFormationClient lakeFormationClient = LakeFormationClient.builder()
                    .credentialsProvider(tagged)
                    .region(region)
                    .build();
            return new LakeFormationClients(stsClient, glueClient, lakeFormationClient);
        } catch (RuntimeException e) {
            // Every client built so far is ours until the wrapper owns them, and a failed catalog
            // creation must not leave connection pools behind. The Lake Formation client is the one that
            // can throw after Glue already succeeded, so Glue has to be closed here too.
            closeQuietly(glueClient);
            closeQuietly(stsClient);
            throw e;
        }
    }

    /**
     * Resolves the tagged session once, at catalog creation, for two reasons.
     *
     * <p><b>Fail fast.</b> StsAssumeRoleCredentialsProvider is lazy, so without this a catalog naming a
     * role it cannot assume - or a session tag the account does not allow - is created successfully and
     * only fails on the first query, far from the configuration that caused it.
     *
     * <p><b>Report the lease ceiling that the session imposes.</b> A tagged session is role chaining,
     * which AWS caps at one hour. Measured against a real account: the session came back with roughly
     * 3600 seconds and every GetTemporaryGlueTableCredentials asking for 21600 or 43200 was refused with
     * InvalidInputException, while 3600 succeeded. The two are consistent with the chaining cap applying
     * to the lease as well, so a longer configured duration is very likely unusable.
     *
     * <p>This warns rather than refuses on purpose. The link between the two limits is inferred from
     * observation, not from a documented contract, and the API itself accepts up to 43200 - so an account
     * where the longer duration does work must not be blocked by a guess made here.
     */
    private static void probeTaggedSession(AwsCredentialsProvider tagged,
                                           LakeFormationCatalogProperties properties) {
        AwsCredentials credentials = tagged.resolveCredentials();
        if (!(credentials instanceof AwsSessionCredentials session)) {
            return;
        }
        session.expirationTime().ifPresent(expiry -> {
            long seconds = Duration.between(Instant.now(), expiry).getSeconds();
            if (seconds > 0 && properties.credentialDurationSeconds() > seconds) {
                LOG.warn("The Lake Formation session for this catalog lasts about {}s, but {}={}s is "
                                + "configured. A tagged session is role chaining, which AWS caps at one "
                                + "hour, and credential requests longer than the session have been "
                                + "observed to be refused outright rather than shortened. Expect every "
                                + "query on this catalog to fail until the duration is lowered.",
                        seconds, LakeFormationCatalogProperties.CREDENTIAL_DURATION_SECONDS,
                        properties.credentialDurationSeconds());
            }
        });
    }

    /**
     * Used only on the construction failure path, where the exception being propagated is the one worth
     * reporting: a close that fails while unwinding must not replace it.
     */
    private static void closeQuietly(SdkAutoCloseable client) {
        if (client == null) {
            return;
        }
        try {
            client.close();
        } catch (RuntimeException ignored) {
            // Reported through the original failure, which is already on its way up.
        }
    }

    public GlueClient glueClient() {
        return glueClient;
    }

    public LakeFormationClient lakeFormationClient() {
        return lakeFormationClient;
    }

    @Override
    public void close() {
        // Closed in reverse order of construction, and each independently: one failing close must not
        // leak the other two.
        try {
            lakeFormationClient.close();
        } finally {
            try {
                glueClient.close();
            } finally {
                stsClient.close();
            }
        }
    }
}
