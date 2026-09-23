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
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one place vended credentials become a CloudConfiguration.
 *
 * <p>Two properties are worth a test each. The configuration is built from nothing rather than by copying
 * the catalog's own properties, because anything in there that selects a credentials provider would win over
 * the static keys and the scan would quietly run as the cluster - the exact failure Lake Formation is
 * deployed to prevent. And the result is checked for being AWS, because the factory answers a validation
 * failure with a DEFAULT-typed object rather than with null, which every caller that only tests for null
 * would take for a working configuration.
 */
public class LakeFormationCredentialsTest {

    private static final String ACCESS_KEY = "AKIAEXAMPLETESTKEY";
    private static final String SECRET_KEY = "wJalrXUtnFEMIsecretKEYtestVALUE";
    private static final String SESSION_TOKEN = "FwoGZXIvYXdzTESTsessionTOKENvalue";

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", null, "us-west-2", "db", "t");

    private static LakeFormationCatalogProperties properties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("hive.metastore.type", "glue");
        raw.put("catalog.access.control", "lakeformation");
        raw.put("aws.lakeformation.session_tag_value", "starrocks");
        raw.put("aws.glue.region", "us-west-2");
        return LakeFormationCatalogProperties.from("hive", raw);
    }

    private static LakeFormationTableAccess access() {
        return LakeFormationTableAccess.from(IDENTITY, "auth-id",
                GetTemporaryGlueTableCredentialsResponse.builder()
                        .accessKeyId(ACCESS_KEY)
                        .secretAccessKey(SECRET_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .expiration(Instant.now().plus(1, ChronoUnit.HOURS))
                        .build());
    }

    /** Captures what the factory was handed, and answers with a configuration of the given cloud type. */
    private static void factoryCaptures(Map<String, String> captured, CloudConfiguration answer) {
        new MockUp<CloudConfigurationFactory>() {
            @Mock
            public CloudConfiguration buildCloudConfigurationForStorage(Map<String, String> properties) {
                captured.putAll(properties);
                return answer;
            }
        };
    }

    /**
     * Blank is refused as firmly as null. An empty access key reaches AwsCloudCredential as "nothing
     * configured", which falls back to the node's own identity instead of failing.
     */
    @Test
    public void testBlankVendedKeysAreRefused() {
        for (GetTemporaryGlueTableCredentialsResponse blank : List.of(
                vended().accessKeyId("").build(),
                vended().secretAccessKey("   ").build(),
                vended().sessionToken("").build())) {
            LakeFormationTableAccessException failure = assertThrows(LakeFormationTableAccessException.class,
                    () -> LakeFormationTableAccess.from(IDENTITY, "auth-id", blank));
            assertTrue(failure.getMessage().contains("incomplete credentials"), failure.getMessage());
        }
    }

    private static GetTemporaryGlueTableCredentialsResponse.Builder vended() {
        return GetTemporaryGlueTableCredentialsResponse.builder()
                .accessKeyId(ACCESS_KEY)
                .secretAccessKey(SECRET_KEY)
                .sessionToken(SESSION_TOKEN)
                .expiration(Instant.now().plus(1, ChronoUnit.HOURS));
    }

    @Test
    public void testTheVendedKeysBecomeAnAwsConfiguration() {
        CloudConfiguration configuration =
                LakeFormationCredentials.toCloudConfiguration(access(), properties(), Map.of());

        assertNotNull(configuration);
        assertEquals(CloudType.AWS, configuration.getCloudType());
    }

    /**
     * The endpoint and path-style options say where the bucket is, not who is asking, so they have to
     * survive. Without them a catalog pointed at a non-default endpoint would stop working the moment Lake
     * Formation is switched on.
     */
    @Test
    public void testTheAddressingOptionsSurvive(@Mocked CloudConfiguration aws) {
        new Expectations() {
            {
                aws.getCloudType();
                result = CloudType.AWS;
                minTimes = 0;
            }
        };
        Map<String, String> captured = new HashMap<>();
        factoryCaptures(captured, aws);

        LakeFormationCredentials.toCloudConfiguration(access(), properties(),
                Map.of(CloudConfigurationConstants.AWS_S3_ENDPOINT, "https://s3.example.internal",
                        CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS, "true"));

        assertEquals("https://s3.example.internal",
                captured.get(CloudConfigurationConstants.AWS_S3_ENDPOINT));
        assertEquals("true", captured.get(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS));
    }

    /**
     * The bucket's region is addressing too. It defaults to the Data Catalog's, which is right whenever the
     * two sit together, but a catalog that names one is saying where its data actually is.
     */
    @Test
    public void testAnExplicitBucketRegionWinsOverTheDataCatalogs(@Mocked CloudConfiguration aws) {
        new Expectations() {
            {
                aws.getCloudType();
                result = CloudType.AWS;
                minTimes = 0;
            }
        };
        Map<String, String> captured = new HashMap<>();
        factoryCaptures(captured, aws);

        LakeFormationCredentials.toCloudConfiguration(access(), properties(),
                Map.of(CloudConfigurationConstants.AWS_S3_REGION, "eu-central-1"));

        assertEquals("eu-central-1", captured.get(CloudConfigurationConstants.AWS_S3_REGION),
                "signing in the Data Catalog's region fails on a bucket that is not in it");
    }

    /**
     * Everything else the catalog carries is left behind on purpose - most of all anything that names an
     * instance profile or a role, which would be preferred over the static keys built here.
     */
    @Test
    public void testNothingElseFromTheCatalogIsCarriedAlong(@Mocked CloudConfiguration aws) {
        new Expectations() {
            {
                aws.getCloudType();
                result = CloudType.AWS;
                minTimes = 0;
            }
        };
        Map<String, String> captured = new HashMap<>();
        factoryCaptures(captured, aws);

        LakeFormationCredentials.toCloudConfiguration(access(), properties(),
                Map.of("aws.s3.use_instance_profile", "true",
                        "aws.s3.iam_role_arn", "arn:aws:iam::1:role/cluster",
                        CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "AKIACLUSTEROWNKEY"));

        assertFalse(captured.containsKey("aws.s3.use_instance_profile"));
        assertFalse(captured.containsKey("aws.s3.iam_role_arn"));
        assertEquals(ACCESS_KEY, captured.get(CloudConfigurationConstants.AWS_S3_ACCESS_KEY),
                "the vended key wins; the catalog's own must not overwrite it");
        assertEquals(SECRET_KEY, captured.get(CloudConfigurationConstants.AWS_S3_SECRET_KEY));
        assertEquals(SESSION_TOKEN, captured.get(CloudConfigurationConstants.AWS_S3_SESSION_TOKEN));
        assertEquals("us-west-2", captured.get(CloudConfigurationConstants.AWS_S3_REGION));
    }

    @Test
    public void testAnAbsentOrEmptyAddressingOptionIsNotCopied(@Mocked CloudConfiguration aws) {
        new Expectations() {
            {
                aws.getCloudType();
                result = CloudType.AWS;
                minTimes = 0;
            }
        };
        Map<String, String> captured = new HashMap<>();
        factoryCaptures(captured, aws);

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, "");

        LakeFormationCredentials.toCloudConfiguration(access(), properties(), catalogProperties);

        assertFalse(captured.containsKey(CloudConfigurationConstants.AWS_S3_ENDPOINT),
                "an empty value is not an endpoint");
        assertFalse(captured.containsKey(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS));
    }

    /** A catalog with no properties at all still converts; the copy step simply has nothing to do. */
    @Test
    public void testNullCatalogPropertiesAreTolerated() {
        CloudConfiguration configuration =
                LakeFormationCredentials.toCloudConfiguration(access(), properties(), null);

        assertEquals(CloudType.AWS, configuration.getCloudType());
    }

    /**
     * The factory ends its chain with a plain configuration whose type is DEFAULT, so a validation failure
     * arrives as a non-null object. Taking that for a working configuration would scan with whatever
     * ambient credentials the process happens to have.
     */
    @Test
    public void testANonAwsConfigurationIsRefusedRatherThanReturned(@Mocked CloudConfiguration notAws) {
        new Expectations() {
            {
                notAws.getCloudType();
                result = CloudType.DEFAULT;
                minTimes = 0;
            }
        };
        factoryCaptures(new HashMap<>(), notAws);

        LakeFormationTableAccessException refusal = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationCredentials.toCloudConfiguration(access(), properties(), Map.of()));

        assertTrue(refusal.getMessage().contains("did not produce an AWS cloud configuration"));
        assertTrue(refusal.getMessage().contains("db"), "the refusal names the table it was vended for");
    }

    @Test
    public void testANullConfigurationIsRefusedToo() {
        factoryCaptures(new HashMap<>(), null);

        assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationCredentials.toCloudConfiguration(access(), properties(), Map.of()));
    }
}
