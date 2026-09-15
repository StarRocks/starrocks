/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.EncryptionAlgorithmSpec;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class AwsProperties implements Serializable {

  /**
   * The ID of the Glue Data Catalog where the tables reside. If none is provided, Glue
   * automatically uses the caller's AWS account ID by default.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-databases.html
   */
  public static final String GLUE_CATALOG_ID = "glue.id";

  /**
   * The account ID used in a Glue resource ARN, e.g.
   * arn:aws:glue:us-east-1:1000000000000:table/db1/table1
   */
  public static final String GLUE_ACCOUNT_ID = "glue.account-id";

  /**
   * If Glue should skip archiving an old table version when creating a new version in a commit. By
   * default Glue archives all old table versions after an UpdateTable call, but Glue has a default
   * max number of archived table versions (can be increased). So for streaming use case with lots
   * of commits, it is recommended to set this value to true.
   */
  public static final String GLUE_CATALOG_SKIP_ARCHIVE = "glue.skip-archive";

  public static final boolean GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT = true;

  /**
   * If Glue should skip name validations It is recommended to stick to Glue best practice in
   * https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html to make sure operations
   * are Hive compatible. This is only added for users that have existing conventions using
   * non-standard characters. When database name and table name validation are skipped, there is no
   * guarantee that downstream systems would all support the names.
   */
  public static final String GLUE_CATALOG_SKIP_NAME_VALIDATION = "glue.skip-name-validation";

  public static final boolean GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT = false;

  /**
   * If set, GlueCatalog will use Lake Formation for access control. For more credential vending
   * details, see: https://docs.aws.amazon.com/lake-formation/latest/dg/api-overview.html. If
   * enabled, the {@link AwsClientFactory} implementation must be {@link
   * LakeFormationAwsClientFactory} or any class that extends it.
   */
  public static final String GLUE_LAKEFORMATION_ENABLED = "glue.lakeformation-enabled";

  public static final boolean GLUE_LAKEFORMATION_ENABLED_DEFAULT = false;

  /**
   * Configure an alternative endpoint of the Glue service for GlueCatalog to access.
   *
   * <p>This could be used to use GlueCatalog with any glue-compatible metastore service that has a
   * different endpoint
   */
  public static final String GLUE_CATALOG_ENDPOINT = "glue.endpoint";

  /** Configure an alternative endpoint of the DynamoDB service to access. */
  public static final String DYNAMODB_ENDPOINT = "dynamodb.endpoint";

  /** DynamoDB table name for {@link DynamoDbCatalog} */
  public static final String DYNAMODB_TABLE_NAME = "dynamodb.table-name";

  public static final String DYNAMODB_TABLE_NAME_DEFAULT = "iceberg";

  /**
   * The implementation class of {@link AwsClientFactory} to customize AWS client configurations. If
   * set, all AWS clients will be initialized by the specified factory. If not set, {@link
   * AwsClientFactories#defaultFactory()} is used as default factory.
   */
  public static final String CLIENT_FACTORY = "client.factory";

  /**
   * Used by {@link AssumeRoleAwsClientFactory}. If set, all AWS clients will assume a role of the
   * given ARN, instead of using the default credential chain.
   */
  public static final String CLIENT_ASSUME_ROLE_ARN = "client.assume-role.arn";

  /**
   * Used by {@link AssumeRoleAwsClientFactory} to pass a list of sessions. Each session tag
   * consists of a key name and an associated value.
   */
  public static final String CLIENT_ASSUME_ROLE_TAGS_PREFIX = "client.assume-role.tags.";

  /**
   * Used by {@link AssumeRoleAwsClientFactory}. The timeout of the assume role session in seconds,
   * default to 1 hour. At the end of the timeout, a new set of role session credentials will be
   * fetched through a STS client.
   */
  public static final String CLIENT_ASSUME_ROLE_TIMEOUT_SEC = "client.assume-role.timeout-sec";

  public static final int CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT = 3600;

  /**
   * Used by {@link AssumeRoleAwsClientFactory}. Optional external ID used to assume an IAM role.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
   */
  public static final String CLIENT_ASSUME_ROLE_EXTERNAL_ID = "client.assume-role.external-id";

  /**
   * Used by {@link AssumeRoleAwsClientFactory}. If set, all AWS clients except STS client will use
   * the given region instead of the default region chain.
   *
   * <p>The value must be one of {@link software.amazon.awssdk.regions.Region}, such as 'us-east-1'.
   * For more details, see https://docs.aws.amazon.com/general/latest/gr/rande.html
   */
  public static final String CLIENT_ASSUME_ROLE_REGION = "client.assume-role.region";

  /**
   * Used by {@link AssumeRoleAwsClientFactory}. Optional session name used to assume an IAM role.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_iam-condition-keys.html#ck_rolesessionname
   */
  public static final String CLIENT_ASSUME_ROLE_SESSION_NAME = "client.assume-role.session-name";

  /**
   * Used by {@link LakeFormationAwsClientFactory}. The table name used as part of lake formation
   * credentials request.
   */
  public static final String LAKE_FORMATION_TABLE_NAME = "lakeformation.table-name";

  /**
   * Used by {@link LakeFormationAwsClientFactory}. The database name used as part of lake formation
   * credentials request.
   */
  public static final String LAKE_FORMATION_DB_NAME = "lakeformation.db-name";

  /** Region to be used by the SigV4 protocol for signing requests. */
  public static final String REST_SIGNER_REGION = "rest.signing-region";

  /** The service name to be used by the SigV4 protocol for signing requests. */
  public static final String REST_SIGNING_NAME = "rest.signing-name";

  /** The default service name (API Gateway and lambda) used during SigV4 signing. */
  public static final String REST_SIGNING_NAME_DEFAULT = "execute-api";

  /**
   * Configure the static access key ID used for SigV4 signing.
   *
   * <p>When set, the default client factory will use the basic or session credentials provided
   * instead of reading the default credential chain to create S3 access credentials. If {@link
   * #REST_SESSION_TOKEN} is set, session credential is used, otherwise basic credential is used.
   */
  public static final String REST_ACCESS_KEY_ID = "rest.access-key-id";

  /**
   * Configure the static secret access key used for SigV4 signing.
   *
   * <p>When set, the default client factory will use the basic or session credentials provided
   * instead of reading the default credential chain to create S3 access credentials. If {@link
   * #REST_SESSION_TOKEN} is set, session credential is used, otherwise basic credential is used.
   */
  public static final String REST_SECRET_ACCESS_KEY = "rest.secret-access-key";

  /**
   * Configure the static session token used for SigV4.
   *
   * <p>When set, the default client factory will use the session credentials provided instead of
   * reading the default credential chain to create access credentials.
   */
  public static final String REST_SESSION_TOKEN = "rest.session-token";

  /** Encryption algorithm used to encrypt/decrypt master table keys */
  public static final String KMS_ENCRYPTION_ALGORITHM_SPEC = "kms.encryption-algorithm-spec";

  public static final EncryptionAlgorithmSpec KMS_ENCRYPTION_ALGORITHM_SPEC_DEFAULT =
      EncryptionAlgorithmSpec.SYMMETRIC_DEFAULT;

  /** Length of data key generated by KMS */
  public static final String KMS_DATA_KEY_SPEC = "kms.data-key-spec";

  public static final DataKeySpec KMS_DATA_KEY_SPEC_DEFAULT = DataKeySpec.AES_256;

  private final Set<software.amazon.awssdk.services.sts.model.Tag> stsClientAssumeRoleTags;

  private final String clientAssumeRoleArn;
  private final String clientAssumeRoleExternalId;
  private final int clientAssumeRoleTimeoutSec;
  private final String clientAssumeRoleRegion;
  private final String clientAssumeRoleSessionName;
  private final String clientCredentialsProvider;
  private final Map<String, String> clientCredentialsProviderProperties;

  private final String glueEndpoint;
  private String glueCatalogId;
  private boolean glueCatalogSkipArchive;
  private boolean glueCatalogSkipNameValidation;
  private boolean glueLakeFormationEnabled;

  private String dynamoDbTableName;
  private final String dynamoDbEndpoint;

  private String restSigningRegion;
  private final String restSigningName;
  private String restAccessKeyId;
  private String restSecretAccessKey;
  private String restSessionToken;
  private EncryptionAlgorithmSpec kmsEncryptionAlgorithmSpec;
  private DataKeySpec kmsDataKeySpec;

  public AwsProperties() {
    this.stsClientAssumeRoleTags = Sets.newHashSet();

    this.clientAssumeRoleArn = null;
    this.clientAssumeRoleTimeoutSec = CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT;
    this.clientAssumeRoleExternalId = null;
    this.clientAssumeRoleRegion = null;
    this.clientAssumeRoleSessionName = null;
    this.clientCredentialsProvider = null;
    this.clientCredentialsProviderProperties = null;

    this.glueCatalogId = null;
    this.glueEndpoint = null;
    this.glueCatalogSkipArchive = GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT;
    this.glueCatalogSkipNameValidation = GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT;
    this.glueLakeFormationEnabled = GLUE_LAKEFORMATION_ENABLED_DEFAULT;

    this.dynamoDbEndpoint = null;
    this.dynamoDbTableName = DYNAMODB_TABLE_NAME_DEFAULT;

    this.restSigningName = REST_SIGNING_NAME_DEFAULT;

    this.kmsEncryptionAlgorithmSpec = KMS_ENCRYPTION_ALGORITHM_SPEC_DEFAULT;
    this.kmsDataKeySpec = KMS_DATA_KEY_SPEC_DEFAULT;
  }

  @SuppressWarnings("MethodLength")
  public AwsProperties(Map<String, String> properties) {
    this.stsClientAssumeRoleTags = toStsTags(properties, CLIENT_ASSUME_ROLE_TAGS_PREFIX);
    this.clientAssumeRoleArn = properties.get(CLIENT_ASSUME_ROLE_ARN);
    this.clientAssumeRoleTimeoutSec =
        PropertyUtil.propertyAsInt(
            properties, CLIENT_ASSUME_ROLE_TIMEOUT_SEC, CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT);
    this.clientAssumeRoleExternalId = properties.get(CLIENT_ASSUME_ROLE_EXTERNAL_ID);
    this.clientAssumeRoleRegion = properties.get(CLIENT_ASSUME_ROLE_REGION);
    this.clientAssumeRoleSessionName = properties.get(CLIENT_ASSUME_ROLE_SESSION_NAME);
    this.clientCredentialsProvider =
        properties.get(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER);
    this.clientCredentialsProviderProperties =
        PropertyUtil.propertiesWithPrefix(
            properties, AwsClientProperties.CLIENT_CREDENTIAL_PROVIDER_PREFIX);

    this.glueEndpoint = properties.get(GLUE_CATALOG_ENDPOINT);
    this.glueCatalogId = properties.get(GLUE_CATALOG_ID);
    this.glueCatalogSkipArchive =
        PropertyUtil.propertyAsBoolean(
            properties, GLUE_CATALOG_SKIP_ARCHIVE, GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT);
    this.glueCatalogSkipNameValidation =
        PropertyUtil.propertyAsBoolean(
            properties,
            GLUE_CATALOG_SKIP_NAME_VALIDATION,
            GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT);
    this.glueLakeFormationEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, GLUE_LAKEFORMATION_ENABLED, GLUE_LAKEFORMATION_ENABLED_DEFAULT);

    this.dynamoDbEndpoint = properties.get(DYNAMODB_ENDPOINT);
    this.dynamoDbTableName =
        PropertyUtil.propertyAsString(properties, DYNAMODB_TABLE_NAME, DYNAMODB_TABLE_NAME_DEFAULT);

    this.restSigningRegion = properties.get(REST_SIGNER_REGION);
    this.restSigningName = properties.getOrDefault(REST_SIGNING_NAME, REST_SIGNING_NAME_DEFAULT);
    this.restAccessKeyId = properties.get(REST_ACCESS_KEY_ID);
    this.restSecretAccessKey = properties.get(REST_SECRET_ACCESS_KEY);
    this.restSessionToken = properties.get(REST_SESSION_TOKEN);

    this.kmsEncryptionAlgorithmSpec =
        EncryptionAlgorithmSpec.fromValue(
            properties.getOrDefault(
                KMS_ENCRYPTION_ALGORITHM_SPEC, KMS_ENCRYPTION_ALGORITHM_SPEC_DEFAULT.toString()));
    this.kmsDataKeySpec =
        DataKeySpec.fromValue(
            properties.getOrDefault(KMS_DATA_KEY_SPEC, KMS_DATA_KEY_SPEC_DEFAULT.toString()));
  }

  public Set<software.amazon.awssdk.services.sts.model.Tag> stsClientAssumeRoleTags() {
    return stsClientAssumeRoleTags;
  }

  public String clientAssumeRoleArn() {
    return clientAssumeRoleArn;
  }

  public int clientAssumeRoleTimeoutSec() {
    return clientAssumeRoleTimeoutSec;
  }

  public String clientAssumeRoleExternalId() {
    return clientAssumeRoleExternalId;
  }

  public String clientAssumeRoleRegion() {
    return clientAssumeRoleRegion;
  }

  public String clientAssumeRoleSessionName() {
    return clientAssumeRoleSessionName;
  }

  public String glueCatalogId() {
    return glueCatalogId;
  }

  public void setGlueCatalogId(String id) {
    this.glueCatalogId = id;
  }

  public boolean glueCatalogSkipArchive() {
    return glueCatalogSkipArchive;
  }

  public void setGlueCatalogSkipArchive(boolean skipArchive) {
    this.glueCatalogSkipArchive = skipArchive;
  }

  public boolean glueCatalogSkipNameValidation() {
    return glueCatalogSkipNameValidation;
  }

  public void setGlueCatalogSkipNameValidation(boolean glueCatalogSkipNameValidation) {
    this.glueCatalogSkipNameValidation = glueCatalogSkipNameValidation;
  }

  public boolean glueLakeFormationEnabled() {
    return glueLakeFormationEnabled;
  }

  public void setGlueLakeFormationEnabled(boolean glueLakeFormationEnabled) {
    this.glueLakeFormationEnabled = glueLakeFormationEnabled;
  }

  public String dynamoDbTableName() {
    return dynamoDbTableName;
  }

  public void setDynamoDbTableName(String name) {
    this.dynamoDbTableName = name;
  }

  /**
   * Override the endpoint for a glue client.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     GlueClient.builder().applyMutation(awsProperties::applyS3EndpointConfigurations)
   * </pre>
   */
  public <T extends GlueClientBuilder> void applyGlueEndpointConfigurations(T builder) {
    configureEndpoint(builder, glueEndpoint);
  }

  /**
   * Override the endpoint for a dynamoDb client.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     DynamoDbClient.builder().applyMutation(awsProperties::applyDynamoDbEndpointConfigurations)
   * </pre>
   */
  public <T extends DynamoDbClientBuilder> void applyDynamoDbEndpointConfigurations(T builder) {
    configureEndpoint(builder, dynamoDbEndpoint);
  }

  public Region restSigningRegion() {
    if (restSigningRegion == null) {
      this.restSigningRegion = DefaultAwsRegionProviderChain.builder().build().getRegion().id();
    }

    return Region.of(restSigningRegion);
  }

  public String restSigningName() {
    return restSigningName;
  }

  /**
   * Returns an {@link AwsCredentialsProvider} for signing REST catalog requests.
   *
   * <p>When {@code client.assume-role.arn} is configured, wraps the base credentials with an
   * auto-refreshing {@link StsAssumeRoleCredentialsProvider} so that both REST API signing and S3
   * data access use the same assumed role.
   */
  public AwsCredentialsProvider restCredentialsProvider() {
    AwsCredentialsProvider base =
        credentialsProvider(this.restAccessKeyId, this.restSecretAccessKey, this.restSessionToken);
    if (clientAssumeRoleArn != null) {
      StsClient stsClient =
          StsClient.builder().credentialsProvider(base).region(restSigningRegion()).build();
      return StsAssumeRoleCredentialsProvider.builder()
          .stsClient(stsClient)
          .refreshRequest(
              AssumeRoleRequest.builder()
                  .roleArn(clientAssumeRoleArn)
                  .roleSessionName(
                      clientAssumeRoleSessionName != null
                          ? clientAssumeRoleSessionName
                          : UUID.randomUUID().toString())
                  .externalId(clientAssumeRoleExternalId)
                  .durationSeconds(clientAssumeRoleTimeoutSec)
                  .tags(stsClientAssumeRoleTags)
                  .build())
          .build();
    }
    return base;
  }

  public EncryptionAlgorithmSpec kmsEncryptionAlgorithmSpec() {
    return this.kmsEncryptionAlgorithmSpec;
  }

  public DataKeySpec kmsDataKeySpec() {
    return this.kmsDataKeySpec;
  }

  private Set<software.amazon.awssdk.services.sts.model.Tag> toStsTags(
      Map<String, String> properties, String prefix) {
    return PropertyUtil.propertiesWithPrefix(properties, prefix).entrySet().stream()
        .map(
            e ->
                software.amazon.awssdk.services.sts.model.Tag.builder()
                    .key(e.getKey())
                    .value(e.getValue())
                    .build())
        .collect(Collectors.toSet());
  }

  private AwsCredentialsProvider credentialsProvider(
      String accessKeyId, String secretAccessKey, String sessionToken) {
    if (accessKeyId != null) {
      if (sessionToken == null) {
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKeyId, secretAccessKey));
      } else {
        return StaticCredentialsProvider.create(
            AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
      }
    }

    if (!Strings.isNullOrEmpty(this.clientCredentialsProvider)) {
      return credentialsProvider(this.clientCredentialsProvider);
    }

    // Create a new credential provider for each client
    return DefaultCredentialsProvider.builder().build();
  }

  private AwsCredentialsProvider credentialsProvider(String credentialsProviderClass) {
    Class<?> providerClass;
    try {
      providerClass = DynClasses.builder().impl(credentialsProviderClass).buildChecked();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot load class %s, it does not exist in the classpath", credentialsProviderClass),
          e);
    }

    Preconditions.checkArgument(
        AwsCredentialsProvider.class.isAssignableFrom(providerClass),
        String.format(
            "Cannot initialize %s, it does not implement %s.",
            credentialsProviderClass, AwsCredentialsProvider.class.getName()));

    AwsCredentialsProvider provider;
    try {
      try {
        provider =
            DynMethods.builder("create")
                .hiddenImpl(providerClass, Map.class)
                .buildStaticChecked()
                .invoke(clientCredentialsProviderProperties);
      } catch (NoSuchMethodException e) {
        provider =
            DynMethods.builder("create").hiddenImpl(providerClass).buildStaticChecked().invoke();
      }

      return provider;
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot create an instance of %s, it does not contain a static 'create' or 'create(Map<String, String>)' method",
              credentialsProviderClass),
          e);
    }
  }

  private <T extends SdkClientBuilder> void configureEndpoint(T builder, String endpoint) {
    if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
    }
  }
}
