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

package com.starrocks.connector.delta.unity;

import com.google.common.base.Strings;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_ENDPOINT;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_SAS_TOKEN;

/**
 * Translates the JSON returned from Databricks Unity Catalog's
 * {@code POST /api/2.1/unity-catalog/temporary-table-credentials} endpoint into a StarRocks
 * {@link CloudConfiguration} by reusing
 * {@link CloudConfigurationFactory#buildCloudConfigurationForVendedCredentials}.
 *
 * <p>The UC response carries a top-level envelope ({@code aws_temp_credentials} /
 * {@code azure_user_delegation_sas} / {@code gcp_oauth_token}); we flatten the fields we care
 * about into the Iceberg-style property keys that the factory already understands, and the
 * factory picks the right cloud provider.</p>
 *
 * <p>v1 supports AWS S3 and Azure ADLS Gen2 only.</p>
 */
public final class UnityCatalogCredentialTranslator {
    private static final Logger LOG = LogManager.getLogger(UnityCatalogCredentialTranslator.class);

    // Iceberg's S3FileIOProperties / AwsClientProperties key names, reused because
    // CloudConfigurationFactory.buildCloudConfigurationForAWSVendedCredentials already
    // understands them.
    private static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
    private static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
    private static final String S3_SESSION_TOKEN = "s3.session-token";
    private static final String CLIENT_REGION = "client.region";

    private UnityCatalogCredentialTranslator() {
    }

    /**
     * Build a {@link CloudConfiguration} from a UC temporary-credentials response.
     *
     * @param creds        the parsed UC response
     * @param tableLocation the table's {@code storage_location} (e.g. {@code s3://bucket/path} or
     *                      {@code abfss://container@account.dfs.core.windows.net/path}); required
     *                      for Azure so we can derive the storage account endpoint.
     * @return a non-null {@link CloudConfiguration}; may be a {@code DEFAULT} one if no supported
     * credential shape was present (in which case the caller should fall back to the
     * catalog-level config).
     */
    public static CloudConfiguration toCloudConfiguration(UnityCatalogTypes.TemporaryTableCredentials creds,
                                                          String tableLocation) {
        return toCloudConfiguration(creds, tableLocation, null);
    }

    /**
     * Overload that also accepts an optional AWS region. UC's temporary-credentials API does not
     * return a region, but the BE's AWS C++ SDK defaults to {@code us-east-1} and does not
     * follow cross-region 301 redirects, so callers that know the bucket region (e.g. from a
     * catalog-level property) should pass it here; it is serialized into the resulting
     * {@link CloudConfiguration} and ultimately onto the scan range delivered to the BE.
     */
    public static CloudConfiguration toCloudConfiguration(UnityCatalogTypes.TemporaryTableCredentials creds,
                                                          String tableLocation,
                                                          String awsRegion) {
        if (creds == null) {
            return CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        }

        if (creds.awsTempCredentials != null
                && !Strings.isNullOrEmpty(creds.awsTempCredentials.accessKeyId)
                && !Strings.isNullOrEmpty(creds.awsTempCredentials.secretAccessKey)
                && !Strings.isNullOrEmpty(creds.awsTempCredentials.sessionToken)) {
            Map<String, String> props = new HashMap<>();
            props.put(S3_ACCESS_KEY_ID, creds.awsTempCredentials.accessKeyId);
            props.put(S3_SECRET_ACCESS_KEY, creds.awsTempCredentials.secretAccessKey);
            props.put(S3_SESSION_TOKEN, creds.awsTempCredentials.sessionToken);
            if (!Strings.isNullOrEmpty(awsRegion)) {
                props.put(CLIENT_REGION, awsRegion);
            }
            CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForVendedCredentials(props,
                    tableLocation);
            if (cc.getCloudType() != CloudType.DEFAULT) {
                return cc;
            }
            LOG.warn("Unity Catalog returned AWS credentials but CloudConfigurationFactory could not " +
                    "build a cloud configuration from them (tableLocation={})", tableLocation);
        }

        if (creds.azureUserDelegationSas != null
                && !Strings.isNullOrEmpty(creds.azureUserDelegationSas.sasToken)) {
            String endpoint = extractAdlsEndpoint(tableLocation);
            if (endpoint != null) {
                Map<String, String> props = new HashMap<>();
                props.put(ADLS_SAS_TOKEN + endpoint, creds.azureUserDelegationSas.sasToken);
                CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForVendedCredentials(props,
                        tableLocation);
                if (cc.getCloudType() != CloudType.DEFAULT) {
                    return cc;
                }
            } else {
                LOG.warn("Unity Catalog returned an Azure SAS token but the table location '{}' does not " +
                        "expose an ADLS Gen2 endpoint; falling back to catalog-level credentials", tableLocation);
            }
        }

        if (creds.gcpOauthToken != null && !Strings.isNullOrEmpty(creds.gcpOauthToken.oauthToken)) {
            LOG.info("Unity Catalog returned GCP OAuth token but GCS is not supported in v1; falling back " +
                    "to catalog-level credentials");
        }

        return CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
    }

    /**
     * Given a table URI like {@code abfss://container@account.dfs.core.windows.net/path} return
     * the endpoint piece {@code account.dfs.core.windows.net} suitable for appending to
     * {@link com.starrocks.credential.azure.AzureCloudConfigurationProvider#ADLS_SAS_TOKEN}.
     * Returns {@code null} if the scheme is not ADLS Gen2.
     */
    static String extractAdlsEndpoint(String tableLocation) {
        if (Strings.isNullOrEmpty(tableLocation)) {
            return null;
        }
        String trimmed = tableLocation.trim();
        String lower = trimmed.toLowerCase();
        if (!(lower.startsWith("abfs://") || lower.startsWith("abfss://"))) {
            return null;
        }
        try {
            URI uri = new URI(trimmed);
            String authority = uri.getRawAuthority();
            if (Strings.isNullOrEmpty(authority)) {
                return null;
            }
            int at = authority.indexOf('@');
            String hostPart = at >= 0 ? authority.substring(at + 1) : authority;
            if (!hostPart.endsWith(ADLS_ENDPOINT)) {
                return null;
            }
            return hostPart;
        } catch (URISyntaxException e) {
            LOG.warn("Unparseable ADLS table location '{}': {}", tableLocation, e.getMessage());
            return null;
        }
    }
}
