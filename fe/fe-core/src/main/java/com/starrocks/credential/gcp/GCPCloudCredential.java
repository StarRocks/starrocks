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

package com.starrocks.credential.gcp;

import com.google.common.base.Preconditions;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.GSFileStoreInfo;
import com.starrocks.credential.CloudCredential;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.gcp.GCPCloudConfigurationProvider.ACCESS_TOKEN_PROVIDER_IMPL;

public class GCPCloudCredential implements CloudCredential {

    private final String endpoint;
    private final boolean useComputeEngineServiceAccount;
    private final String serviceAccountEmail;
    private final String serviceAccountPrivateKeyId;
    private final String serviceAccountPrivateKey;
    private final String impersonationServiceAccount;
    private final String accessToken;
    private final String accessTokenExpiresAt;

    private final Map<String, String> hadoopConfiguration;

    public GCPCloudCredential(String endpoint, boolean useComputeEngineServiceAccount, String serviceAccountEmail,
                              String serviceAccountPrivateKeyId, String serviceAccountPrivateKey,
                              String impersonationServiceAccount, String accessToken, String accessTokenExpiresAt) {
        Preconditions.checkNotNull(endpoint);
        Preconditions.checkNotNull(serviceAccountEmail);
        Preconditions.checkNotNull(serviceAccountPrivateKeyId);
        Preconditions.checkNotNull(serviceAccountPrivateKey);
        Preconditions.checkNotNull(impersonationServiceAccount);
        this.endpoint = endpoint;
        this.useComputeEngineServiceAccount = useComputeEngineServiceAccount;
        this.serviceAccountEmail = serviceAccountEmail;
        this.serviceAccountPrivateKeyId = serviceAccountPrivateKeyId;
        this.serviceAccountPrivateKey = serviceAccountPrivateKey;
        this.impersonationServiceAccount = impersonationServiceAccount;
        this.accessToken = accessToken;
        this.accessTokenExpiresAt = accessTokenExpiresAt;
        hadoopConfiguration = new HashMap<>();
        tryGenerateHadoopConfiguration(hadoopConfiguration);
    }

    public String getEndpoint() {
        return endpoint;
    }

    private boolean hasServiceAccountCredentials() {
        return !serviceAccountEmail.isEmpty() && !serviceAccountPrivateKeyId.isEmpty()
                && !serviceAccountPrivateKey.isEmpty();
    }

    private boolean hasAccessToken() {
        return accessToken != null && !accessToken.isEmpty();
    }

    private void tryGenerateHadoopConfiguration(Map<String, String> hadoopConfiguration) {
        if (!endpoint.isEmpty()) {
            hadoopConfiguration.put("fs.gs.endpoint", endpoint);
        }
        if (useComputeEngineServiceAccount) {
            hadoopConfiguration.put("fs.gs.auth.type", "COMPUTE_ENGINE");
        } else if (hasServiceAccountCredentials()) {
            hadoopConfiguration.put("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
            hadoopConfiguration.put("fs.gs.auth.service.account.email", serviceAccountEmail);
            hadoopConfiguration.put("fs.gs.auth.service.account.private.key.id", serviceAccountPrivateKeyId);
            hadoopConfiguration.put("fs.gs.auth.service.account.private.key", serviceAccountPrivateKey);
        }
        if (!impersonationServiceAccount.isEmpty()) {
            hadoopConfiguration.put("fs.gs.auth.impersonation.service.account", impersonationServiceAccount);
        }
        if (hasAccessToken()) {
            hadoopConfiguration.put("fs.gs.auth.access.token.provider.impl",
                    ACCESS_TOKEN_PROVIDER_IMPL);
            hadoopConfiguration.put(GCPCloudConfigurationProvider.ACCESS_TOKEN_KEY, accessToken);
            hadoopConfiguration.put(GCPCloudConfigurationProvider.TOKEN_EXPIRATION_KEY, accessTokenExpiresAt);
        }
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        for (Map.Entry<String, String> entry : hadoopConfiguration.entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean validate() {
        if (useComputeEngineServiceAccount) {
            return true;
        }
        if (hasServiceAccountCredentials()) {
            return true;
        }
        if (hasAccessToken()) {
            return true;
        }
        return false;
    }

    @Override
    public void toThrift(Map<String, String> properties) {
        properties.putAll(hadoopConfiguration);
    }

    @Override
    public String toCredString() {
        return "GCPCloudCredential{" +
                "endpoint='" + endpoint + '\'' +
                ", useComputeEngineServiceAccount=" + useComputeEngineServiceAccount +
                ", serviceAccountEmail='" + serviceAccountEmail + '\'' +
                ", serviceAccountPrivateKeyId='" + serviceAccountPrivateKeyId + '\'' +
                ", serviceAccountPrivateKey='" + serviceAccountPrivateKey + '\'' +
                ", impersonationServiceAccount='" + impersonationServiceAccount + '\'' +
                ", accessToken='" + accessToken + '\'' +
                ", accessTokenExpiresAt='" + accessTokenExpiresAt + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        FileStoreInfo.Builder fsb = FileStoreInfo.newBuilder();
        fsb.setFsType(FileStoreType.GS);
        GSFileStoreInfo.Builder gsFileStoreInfo = GSFileStoreInfo.newBuilder();
        gsFileStoreInfo.setEndpoint(endpoint);

        gsFileStoreInfo.setUseComputeEngineServiceAccount(useComputeEngineServiceAccount);
        if (hasServiceAccountCredentials()) {
            gsFileStoreInfo.setServiceAccountEmail(serviceAccountEmail);
            gsFileStoreInfo.setServiceAccountPrivateKeyId(serviceAccountPrivateKeyId);
            gsFileStoreInfo.setServiceAccountPrivateKey(serviceAccountPrivateKey);
        }
        gsFileStoreInfo.setImpersonation(impersonationServiceAccount);
        fsb.setGsFsInfo(gsFileStoreInfo);
        return fsb.build();
    }
}
