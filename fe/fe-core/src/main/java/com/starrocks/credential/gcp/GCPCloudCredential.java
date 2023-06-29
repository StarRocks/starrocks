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
import com.starrocks.credential.CloudCredential;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class GCPCloudCredential implements CloudCredential {

    private final boolean useComputeEngineServiceAccount;
    private final String serviceAccountEmail;
    private final String serviceAccountPrivateKeyId;
    private final String serviceAccountPrivateKey;
    private final String impersonationServiceAccount;

    private final Map<String, String> hadoopConfiguration;

    public GCPCloudCredential(boolean useComputeEngineServiceAccount, String serviceAccountEmail,
                              String serviceAccountPrivateKeyId, String serviceAccountPrivateKey,
                              String impersonationServiceAccount) {
        Preconditions.checkNotNull(serviceAccountEmail);
        Preconditions.checkNotNull(serviceAccountPrivateKeyId);
        Preconditions.checkNotNull(serviceAccountPrivateKey);
        Preconditions.checkNotNull(impersonationServiceAccount);
        this.useComputeEngineServiceAccount = useComputeEngineServiceAccount;
        this.serviceAccountEmail = serviceAccountEmail;
        this.serviceAccountPrivateKeyId = serviceAccountPrivateKeyId;
        this.serviceAccountPrivateKey = serviceAccountPrivateKey;
        this.impersonationServiceAccount = impersonationServiceAccount;
        hadoopConfiguration = new HashMap<>();
        tryGenerateHadoopConfiguration(hadoopConfiguration);
    }

    private void tryGenerateHadoopConfiguration(Map<String, String> hadoopConfiguration) {
        if (useComputeEngineServiceAccount) {
            hadoopConfiguration.put("fs.gs.auth.type", "COMPUTE_ENGINE");
        } else if (!serviceAccountEmail.isEmpty() && !serviceAccountPrivateKeyId.isEmpty() &&
                !serviceAccountPrivateKey.isEmpty()) {
            hadoopConfiguration.put("fs.gs.auth.service.account.email", serviceAccountEmail);
            hadoopConfiguration.put("fs.gs.auth.service.account.private.key.id", serviceAccountPrivateKeyId);
            hadoopConfiguration.put("fs.gs.auth.service.account.private.key", serviceAccountPrivateKey);
        }
        if (!impersonationServiceAccount.isEmpty()) {
            hadoopConfiguration.put("fs.gs.auth.impersonation.service.account", impersonationServiceAccount);
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
        if (!serviceAccountEmail.isEmpty() && !serviceAccountPrivateKeyId.isEmpty()
                && !serviceAccountPrivateKey.isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    public void toThrift(Map<String, String> properties) {
        properties.putAll(hadoopConfiguration);
    }

    @Override
    public String getCredentialString() {
        return "GCPCloudCredential{" +
                "useComputeEngineServiceAccount=" + useComputeEngineServiceAccount +
                ", serviceAccountEmail='" + serviceAccountEmail + '\'' +
                ", serviceAccountPrivateKeyId='" + serviceAccountPrivateKeyId + '\'' +
                ", serviceAccountPrivateKey='" + serviceAccountPrivateKey + '\'' +
                ", impersonationServiceAccount='" + impersonationServiceAccount + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        // TODO: Support gcp credential
        return null;
    }
}
