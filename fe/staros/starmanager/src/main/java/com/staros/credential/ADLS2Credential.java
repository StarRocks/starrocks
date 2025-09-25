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

package com.staros.credential;

import com.staros.proto.ADLS2CredentialInfo;

public class ADLS2Credential {
    private String sharedKey;
    private String sasToken;
    private String tenantId;
    private String clientId;
    private String clientSecret;
    private String clientCertificatePath;
    private String authorityHost;

    public ADLS2Credential(String sharedKey, String sasToken, String tenantId, String clientId, String clientSecret,
            String clientCertificatePath, String authorityHost) {
        this.sharedKey = sharedKey;
        this.sasToken = sasToken;
        this.tenantId = tenantId;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.clientCertificatePath = clientCertificatePath;
        this.authorityHost = authorityHost;
    }

    public ADLS2CredentialInfo toProtobuf() {
        ADLS2CredentialInfo.Builder builder = ADLS2CredentialInfo.newBuilder();
        return builder.setSharedKey(sharedKey)
                .setSasToken(sasToken)
                .setTenantId(tenantId)
                .setClientId(clientId)
                .setClientSecret(clientSecret)
                .setClientCertificatePath(clientCertificatePath)
                .setAuthorityHost(authorityHost)
                .build();
    }

    public boolean isEmpty() {
        return sharedKey.isEmpty() && sasToken.isEmpty() && tenantId.isEmpty() && clientId.isEmpty() &&
                clientSecret.isEmpty() && clientCertificatePath.isEmpty() && authorityHost.isEmpty();
    }

    public static ADLS2Credential fromProtobuf(ADLS2CredentialInfo credentialInfo) {
        String sharedKey = credentialInfo.getSharedKey();
        String sasToken = credentialInfo.getSasToken();
        String tenantId = credentialInfo.getTenantId();
        String clientId = credentialInfo.getClientId();
        String clientSecret = credentialInfo.getClientSecret();
        String clientCertificatePath = credentialInfo.getClientCertificatePath();
        String authorityHost = credentialInfo.getAuthorityHost();
        return new ADLS2Credential(sharedKey, sasToken, tenantId, clientId, clientSecret, clientCertificatePath,
                authorityHost);
    }
}
