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

public class GSCredential {
    private boolean useComputeEngineServiceAccount;
    private String serviceAccountEmail;
    private String serviceAccountPrivateKeyId;
    private String serviceAccountPrivateKey;
    private String impersonation;

    public GSCredential(boolean useComputeEngineServiceAccount, String serviceAccountEmail, String serviceAccountPrivateKeyId,
                        String serviceAccountPrivateKey, String impersonation) {
        this.useComputeEngineServiceAccount = useComputeEngineServiceAccount;
        this.serviceAccountEmail = serviceAccountEmail;
        this.serviceAccountPrivateKeyId = serviceAccountPrivateKeyId;
        this.serviceAccountPrivateKey = serviceAccountPrivateKey;
        this.impersonation = impersonation;
    }

    public boolean isUseComputeEngineServiceAccount() {
        return useComputeEngineServiceAccount;
    }

    public String getServiceAccountEmail() {
        return serviceAccountEmail;
    }

    public String getServiceAccountPrivateKeyId() {
        return serviceAccountPrivateKeyId;
    }

    public String getServiceAccountPrivateKey() {
        return serviceAccountPrivateKey;
    }

    public String getImpersonation() {
        return impersonation;
    }

    public boolean isEmpty() {
        return false;
    }
}
