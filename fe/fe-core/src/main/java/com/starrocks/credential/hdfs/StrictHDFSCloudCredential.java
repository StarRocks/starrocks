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

package com.starrocks.credential.hdfs;

import java.util.Map;

public class StrictHDFSCloudCredential extends HDFSCloudCredential {
    public StrictHDFSCloudCredential(String authentication, String username, String password, String kerberosPrincipal,
                                     String keytab, String keytabContent, Map<String, String> hadoopConfiguration) {
        super(authentication, username, password, kerberosPrincipal, keytab, keytabContent, hadoopConfiguration);
    }

    @Override
    public boolean validate() {
        if (!authentication.isEmpty()) {
            return authentication.equals(SIMPLE_AUTH) || authentication.equals(KERBEROS_AUTH);
        }
        return true;
    }
}
