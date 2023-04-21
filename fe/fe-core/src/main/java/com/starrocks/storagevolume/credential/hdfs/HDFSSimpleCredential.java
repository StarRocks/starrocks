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

package com.starrocks.storagevolume.credential.hdfs;

import java.util.Map;

public class HDFSSimpleCredential implements HDFSCredential {
    private String userName;
    private String password;
    private Map<String, String> haConfigurations;

    public HDFSSimpleCredential(String userName, String password, Map<String, String> haConfigurations) {
        this.userName = userName;
        this.password = password;
        this.haConfigurations = haConfigurations;
    }

    @Override
    public HDFSCredentialType type() {
        return HDFSCredentialType.SIMPLE;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public Map<String, String> getHaConfigurations() {
        return haConfigurations;
    }
}
