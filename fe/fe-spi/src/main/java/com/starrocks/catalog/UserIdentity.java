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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/UserIdentity.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;

import java.io.IOException;

// https://dev.mysql.com/doc/refman/8.0/en/account-names.html
// username must be literally matched.
public class UserIdentity implements Writable, GsonPostProcessable {
    @SerializedName("user")
    private String user;
    @SerializedName("host")
    private String host;
    @SerializedName("isDomain")
    private boolean isDomain;
    /**
     * A user is ephemeral meaning that it has a session level life circle, i.e. it's created on user connecting and
     * destroyed after disconnected, currently it's used by ldap security integration where we use external ldap server
     * to authenticate and the metadata of a user is not stored on StarRocks.
     */
    private boolean ephemeral;

    public static final UserIdentity ROOT;

    static {
        ROOT = new UserIdentity("root", "%");
    }

    /**
     * Allow empty construction for gson
     */
    public UserIdentity() {
    }

    public UserIdentity(String user, String host) {
        this(user, host, false);
    }

    public UserIdentity(boolean ephemeral, String user, String host) {
        this(user, host, false, ephemeral);
    }

    public UserIdentity(String user, String host, boolean isDomain) {
        this(user, host, isDomain, false);
    }

    public UserIdentity(String user, String host, boolean isDomain, boolean ephemeral) {
        this.user = user;
        this.host = Strings.emptyToNull(host);
        this.isDomain = isDomain;
        this.ephemeral = ephemeral;
    }

    public static UserIdentity createAnalyzedUserIdentWithIp(String user, String host) {
        return new UserIdentity(user, host);
    }

    public static UserIdentity createAnalyzedUserIdentWithDomain(String user, String domain) {
        return new UserIdentity(user, domain, true);
    }

    public static UserIdentity createEphemeralUserIdent(String user, String host) {
        return new UserIdentity(true, user, host);
    }

    public String getUser() {
        return user;
    }

    public String getHost() {
        return host;
    }

    public boolean isDomain() {
        return isDomain;
    }

    public boolean isEphemeral() {
        return ephemeral;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UserIdentity)) {
            return false;
        }
        UserIdentity other = (UserIdentity) obj;
        return user.equals(other.getUser()) && host.equals(other.getHost()) && this.isDomain == other.isDomain;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + user.hashCode();
        result = 31 * result + host.hashCode();
        result = 31 * result + Boolean.valueOf(isDomain).hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("'");
        if (!Strings.isNullOrEmpty(user)) {
            sb.append(user);
        }
        sb.append("'@");
        if (!Strings.isNullOrEmpty(host)) {
            if (isDomain) {
                sb.append("['").append(host).append("']");
            } else {
                sb.append("'").append(host).append("'");
            }
        } else {
            sb.append("%");
        }
        return sb.toString();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        user = ClusterNamespace.getNameFromFullName(user);
    }
}
