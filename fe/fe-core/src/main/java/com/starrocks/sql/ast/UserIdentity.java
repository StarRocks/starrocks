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

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TUserIdentity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// https://dev.mysql.com/doc/refman/8.0/en/account-names.html
// username must be literally matched.
public class UserIdentity implements ParseNode, Writable, GsonPostProcessable {
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

    private final NodePosition pos;

    public static final UserIdentity ROOT;

    static {
        ROOT = new UserIdentity(AuthenticationMgr.ROOT_USER, "%");
    }

    /**
     * Allow empty construction for gson
     */
    public UserIdentity() {
        pos = NodePosition.ZERO;
    }

    public UserIdentity(String user, String host) {
        this(user, host, false);
    }

    public UserIdentity(boolean ephemeral, String user, String host) {
        this(user, host, false, NodePosition.ZERO, ephemeral);
    }

    public UserIdentity(String user, String host, boolean isDomain) {
        this(user, host, isDomain, NodePosition.ZERO, false);
    }

    public UserIdentity(String user, String host, boolean isDomain, NodePosition pos, boolean ephemeral) {
        this.pos = pos;
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

    public static UserIdentity fromThrift(TUserIdentity tUserIdent) {
        return new UserIdentity(tUserIdent.getUsername(), tUserIdent.getHost(), tUserIdent.is_domain);
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

    public void analyze() {
        if (Strings.isNullOrEmpty(user)) {
            throw new SemanticException("Does not support anonymous user");
        }

        FeNameFormat.checkUserName(user);

        // reuse createMysqlPattern to validate host pattern
        PatternMatcher.createMysqlPattern(host, CaseSensibility.HOST.getCaseSensibility());
    }

    public static UserIdentity fromString(String userIdentStr) {
        if (Strings.isNullOrEmpty(userIdentStr)) {
            return null;
        }

        String[] parts = userIdentStr.split("@");
        if (parts.length != 2) {
            return null;
        }

        String user = parts[0];
        if (!user.startsWith("'") || !user.endsWith("'")) {
            return null;
        }

        String host = parts[1];
        if (host.startsWith("['") && host.endsWith("']")) {
            return new UserIdentity(user.substring(1, user.length() - 1),
                    host.substring(2, host.length() - 2), true);
        } else if (host.startsWith("'") && host.endsWith("'")) {
            return new UserIdentity(user.substring(1, user.length() - 1),
                    host.substring(1, host.length() - 1));
        }

        return null;
    }

    public TUserIdentity toThrift() {
        TUserIdentity tUserIdent = new TUserIdentity();
        tUserIdent.setHost(host);
        tUserIdent.setUsername(user);
        tUserIdent.setIs_domain(isDomain);
        return tUserIdent;
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

    // change user to default_cluster:user for write
    // and change default_cluster:user to user after read
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, ClusterNamespace.getFullName(user));
        Text.writeString(out, host);
        out.writeBoolean(isDomain);
    }

    public static UserIdentity read(DataInput in) throws IOException {
        UserIdentity userIdentity = new UserIdentity();
        userIdentity.readFields(in);
        return userIdentity;
    }

    public void readFields(DataInput in) throws IOException {
        user = ClusterNamespace.getNameFromFullName(Text.readString(in));
        host = Text.readString(in);
        isDomain = in.readBoolean();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        user = ClusterNamespace.getNameFromFullName(user);
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
