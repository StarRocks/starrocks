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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/PrivInfo.java

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

package com.starrocks.persist;

import com.google.common.base.Strings;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.StarRocksFEMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.mysql.privilege.Password;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PrivInfo implements Writable {
    private UserIdentity userIdent;
    private TablePattern tblPattern;
    private ResourcePattern resourcePattern;
    private PrivBitSet privs;
    private Password passwd;
    private String role;

    private PrivInfo() {

    }

    public PrivInfo(UserIdentity userIdent, PrivBitSet privs, Password passwd, String role) {
        this.userIdent = userIdent;
        this.tblPattern = null;
        this.resourcePattern = null;
        this.privs = privs;
        this.passwd = passwd;
        this.role = role;
    }

    public PrivInfo(UserIdentity userIdent, TablePattern tablePattern, PrivBitSet privs,
                    Password passwd, String role) {
        this.userIdent = userIdent;
        this.tblPattern = tablePattern;
        this.resourcePattern = null;
        this.privs = privs;
        this.passwd = passwd;
        this.role = role;
    }

    public PrivInfo(UserIdentity userIdent, ResourcePattern resourcePattern, PrivBitSet privs,
                    Password passwd, String role) {
        this.userIdent = userIdent;
        this.tblPattern = null;
        this.resourcePattern = resourcePattern;
        this.privs = privs;
        this.passwd = passwd;
        this.role = role;
    }

    public PrivInfo(UserIdentity userIdent, String role) {
        this.userIdent = userIdent;
        this.role = role;
        this.tblPattern = null;
        this.resourcePattern = null;
        this.privs = null;
        this.passwd = null;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public TablePattern getTblPattern() {
        return tblPattern;
    }

    public ResourcePattern getResourcePattern() {
        return resourcePattern;
    }

    public PrivBitSet getPrivs() {
        return privs;
    }

    public Password getPasswd() {
        return passwd;
    }

    public String getRole() {
        return role;
    }

    public static PrivInfo read(DataInput in) throws IOException {
        PrivInfo info = new PrivInfo();
        info.readFields(in);
        return info;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (userIdent != null) {
            out.writeBoolean(true);
            userIdent.write(out);
        } else {
            out.writeBoolean(false);
        }

        if (tblPattern != null) {
            out.writeBoolean(true);
            tblPattern.write(out);
        } else {
            out.writeBoolean(false);
        }

        if (resourcePattern != null) {
            out.writeBoolean(true);
            resourcePattern.write(out);
        } else {
            out.writeBoolean(false);
        }

        if (privs != null) {
            out.writeBoolean(true);
            privs.write(out);
        } else {
            out.writeBoolean(false);
        }

        if (passwd != null) {
            out.writeBoolean(true);
            passwd.write(out);
        } else {
            out.writeBoolean(false);
        }

        if (!Strings.isNullOrEmpty(role)) {
            out.writeBoolean(true);
            Text.writeString(out, ClusterNamespace.getFullName(role));
        } else {
            out.writeBoolean(false);
        }
    }

    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            userIdent = UserIdentity.read(in);
        }

        if (in.readBoolean()) {
            tblPattern = TablePattern.read(in);
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_87) {
            if (in.readBoolean()) {
                resourcePattern = ResourcePattern.read(in);
            }
        }

        if (in.readBoolean()) {
            privs = PrivBitSet.read(in);
        }

        if (in.readBoolean()) {
            if (GlobalStateMgr.getCurrentStateStarRocksMetaVersion() >= StarRocksFEMetaVersion.VERSION_2) {
                passwd = Password.read(in);
            } else {
                int passwordLen = in.readInt();
                byte[] password = new byte[passwordLen];
                in.readFully(password);
                passwd = new Password(password);
            }
        }

        if (in.readBoolean()) {
            role = ClusterNamespace.getNameFromFullName(Text.readString(in));
        }

    }

}
