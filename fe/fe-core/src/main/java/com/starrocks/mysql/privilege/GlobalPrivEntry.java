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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/GlobalPrivEntry.java

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

package com.starrocks.mysql.privilege;

import com.starrocks.analysis.TablePattern;
import com.starrocks.common.StarRocksFEMetaVersion;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GlobalPrivEntry extends PrivEntry {
    private static final Logger LOG = LogManager.getLogger(GlobalPrivEntry.class);
    private Password password;
    // set domainUserIdent when this a password entry and is set by domain resolver.
    // so that when user checking password with user@'IP' and match a entry set by the resolver,
    // it should return this domainUserIdent as "current user". And user can use this user ident to get privileges
    // further.
    private UserIdentity domainUserIdent;

    protected GlobalPrivEntry() {
    }

    protected GlobalPrivEntry(String origHost, String origUser, boolean isDomain, PrivBitSet privSet, Password password) {
        super(origHost, origUser, isDomain, privSet);
        this.password = password;
    }

    public static GlobalPrivEntry create(String host, String user, boolean isDomain, Password password,
                                         PrivBitSet privs)
            throws AnalysisException {
        GlobalPrivEntry globalPrivEntry = new GlobalPrivEntry(host, user, isDomain, privs, password);
        globalPrivEntry.analyse();
        return globalPrivEntry;
    }

    public Password getPassword() {
        return password;
    }

    public void setPassword(Password password) {
        this.password = password;
    }

    public void setDomainUserIdent(UserIdentity domainUserIdent) {
        this.domainUserIdent = domainUserIdent;
    }

    public UserIdentity getDomainUserIdent() {
        if (isSetByDomainResolver()) {
            return domainUserIdent;
        } else {
            return getUserIdent();
        }
    }

    /*
     * UserTable is ordered by Host, User
     * eg:
     * +-----------+----------+-
     * | Host      | User     | ...
     * +-----------+----------+-
     * | %         | root     | ...
     * | %         | jeffrey  | ...
     * | localhost | root     | ...
     * | localhost |          | ...
     * +-----------+----------+-
     *
     * will be sorted like:
     *
     * +-----------+----------+-
     * | Host      | User     | ...
     * +-----------+----------+-
     * | localhost | root     | ...
     * | localhost |          | ...
     * | %         | jeffrey  | ...
     * | %         | root     | ...
     * +-----------+----------+-
     *
     * https://dev.mysql.com/doc/refman/8.0/en/connection-access.html
     */
    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof GlobalPrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        GlobalPrivEntry otherEntry = (GlobalPrivEntry) other;
        int res = otherEntry.origHost.compareTo(origHost);
        if (res != 0) {
            return res;
        }

        return otherEntry.realOrigUser.compareTo(realOrigUser);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof GlobalPrivEntry)) {
            return false;
        }

        GlobalPrivEntry otherEntry = (GlobalPrivEntry) other;
        if (origHost.equals(otherEntry.origHost) && realOrigUser.equals(otherEntry.realOrigUser)
                && isDomain == otherEntry.isDomain) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("global priv. host: ").append(origHost).append(", user: ").append(realOrigUser);
        sb.append(", priv: ").append(privSet).append(", set by resolver: ").append(isSetByDomainResolver);
        sb.append(", domain user ident: ").append(domainUserIdent);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = GlobalPrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        LOG.info("global priv: {}", this.toString());
        super.write(out);

        password.write(out);
        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if (GlobalStateMgr.getCurrentStateStarRocksMetaVersion() >= StarRocksFEMetaVersion.VERSION_2) {
            this.password = Password.read(in);
        } else {
            int passwordLen = in.readInt();
            byte[] pdBytes = new byte[passwordLen];
            in.readFully(pdBytes);
            this.password = new Password(pdBytes);
        }
    }

    @Override
    public String toGrantSQL() {
        if (privSet.isEmpty()) {
            return null;
        }
        GrantPrivilegeStmt stmt = new GrantPrivilegeStmt(null, "TABLE", getUserIdent(), false);
        stmt.setAnalysedTable(privSet, new TablePattern("*", "*"));
        return AstToStringBuilder.toString(stmt);
    }
}
