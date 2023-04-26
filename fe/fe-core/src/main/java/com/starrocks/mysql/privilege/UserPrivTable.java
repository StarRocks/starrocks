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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/UserPrivTable.java

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

import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/*
 * UserPrivTable saves all global privs and also password for users
 */
public class UserPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(UserPrivTable.class);

    public UserPrivTable() {
    }

    /**
     * get privileges of specific user
     */
    public void getPrivs(UserIdentity currentUser, PrivBitSet savedPrivs) {
        List<PrivEntry> userPrivEntryList = map.get(currentUser);
        if (userPrivEntryList == null) {
            return;
        }
        savedPrivs.or(userPrivEntryList.get(0).getPrivSet());
    }

    /**
     * get password of specific user
     */
    public Password getPassword(UserIdentity currentUser) {
        List<PrivEntry> userPrivEntryList = map.get(currentUser);
        if (userPrivEntryList == null) {
            return null;
        }
        GlobalPrivEntry entry = (GlobalPrivEntry) userPrivEntryList.get(0);
        return entry.getPassword();
    }

    /**
     * Sometimes a password is required in the handshake process using plugin authentication,
     * such as kerberos authentication. This interface is mainly used to obtain password information approximately
     * before {@link Auth#checkPassword}. for sending handshake request.
     */
    public Password getPasswordByApproximate(String remoteUser, String remoteHost) {
        Iterator<PrivEntry> iter = getReadOnlyIteratorByUser(remoteUser, remoteHost);
        while (iter.hasNext()) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) iter.next();
            return globalPrivEntry.getPassword();
        }
        return null;
    }

    /*
     * Check if current user has specified privilege
     */
    public boolean hasPriv(UserIdentity currentUser, PrivPredicate wanted) {
        Iterator<PrivEntry> iter = getReadOnlyIteratorByUser(currentUser);
        while (iter.hasNext()) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) iter.next();
            if (globalPrivEntry.getPrivSet().satisfy(wanted)) {
                return true;
            }
        }
        return false;
    }

    // validate the connection by host, user and password.
    // return true if this connection is valid, and 'savedPrivs' save all global privs got from user table.
    // if currentUser is not null, save ALL matching user identity
    public boolean checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
                                 List<UserIdentity> currentUser) {
        LOG.debug("check password for user: {} from {}, password: {}, random string: {}",
                remoteUser, remoteHost, remotePasswd, randomString);

        Iterator<PrivEntry> iter = getReadOnlyIteratorByUser(remoteUser, remoteHost);
        while (iter.hasNext()) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) iter.next();

            Password password = globalPrivEntry.getPassword();
            if (password == null) {
                continue;
            }

            if (password.check(remoteUser, remotePasswd, randomString)) {
                if (currentUser != null) {
                    currentUser.add(globalPrivEntry.getDomainUserIdent());
                }
                return true;
            } else {
                // case A. this means we already matched a entry by user@host, but password is incorrect.
                // return false, NOT continue matching other entries.
                // For example, there are 2 entries in order:
                // 1. cmy@"192.168.%" identified by '123';
                // 2. cmy@"%" identified by 'abc';
                // if user cmy@'192.168.1.1' try to login with password 'abc', it will be denied.
                return false;
            }
        }

        return false;
    }

    public boolean checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
                                      List<UserIdentity> currentUser) {
        Iterator<PrivEntry> iter = getReadOnlyIteratorByUser(remoteUser, remoteHost);
        while (iter.hasNext()) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) iter.next();

            Password password = globalPrivEntry.getPassword();
            if (password == null) {
                continue;
            }
            if (password.checkPlain(remoteUser, remotePasswd)) {
                if (currentUser != null) {
                    currentUser.add(globalPrivEntry.getDomainUserIdent());
                }
                return true;
            } else {
                // set case A. in checkPassword()
                return false;
            }
        }

        return false;
    }

    /*
     * set password for specified entry. It is same as adding an entry to the user priv table.
     */
    public void setPassword(GlobalPrivEntry passwdEntry, boolean errOnNonExist) throws DdlException {
        GlobalPrivEntry addedEntry = (GlobalPrivEntry) addEntry(passwdEntry, false /* err on exist */,
                errOnNonExist /* err on non exist */);
        addedEntry.setPassword(passwdEntry.getPassword());
    }

    // return true only if user exist and not set by domain
    // user set by domain should be checked in property manager
    public boolean doesUserExist(UserIdentity userIdent) {
        if (!map.containsKey(userIdent)) {
            return false;
        }
        for (PrivEntry privEntry : map.get(userIdent)) {
            if (!privEntry.isSetByDomainResolver()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = UserPrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        super.write(out);
    }
}
