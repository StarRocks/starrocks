// This file is made available under Elastic License 2.0.
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

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Text;
import org.apache.commons.collections.map.HashedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * UserPrivTable saves all global privs and also password for users
 */
public class UserPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(UserPrivTable.class);

    // Used for lock on multiple failed attempts
    // user -> the first time that login failed
    private Map<String, Long> firstFailedAttemptMap = new HashMap<>();
    // user -> how many failed attempts
    private Map<String, Integer> continuouslyFailedAttemptMap = new HashedMap();

    public UserPrivTable() {
    }

    public void getPrivs(UserIdentity currentUser, PrivBitSet savedPrivs) {
        GlobalPrivEntry matchedEntry = null;
        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;

            if (!globalPrivEntry.match(currentUser, true)) {
                continue;
            }

            matchedEntry = globalPrivEntry;
            break;
        }
        if (matchedEntry == null) {
            return;
        }

        savedPrivs.or(matchedEntry.getPrivSet());
    }

    public Password getPassword(UserIdentity currentUser) {
        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;

            if (globalPrivEntry.match(currentUser, true)) {
                return globalPrivEntry.getPassword();
            }
        }

        return null;
    }

    /**
     * Sometimes a password is required in the handshake process using plugin authentication,
     * such as kerberos authentication. This interface is mainly used to obtain password information approximately
     * before {@link Auth#checkPassword}. for sending handshake request.
     */
    public Password getPasswordByApproximate(String remoteUser, String remoteHost) {
        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;

            // check host
            if (!globalPrivEntry.isAnyHost() && !globalPrivEntry.getHostPattern().match(remoteHost)) {
                continue;
            }

            // check user
            if (!globalPrivEntry.isAnyUser() && !globalPrivEntry.getUserPattern().match(remoteUser)) {
                continue;
            }

            return globalPrivEntry.getPassword();
        }

        return null;
    }

    /*
     * Check if user@host has specified privilege
     */
    public boolean hasPriv(String host, String user, PrivPredicate wanted) {
        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;
            // check host
            if (!globalPrivEntry.isAnyHost() && !globalPrivEntry.getHostPattern().match(host)) {
                continue;
            }
            // check user
            if (!globalPrivEntry.isAnyUser() && !globalPrivEntry.getUserPattern().match(user)) {
                continue;
            }
            if (globalPrivEntry.getPrivSet().satisfy(wanted)) {
                return true;
            }
        }
        return false;
    }

    // validate the connection by host, user and password.
    // return true if this connection is valid, and 'savedPrivs' save all global privs got from user table.
    // if currentUser is not null, save the current user identity
    public boolean checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
                                 List<UserIdentity> currentUser) {
        LOG.debug("check password for user: {} from {}, password: {}, random string: {}",
                remoteUser, remoteHost, remotePasswd, randomString);

        // TODO(cmy): for now, we check user table from first entry to last,
        // This may not efficient, but works.
        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;

            // check host
            if (!globalPrivEntry.isAnyHost() && !globalPrivEntry.getHostPattern().match(remoteHost)) {
                continue;
            }

            // check user
            if (!globalPrivEntry.isAnyUser() && !globalPrivEntry.getUserPattern().match(remoteUser)) {
                continue;
            }

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
        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;

            // check host
            if (!globalPrivEntry.isAnyHost() && !globalPrivEntry.getHostPattern().match(remoteHost)) {
                continue;
            }

            // check user
            if (!globalPrivEntry.isAnyUser() && !globalPrivEntry.getUserPattern().match(remoteUser)) {
                continue;
            }

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
        for (PrivEntry privEntry : entries) {
            if (privEntry.match(userIdent, true /* exact match */) && !privEntry.isSetByDomainResolver()) {
                return true;
            }
        }
        return false;
    }

    /**
     * check login is allowed
     **/
    public boolean allowLoginAttempt(String remoteUser, String remoteHost) {
        // unlimited attempt
        if (Config.max_failed_login_attempts < 0) {
            return true;
        }

        // no failed attempt found, allow login attempt
        if (!firstFailedAttemptMap.containsKey(remoteUser)) {
            return true;
        }

        long now = System.currentTimeMillis();
        long firstFailedAttempt = firstFailedAttemptMap.get(remoteUser);
        long remainingSeconds = Config.password_lock_interval_seconds - (now - firstFailedAttempt) / 1000;
        if (remainingSeconds <= 0) {
            LOG.debug("user {} has been allowed to login since {} seconds ago.", remoteUser, 0 - remainingSeconds);
            // failed attempt timeout, clear record and allow attempt
            clearFailedAttemptRecords(remoteUser);
            return true;
        }

        // failed attempt too many times
        int failedAttemptTimes = continuouslyFailedAttemptMap.get(remoteUser);
        if (failedAttemptTimes >= Config.max_failed_login_attempts) {
            ErrorReport.report(ErrorCode.ERR_FAILED_ATTEMPT, remoteUser, remoteHost,
                    Config.password_lock_interval_seconds, remainingSeconds, Config.max_failed_login_attempts);
            LOG.debug("user {} is not allowed to login until {} seconds later", remoteUser, remainingSeconds);
            return false;
        }

        return true;
    }

    /**
     * If login fails, call this method to record it.
     **/
    public void recordFailedAttempt(String remoteUser) {
        if (!firstFailedAttemptMap.containsKey(remoteUser)) {
            firstFailedAttemptMap.put(remoteUser, System.currentTimeMillis());
            continuouslyFailedAttemptMap.put(remoteUser, 1);
        } else {
            int failedAttemptTimes = continuouslyFailedAttemptMap.get(remoteUser);
            continuouslyFailedAttemptMap.put(remoteUser, failedAttemptTimes + 1);
        }
    }

    /**
     * Clear failed attempt records upon login success.
     **/
    public void clearFailedAttemptRecords(String remoteUser) {
        if (firstFailedAttemptMap.containsKey(remoteUser)) {
            firstFailedAttemptMap.remove(remoteUser);
            continuouslyFailedAttemptMap.remove(remoteUser);
        }
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
