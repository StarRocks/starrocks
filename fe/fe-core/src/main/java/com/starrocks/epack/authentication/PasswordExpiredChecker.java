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
package com.starrocks.epack.authentication;

import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.epack.authorization.PasswordPolicy;
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.epack.sql.ast.UserPasswordOption;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserLockOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class PasswordExpiredChecker extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(PasswordExpiredChecker.class);

    public PasswordExpiredChecker() {
        super("password expired checker", 60000);
    }

    @Override
    protected void runAfterCatalogReady() {
        long currentTs = System.currentTimeMillis();
        checkPasswordExpiredAndLock(currentTs);
    }

    public void checkPasswordExpiredAndLock(long currentTs) {
        AuthenticationMgrEPack authenticationMgrEPack =
                (AuthenticationMgrEPack) GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getServingState().getSecurityPolicyManager();
        PasswordPolicy passwordPolicy = securityPolicyMgr.getGlobalPasswordPolicy();
        if (passwordPolicy == null) {
            return;
        }

        Map<UserIdentity, UserAuthenticationInfo> userAuthenticationInfoMap
                = authenticationMgrEPack.getUserToAuthenticationInfo();

        for (Map.Entry<UserIdentity, UserAuthenticationInfo> entry : userAuthenticationInfoMap.entrySet()) {
            UserIdentity userIdentity = entry.getKey();
            UserAuthenticationInfo userAuthenticationInfo = entry.getValue();

            if (passwordPolicy.getPasswordMaxAgeDays() != null) {
                long lastModifiedTs = userAuthenticationInfo.getPasswordLastModifiedTimestamp();
                long maxTimeMsSpan = (long) passwordPolicy.getPasswordMaxAgeDays() * 24 * 60 * 60 * 1000;

                if (currentTs - lastModifiedTs >= maxTimeMsSpan) {
                    try {
                        UserPasswordOption userPasswordOption = new UserPasswordOption(true);
                        authenticationMgrEPack.alterUser(userIdentity, null, userPasswordOption,
                                null, null);
                    } catch (DdlException e) {
                        LOG.error(e.getMessage());
                    }
                }
            }

            if (userAuthenticationInfo.isLock()) {
                long lockTimestamp = userAuthenticationInfo.getLockTimestamp();
                int passwordLockoutTimeMins = passwordPolicy.getPasswordLockoutTimeMins();
                if (currentTs - lockTimestamp >= (long) passwordLockoutTimeMins * 60 * 1000) {
                    UserLockOption userLockOption = new UserLockOption(false);
                    try {
                        authenticationMgrEPack.alterUser(userIdentity, null, null, userLockOption,
                                null);
                    } catch (DdlException e) {
                        LOG.error(e.getMessage());
                    }
                }
            }
        }
    }
}
