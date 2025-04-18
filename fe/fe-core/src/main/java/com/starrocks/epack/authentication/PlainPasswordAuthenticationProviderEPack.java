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

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.common.Config;
import com.starrocks.epack.authorization.PasswordPolicy;
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PlainPasswordAuthenticationProviderEPack extends PlainPasswordAuthenticationProvider {

    @Override
    protected void validatePassword(UserIdentity userIdentity, UserAuthOption userAuthOption) throws AuthenticationException {
        SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        PasswordPolicy passwordPolicy = securityPolicyMgr.getGlobalPasswordPolicy();

        String password = userAuthOption.getAuthString();

        if (passwordPolicy == null) {
            if (Config.enable_validate_password) {
                PasswordPolicy.defaultPasswordPolicy.checkPasswordValid(password);
            }
        } else {
            if (!userAuthOption.isPasswordPlain()) {
                throw new AuthenticationException("Because the Password Policy is in effect, you cannot use a hashed password.");
            }
            passwordPolicy.checkPasswordValid(password);
        }

        if (!Config.enable_password_reuse) {
            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            Map.Entry<UserIdentity, UserAuthenticationInfo> userAuthenticationInfoEntry =
                    authenticationMgr.getBestMatchedUserIdentity(userIdentity.getUser(), userIdentity.getHost());
            if (userAuthenticationInfoEntry != null) {
                try {
                    authenticate(new ConnectContext(), userIdentity.getUser(), userIdentity.getHost(),
                            password.getBytes(StandardCharsets.UTF_8), userAuthenticationInfoEntry.getValue());
                } catch (AuthenticationException e) {
                    return;
                }

                throw new AuthenticationException("Can't reuse password");
            }
        }
    }
}
