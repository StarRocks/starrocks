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


package com.starrocks.authz.authentication;

import com.starrocks.mysql.privilege.Password;
import com.starrocks.sql.ast.UserIdentity;
import org.junit.Assert;
import org.junit.Test;

public class AuthenticationProviderFactoryTest {
    @Test
    public void testNormal() throws Exception {
        AuthenticationProvider fakeProvider = new AuthenticationProvider() {
            @Override
            public UserAuthenticationInfo validAuthenticationInfo(UserIdentity userIdentity, String password,
                                                                  String textForAuthPlugin)
                    throws AuthenticationException {
                return null;
            }

            @Override
            public void authenticate(String user, String host, byte[] password, byte[] randomString,
                                     UserAuthenticationInfo authenticationInfo) throws AuthenticationException {

            }

            @Override
            public UserAuthenticationInfo upgradedFromPassword(UserIdentity userIdentity, Password password)
                    throws AuthenticationException {
                return null;
            }
        };
        String fakeName = "fake";

        AuthenticationProviderFactory.installPlugin(fakeName, fakeProvider);
        AuthenticationProviderFactory.create(fakeName);

        // install multiple times will success
        AuthenticationProviderFactory.installPlugin(fakeName, fakeProvider);
        AuthenticationProviderFactory.create(fakeName);

        AuthenticationProviderFactory.uninstallPlugin(fakeName);
        try {
            AuthenticationProviderFactory.create(fakeName);
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot find " + fakeName + " from"));
        }
        AuthenticationProviderFactory.uninstallPlugin(fakeName);

    }
}
