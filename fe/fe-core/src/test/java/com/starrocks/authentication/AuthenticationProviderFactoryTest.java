// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.authentication;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.mysql.privilege.Password;
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
