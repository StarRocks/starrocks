// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.authentication;

import com.starrocks.analysis.UserIdentity;

public interface AuthenticationProvider {

    /**
     * valid authentication info, and initialize the UserAuthenticationInfo structure
     * used when creating a user or modifying user's authentication infomation
     */
    UserAuthenticationInfo validAuthenticationInfo(
            UserIdentity userIdentity,
            String password,
            String textForAuthPlugin) throws AuthenticationException;

    /**
     * login authentication
     */
    void authenticate(
            String user,
            String host,
            byte[] password,
            byte[] randomString,
            UserAuthenticationInfo authenticationInfo) throws AuthenticationException;
}
