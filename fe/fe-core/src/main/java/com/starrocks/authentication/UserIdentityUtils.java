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

package com.starrocks.authentication;

import com.google.common.base.Strings;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TUserIdentity;

import java.util.HashSet;

public class UserIdentityUtils {
    
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

    public static TUserIdentity toThrift(UserIdentity userIdentity) {
        TUserIdentity tUserIdent = new TUserIdentity();
        tUserIdent.setHost(userIdentity.getHost());
        tUserIdent.setUsername(userIdentity.getUser());
        tUserIdent.setIs_domain(userIdentity.isDomain());
        return tUserIdent;
    }

    public static UserIdentity fromThrift(TUserIdentity tUserIdent) {
        return new UserIdentity(tUserIdent.getUsername(), tUserIdent.getHost(), tUserIdent.is_domain,
                tUserIdent.is_ephemeral);
    }

    public static void setAuthInfoFromThrift(ConnectContext context, TAuthInfo authInfo) {
        if (authInfo.isSetCurrent_user_ident()) {
            setAuthInfoFromThrift(context, authInfo.getCurrent_user_ident());
        } else {
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(authInfo.user, authInfo.user_ip);
            context.setCurrentUserIdentity(userIdentity);
            context.setCurrentRoleIds(userIdentity);
        }
    }

    public static void setAuthInfoFromThrift(ConnectContext context, TUserIdentity tUserIdent) {
        UserIdentity userIdentity = UserIdentityUtils.fromThrift(tUserIdent);
        context.setCurrentUserIdentity(userIdentity);
        if (tUserIdent.isSetCurrent_role_ids()) {
            context.setCurrentRoleIds(new HashSet<>(tUserIdent.current_role_ids.getRole_id_list()));
        } else {
            context.setCurrentRoleIds(userIdentity);
        }
    }

}
