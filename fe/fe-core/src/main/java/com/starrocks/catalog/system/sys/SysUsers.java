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
package com.starrocks.catalog.system.sys;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetUsersRequest;
import com.starrocks.thrift.TGetUsersResponse;
import com.starrocks.thrift.TGetUsersResponseItem;
import com.starrocks.thrift.TSchemaTableType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class SysUsers {
    public static SystemTable create() {
        return new SystemTable(SystemId.USERS_ID, "users", Table.TableType.SCHEMA,
                builder()
                        .column("HOST", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("USER", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PASSWORD_EXPIRED", ScalarType.createType(PrimitiveType.BOOLEAN))
                        .column("PASSWORD_POLICY", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PASSWORD_LAST_CHANGED", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("IS_LOCKED", ScalarType.createType(PrimitiveType.BOOLEAN))
                        .build(),
                TSchemaTableType.SYS_USERS);
    }

    public static TGetUsersResponse getUsers(TGetUsersRequest request) {
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        TGetUsersResponse response = new TGetUsersResponse();
        Map<UserIdentity, UserAuthenticationInfo> userToAuthenticationInfo = authenticationMgr.getUserToAuthenticationInfo();
        for (Map.Entry<UserIdentity, UserAuthenticationInfo> userAuthenticationInfoEntry : userToAuthenticationInfo.entrySet()) {
            UserAuthenticationInfo userAuthenticationInfo = userAuthenticationInfoEntry.getValue();

            TGetUsersResponseItem item = new TGetUsersResponseItem();
            item.setHost(userAuthenticationInfo.getOrigHost());
            item.setUser(userAuthenticationInfo.getOrigUser());
            item.setPassword_expired(userAuthenticationInfo.isPasswordExpired());
            item.setPassword_policy("");

            String passwordLastModifiedDate = LocalDateTime.ofInstant(
                    Instant.ofEpochSecond(userAuthenticationInfo.getPasswordLastModifiedTimestamp() / 1000),
                    TimeUtils.getTimeZone().toZoneId()).format(DateUtils.DATE_TIME_FORMATTER_UNIX);
            item.setPassword_last_change(passwordLastModifiedDate);

            item.setIs_locked(userAuthenticationInfo.isLock());

            response.addToUsers(item);
        }

        return response;
    }
}