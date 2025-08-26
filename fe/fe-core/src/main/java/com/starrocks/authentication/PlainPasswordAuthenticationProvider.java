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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.epack.authentication.AuthenticationMgrEPack;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUserSecurityPolicyRequest;
import com.starrocks.thrift.TUserSecurityPolicyResponse;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;

public class PlainPasswordAuthenticationProvider implements AuthenticationProvider {
    private final byte[] password;

    public PlainPasswordAuthenticationProvider(byte[] password) {
        this.password = password;
    }

    public void authenticate(
            ConnectContext context,
            UserIdentity userIdentity,
            byte[] authResponse) throws AuthenticationException {
        AuthenticationMgrEPack authenticationMgr =
                (AuthenticationMgrEPack) GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String usePassword = authResponse.length == 0 ? "NO" : "YES";

        byte[] randomString = context.getAuthDataSalt();
        // The password sent by mysql client has already been scrambled(encrypted) using random string,
        // so we don't need to scramble it again.
        if (randomString != null) {
            byte[] saltPassword = MysqlPassword.getSaltFromPassword(password);
            if (saltPassword.length != authResponse.length) {
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, userIdentity.getUser(), usePassword);
            }

            if (authResponse.length > 0 && !MysqlPassword.checkScramble(authResponse, randomString, saltPassword)) {
                try {
                    if (GlobalStateMgr.getCurrentState().isLeader()) {
                        authenticationMgr.increasePasswordErrorTimes(userIdentity);
                    } else {
                        TAuthInfo tAuthInfo = new TAuthInfo();
                        tAuthInfo.current_user_ident = userIdentity.toThrift();

                        TUserSecurityPolicyRequest tUserSecurityPolicyRequest = new TUserSecurityPolicyRequest();
                        tUserSecurityPolicyRequest.setAuthInfo(tAuthInfo);

                        Pair<String, Integer> ipAndPort =
                                GlobalStateMgr.getCurrentState().getNodeMgr().getLeaderIpAndRpcPort();
                        TNetworkAddress thriftAddress = new TNetworkAddress(ipAndPort.first, ipAndPort.second);
                        TUserSecurityPolicyResponse response = ThriftRPCRequestExecutor.call(
                                ThriftConnectionPool.frontendPool,
                                thriftAddress,
                                client -> client.increasePasswordErrorTimes(tUserSecurityPolicyRequest));
                    }
                } catch (Exception ex) {
                    throw new AuthenticationException(ex.getMessage());
                }

                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, userIdentity.getUser(), usePassword);
            }
        } else {
            // Plain remote password, scramble it first.
            byte[] scrambledRemotePass = MysqlPassword.makeScrambledPassword((
                    StringUtils.stripEnd(new String(authResponse, StandardCharsets.UTF_8), "\0")));
            if (!MysqlPassword.checkScrambledPlainPass(password, scrambledRemotePass)) {
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, userIdentity.getUser(), usePassword);
            }
        }

        if (authenticationMgr.checkUserPasswordExpired(userIdentity)) {
            context.setPasswordExpired(true);
        }

        if (authenticationMgr.checkUserLocked(userIdentity)) {
            throw new AuthenticationException("user locked!");
        }
    }

    @Override
    public byte[] authSwitchRequestPacket(ConnectContext context, String user, String host) throws AuthenticationException {
        return context.getAuthDataSalt();
    }
}