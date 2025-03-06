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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/MysqlProto.java

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

package com.starrocks.mysql;

import com.google.common.base.Strings;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.OIDCSecurityIntegration;
import com.starrocks.authentication.OpenIdConnectAuthenticationProvider;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.ssl.SSLContextLoader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// MySQL protocol util
public class MysqlProto {
    private static final Logger LOG = LogManager.getLogger(MysqlProto.class);

    private static final String LOCALHOST = "127.0.0.1";

    // send response packet(OK/EOF/ERR).
    // before call this function, should set information in state of ConnectContext
    public static void sendResponsePacket(ConnectContext context) throws IOException {
        MysqlSerializer serializer = context.getSerializer();
        MysqlChannel channel = context.getMysqlChannel();
        MysqlPacket packet = context.getState().toResponsePacket();

        // send response packet to client
        serializer.reset();
        packet.writeTo(serializer);
        channel.sendAndFlush(serializer.toByteBuffer());
    }

    /**
     * negotiate with client, use MySQL protocol
     * server ---handshake---> client
     * server <--- authenticate --- client
     * server --- response(OK/ERR) ---> client
     * Exception:
     * IOException:
     */
    public static NegotiateResult negotiate(ConnectContext context) throws IOException {
        MysqlSerializer serializer = context.getSerializer();
        MysqlChannel channel = context.getMysqlChannel();
        context.getState().setOk();

        // Server send handshake packet to client.
        serializer.reset();
        MysqlHandshakePacket handshakePacket = new MysqlHandshakePacket(context.getConnectionId(),
                SSLContextLoader.getSslContext() != null);
        handshakePacket.writeTo(serializer);
        channel.sendAndFlush(serializer.toByteBuffer());

        MysqlAuthPacket authPacket = readAuthPacket(context);
        if (authPacket == null) {
            return new NegotiateResult(null, NegotiateState.READ_FIRST_AUTH_PKG_FAILED);
        }

        if (authPacket.isSSLConnRequest()) {
            // change to ssl session
            LOG.info("start to enable ssl connection");
            if (!context.enableSSL()) {
                LOG.warn("enable ssl connection failed");
                ErrorReport.report(ErrorCode.ERR_CHANGE_TO_SSL_CONNECTION_FAILED);
                sendResponsePacket(context);
                return new NegotiateResult(authPacket, NegotiateState.ENABLE_SSL_FAILED);
            } else {
                LOG.info("enable ssl connection successfully");
            }

            // read the authentication package again from client
            authPacket = readAuthPacket(context);
            if (authPacket == null) {
                return new NegotiateResult(null, NegotiateState.READ_SSL_AUTH_PKG_FAILED);
            }
        } else if (Config.ssl_force_secure_transport) {
            if (!isRemoteIPLocalhost(context.getRemoteIP())) {
                LOG.warn("Connections using insecure transport are prohibited");
                ErrorReport.report(ErrorCode.ERR_SECURE_TRANSPORT_REQUIRED);
                sendResponsePacket(context);
                return new NegotiateResult(null, NegotiateState.INSECURE_TRANSPORT_PROHIBITED);
            } else {
                LOG.info("Connection made from a localhost, no secure transport enforced");
            }
        }

        // check capability
        if (!MysqlCapability.isCompatible(context.getServerCapability(), authPacket.getCapability())) {
            // TODO: client return capability can not support
            ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
            sendResponsePacket(context);
            return new NegotiateResult(authPacket, NegotiateState.NOT_SUPPORTED_AUTH_MODE);
        }

        // StarRocks support the Protocol::AuthSwitchRequest to tell client which auth plugin is using
        String user = authPacket.getUser();
        String authPluginName = authPacket.getPluginName();
        String switchAuthPlugin = switchAuthPlugin(user, authPluginName, context);
        if (switchAuthPlugin != null && !switchAuthPlugin.equals(authPluginName)) {
            // 1. clear the serializer
            serializer.reset();
            // 2. build the auth switch request and send to the client
            if (switchAuthPlugin.equals(MysqlHandshakePacket.AUTHENTICATION_KERBEROS_CLIENT)) {
                if (GlobalStateMgr.getCurrentState().getAuthenticationMgr().isSupportKerberosAuth()) {
                    try {
                        handshakePacket.buildKrb5AuthRequest(serializer, context.getRemoteIP(), authPacket.getUser());
                    } catch (Exception e) {
                        ErrorReport.report("Building handshake with kerberos error, msg: %s", e.getMessage());
                        sendResponsePacket(context);
                        return new NegotiateResult(authPacket, NegotiateState.KERBEROS_HANDSHAKE_FAILED);
                    }
                } else {
                    ErrorReport.report(ErrorCode.ERR_AUTH_PLUGIN_NOT_LOADED, "authentication_kerberos");
                    sendResponsePacket(context);
                    return new NegotiateResult(authPacket, NegotiateState.KERBEROS_PLUGIN_NOT_LOADED);
                }
            } else {
                handshakePacket.buildAuthSwitchRequest(serializer, switchAuthPlugin);
            }
            authPluginName = switchAuthPlugin;
            channel.sendAndFlush(serializer.toByteBuffer());
            // Server receive auth switch response packet from client.
            ByteBuffer authSwitchResponse = channel.fetchOnePacket();
            if (authSwitchResponse == null) {
                // receive response failed.
                LOG.warn("read auth switch response failed for user {}", authPacket.getUser());
                return new NegotiateResult(authPacket, NegotiateState.READ_AUTH_SWITCH_PKG_FAILED);
            }
            // 3. the client use default password plugin of StarRocks to dispose
            // password
            authPacket.setAuthResponse(readEofString(authSwitchResponse));
        }

        // change the capability of serializer
        context.setCapability(context.getServerCapability());
        serializer.setCapability(context.getCapability());

        // NOTE: when we behind proxy, we need random string sent by proxy.
        byte[] randomString =
                Objects.equals(authPluginName, MysqlHandshakePacket.CLEAR_PASSWORD_PLUGIN_NAME) ?
                        null : handshakePacket.getAuthPluginData();
        try {
            AuthenticationHandler.authenticate(context, authPacket.getUser(), context.getMysqlChannel().getRemoteIp(),
                    authPacket.getAuthResponse(), randomString);
        } catch (AuthenticationException e) {
            sendResponsePacket(context);
            return new NegotiateResult(authPacket, NegotiateState.AUTHENTICATION_FAILED);
        }

        // set database
        String db = authPacket.getDb();
        if (!Strings.isNullOrEmpty(db)) {
            try {
                context.changeCatalogDb(db);
            } catch (DdlException e) {
                sendResponsePacket(context);
                return new NegotiateResult(authPacket, NegotiateState.SET_DATABASE_FAILED);
            }
        }
        return new NegotiateResult(authPacket, NegotiateState.OK);
    }

    private static String switchAuthPlugin(String user, String authPluginName, ConnectContext context) {
        Map.Entry<UserIdentity, UserAuthenticationInfo> localUser = context.getGlobalStateMgr().getAuthenticationMgr()
                .getBestMatchedUserIdentity(user, context.getMysqlChannel().getRemoteIp());
        if (localUser != null) {
            // Older version mysql client does not send auth plugin info, like 5.1 version.
            if (authPluginName == null) {
                return null;
            }

            // kerberos
            if (authPluginName.equals(MysqlHandshakePacket.AUTHENTICATION_KERBEROS_CLIENT)) {
                return MysqlHandshakePacket.AUTHENTICATION_KERBEROS_CLIENT;
            }

            UserAuthenticationInfo authInfo = localUser.getValue();
            if (authInfo.getAuthPlugin().equalsIgnoreCase(OpenIdConnectAuthenticationProvider.PLUGIN_NAME)) {
                return MysqlHandshakePacket.AUTHENTICATION_OPENID_CONNECT_CLIENT;
            }

            // Starting with MySQL 8.0.4, MySQL changed the default authentication plugin for MySQL client
            // from mysql_native_password to caching_sha2_password.
            // ref: https://mysqlserverteam.com/mysql-8-0-4-new-default-authentication-plugin-caching_sha2_password/
            // But caching_sha2_password is not supported in starrocks. So switch to mysql_native_password.
            if (!MysqlHandshakePacket.checkAuthPluginSameAsStarRocks(authPluginName)) {
                return MysqlHandshakePacket.NATIVE_AUTH_PLUGIN_NAME;
            }

            return null;
        } else {
            String[] authChain = Config.authentication_chain;

            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            String securityType = null;
            for (String authMechanism : authChain) {
                if (authMechanism.equals(ConfigBase.AUTHENTICATION_CHAIN_MECHANISM_NATIVE)) {
                    continue;
                }

                SecurityIntegration securityIntegration = authenticationMgr.getSecurityIntegration(authMechanism);
                if (securityIntegration == null) {
                    continue;
                }

                if (securityType == null) {
                    securityType = securityIntegration.getType();
                } else if (!securityType.equalsIgnoreCase(securityIntegration.getType())) {
                    // There are multiple different types of security integration,
                    // abandon switch authPlugin and use the type specified by the client.
                    return null;
                }
            }

            if (securityType == null) {
                return null;
            }

            if (securityType.equalsIgnoreCase(OIDCSecurityIntegration.TYPE)) {
                return MysqlHandshakePacket.AUTHENTICATION_OPENID_CONNECT_CLIENT;
            }
        }

        return null;
    }

    private static MysqlAuthPacket readAuthPacket(ConnectContext context) throws IOException {
        // Server receive authenticate packet from client.
        ByteBuffer handshakeResponse = context.getMysqlChannel().fetchOnePacket();
        if (handshakeResponse == null) {
            // receive response failed.
            return null;
        }
        MysqlAuthPacket authPacket = new MysqlAuthPacket();
        if (!authPacket.readFrom(handshakeResponse)) {
            ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
            sendResponsePacket(context);
            return null;
        }
        return authPacket;
    }

    /**
     * Change user command use MySQL protocol
     * Exception:
     * IOException:
     */
    public static boolean changeUser(ConnectContext context, ByteBuffer buffer) throws IOException {
        // parse change user packet
        MysqlChangeUserPacket changeUserPacket = new MysqlChangeUserPacket(context.getCapability());
        if (!changeUserPacket.readFrom(buffer)) {
            ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
            sendResponsePacket(context);
            // reconstruct serializer with context capability
            context.getSerializer().setCapability(context.getCapability());
            return false;
        }
        // save previous user login info
        UserIdentity previousUserIdentity = context.getCurrentUserIdentity();
        Set<Long> previousRoleIds = context.getCurrentRoleIds();
        String previousQualifiedUser = context.getQualifiedUser();
        String previousResourceGroup = context.getSessionVariable().getResourceGroup();
        // do authenticate again

        try {
            AuthenticationHandler.authenticate(context, changeUserPacket.getUser(), context.getMysqlChannel().getRemoteIp(),
                    changeUserPacket.getAuthResponse(), context.getAuthDataSalt());
        } catch (AuthenticationException e) {
            LOG.warn("Command `Change user` failed, from [{}] to [{}]. ", previousQualifiedUser,
                    changeUserPacket.getUser());
            sendResponsePacket(context);
            // reconstruct serializer with context capability
            context.getSerializer().setCapability(context.getCapability());
            // recover from previous user login info
            context.getSessionVariable().setResourceGroup(previousResourceGroup);
            return false;
        }
        // set database
        String db = changeUserPacket.getDb();
        if (!Strings.isNullOrEmpty(db)) {
            try {
                context.changeCatalogDb(db);
            } catch (DdlException e) {
                LOG.error("Command `Change user` failed at stage changing db, from [{}] to [{}], err[{}] ",
                        previousQualifiedUser, changeUserPacket.getUser(), e.getMessage());
                sendResponsePacket(context);
                // reconstruct serializer with context capability
                context.getSerializer().setCapability(context.getCapability());
                // recover from previous user login info
                context.getSessionVariable().setResourceGroup(previousResourceGroup);
                context.setCurrentUserIdentity(previousUserIdentity);
                context.setCurrentRoleIds(previousRoleIds);
                context.setQualifiedUser(previousQualifiedUser);
                return false;
            }
        }
        LOG.info("Command `Change user` succeeded, from [{}] to [{}]. ", previousQualifiedUser,
                context.getQualifiedUser());
        return true;
    }

    public static byte readByte(ByteBuffer buffer) {
        return buffer.get();
    }

    public static int readInt1(ByteBuffer buffer) {
        return readByte(buffer) & 0XFF;
    }

    public static int readInt2(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8);
    }

    public static int readInt3(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8) | ((readByte(
                buffer) & 0xFF) << 16);
    }

    public static int readInt4(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8) | ((readByte(
                buffer) & 0xFF) << 16) | ((readByte(buffer) & 0XFF) << 24);
    }

    public static long readInt6(ByteBuffer buffer) {
        return (readInt4(buffer) & 0XFFFFFFFFL) | (((long) readInt2(buffer)) << 32);
    }

    public static long readInt8(ByteBuffer buffer) {
        return (readInt4(buffer) & 0XFFFFFFFFL) | (((long) readInt4(buffer)) << 32);
    }

    public static long readVInt(ByteBuffer buffer) {
        int b = readInt1(buffer);

        if (b < 251) {
            return b;
        }
        if (b == 252) {
            return readInt2(buffer);
        }
        if (b == 253) {
            return readInt3(buffer);
        }
        if (b == 254) {
            return readInt8(buffer);
        }
        if (b == 251) {
            throw new NullPointerException();
        }
        return 0;
    }

    public static byte[] readFixedString(ByteBuffer buffer, int len) {
        byte[] buf = new byte[len];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readEofString(ByteBuffer buffer) {
        byte[] buf = new byte[buffer.remaining()];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readLenEncodedString(ByteBuffer buffer) {
        long length = readVInt(buffer);
        byte[] buf = new byte[(int) length];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readNulTerminateString(ByteBuffer buffer) {
        int oldPos = buffer.position();
        int nullPos;
        for (nullPos = oldPos; nullPos < buffer.limit(); ++nullPos) {
            if (buffer.get(nullPos) == 0) {
                break;
            }
        }
        byte[] buf = new byte[nullPos - oldPos];
        buffer.get(buf);
        // skip null byte.
        buffer.get();
        return buf;
    }

    public static boolean isRemoteIPLocalhost(String remoteIP) {
        if (remoteIP == null) {
            return false;
        }
        //Using "String.contains" here because the remote IP address starts with a forward slash, like “/127.0.0.1”.
        return remoteIP.contains(LOCALHOST);
    }

    public record NegotiateResult(MysqlAuthPacket authPacket, NegotiateState state) {
    }
}
