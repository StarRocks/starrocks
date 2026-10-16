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
import com.starrocks.authentication.AuthChannel;
import com.starrocks.authentication.AuthCredentialKind;
import com.starrocks.authentication.AuthCredentials;
import com.starrocks.authentication.AuthFailureReason;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.AuthenticationProviderFactory;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.ssl.SSLContextLoader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
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

        byte[] randomString = MysqlPassword.createRandomString();
        context.setAuthDataSalt(randomString);

        MysqlHandshakePacket handshakePacket = new MysqlHandshakePacket(context.getConnectionId(),
                SSLContextLoader.getSslContext() != null, randomString);
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
        try {
            switchAuthPlugin(authPacket, context);
        } catch (AuthenticationException e) {
            // receive response failed.
            LOG.warn("read auth switch response failed for user {}", authPacket.getUser());
            return new NegotiateResult(authPacket, NegotiateState.READ_AUTH_SWITCH_PKG_FAILED);
        }

        // Set the real user used for this connection.
        context.setQualifiedUser(authPacket.getUser());
        context.setAuthPlugin(authPacket.getPluginName());
        // change the capability of serializer
        context.setCapability(context.getServerCapability());
        serializer.setCapability(context.getCapability());

        return new NegotiateResult(authPacket, NegotiateState.OK);
    }

    /**
     * Decode a MySQL handshake frame into a protocol-neutral credential.
     *
     * <p>This is the one place that needs to know which auth plugin the handshake negotiated, and
     * it is deliberately the only one: the plugin name is read here and thrown away, never stored
     * on the credential, and {@code AuthenticationHandler} never sees it. Two of the five frames
     * have an exact counterpart on the other channels - a clear password is a password, an OpenID
     * Connect frame wraps a token - so they are unwrapped here and travel as {@code PLAIN_PASSWORD}
     * / {@code BEARER_TOKEN}. Providers downstream then have a single path per credential shape
     * instead of a MySQL branch each, and the security-integration chain can filter purely on what
     * a provider says it can read.
     *
     * <p>{@code clientPlugin} comes from the packet that carried {@code authResponse}, not from the
     * context: the bytes and the name of the format they are in belong together, and reading the
     * name from a field a different method happens to have filled is how it goes stale.
     *
     * <p>An unrecognised or absent plugin name falls through to {@code MYSQL_NATIVE_SCRAMBLE}: an old
     * client that sends no plugin name is speaking {@code mysql_native_password}, and guessing any
     * other shape for a name we do not know would read the bytes as something they are not.
     */
    public static AuthCredentials handshakeCredentials(String user, byte[] authResponse, String clientPlugin,
                                                       String remoteHost) throws AuthenticationException {
        if (AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString().equalsIgnoreCase(clientPlugin)) {
            // The clear-password frame appends a trailing NUL; every other channel sends the bare
            // password, so strip it here and the difference stops existing.
            byte[] password = authResponse;
            if (password.length > 0 && password[password.length - 1] == 0) {
                password = Arrays.copyOf(password, password.length - 1);
            }
            return new AuthCredentials(user, password, AuthCredentialKind.PLAIN_PASSWORD,
                    AuthChannel.MYSQL, remoteHost);
        }
        if (AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString().equalsIgnoreCase(clientPlugin)) {
            byte[] token;
            try {
                ByteBuffer buffer = ByteBuffer.wrap(authResponse);
                MysqlCodec.readInt1(buffer);            // 1 byte of client capability
                token = MysqlCodec.readLenEncodedString(buffer);
            } catch (RuntimeException e) {
                throw new AuthenticationException("Malformed JWT")
                        .withReason(AuthFailureReason.MALFORMED);
            }
            return new AuthCredentials(user, token, AuthCredentialKind.BEARER_TOKEN,
                    AuthChannel.MYSQL, remoteHost);
        }
        if (AuthPlugin.Client.AUTHENTICATION_OAUTH2_CLIENT.toString().equalsIgnoreCase(clientPlugin)) {
            // The client declared the OAuth2 plugin. There are no credential bytes - on MySQL the
            // token always arrives later, through the browser callback - and the kind records that
            // this client received the authorization endpoint and can open the browser itself, so
            // the provider may hold the handshake until the callback lands. A client without the
            // plugin sends a scramble and falls through to MYSQL_NATIVE_SCRAMBLE below; the OAuth2
            // provider answers that one immediately and lets the first statement carry the URL.
            return new AuthCredentials(user, new byte[0],
                    AuthCredentialKind.MYSQL_OAUTH2_CLIENT, AuthChannel.MYSQL, remoteHost);
        }
        // mysql_native_password, and an old client that names no plugin - which is speaking it too.
        return AuthCredentials.ofNativeScramble(user, authResponse, remoteHost);
    }

    public static NegotiateResult authenticate(ConnectContext context, MysqlAuthPacket authPacket) throws IOException {
        try {
            AuthCredentials cred = handshakeCredentials(authPacket.getUser(), authPacket.getAuthResponse(),
                    authPacket.getPluginName(), context.getMysqlChannel().getRemoteIp());
            AuthenticationHandler.authenticate(context, cred);
        } catch (AuthenticationException e) {
            if (e.getErrorCode() == null) {
                context.getState().setErrorCode(ErrorCode.ERR_AUTHENTICATION_FAIL);
                context.getState().setError(e.getMessage());
            } else {
                context.getState().setErrorCode(e.getErrorCode());
                context.getState().setError(e.getMessage());
            }

            sendResponsePacket(context);
            return new NegotiateResult(authPacket, NegotiateState.AUTHENTICATION_FAILED);
        }

        // set database
        String db = authPacket.getDb();
        if (!Strings.isNullOrEmpty(db)) {
            try {
                context.changeCatalogDb(db);
            } catch (Exception e) {
                LOG.warn("Set database [{}] failed during negotiate, user={}, reason={}",
                        db, authPacket.getUser(), e.getMessage(), e);
                if (!context.getState().isError()) {
                    context.getState().setError(e.getMessage());
                }
                sendResponsePacket(context);
                return new NegotiateResult(authPacket, NegotiateState.SET_DATABASE_FAILED);
            }
        }

        return new NegotiateResult(authPacket, NegotiateState.OK);
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
        String previousCatalog = context.getCurrentCatalog();
        String previousDb = context.getDatabase();
        // do authenticate again

        try {
            AuthCredentials cred = handshakeCredentials(changeUserPacket.getUser(), changeUserPacket.getAuthResponse(),
                    changeUserPacket.getPluginName(), context.getMysqlChannel().getRemoteIp());
            AuthenticationHandler.authenticate(context, cred);
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
            } catch (Exception e) {
                LOG.warn("Command `Change user` failed at stage changing db, from [{}] to [{}], err[{}] ",
                        previousQualifiedUser, changeUserPacket.getUser(), e.getMessage(), e);
                if (!context.getState().isError()) {
                    context.getState().setError(e.getMessage());
                }
                sendResponsePacket(context);
                // reconstruct serializer with context capability
                context.getSerializer().setCapability(context.getCapability());
                context.setCurrentCatalog(previousCatalog);
                context.setDatabase(previousDb);
                // recover from previous user login info
                context.getSessionVariable().setResourceGroup(previousResourceGroup);
                context.setCurrentUserIdentity(previousUserIdentity);
                context.setCurrentRoleIds(previousRoleIds);
                context.setQualifiedUser(previousQualifiedUser);
                return false;
            }
        }
        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
        Pair<Boolean, String> userChangeResult = connectScheduler.onUserChanged(
                context, previousQualifiedUser, context.getQualifiedUser());
        if (!userChangeResult.first) {
            context.getState().setErrorCode(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS);
            context.getState().setError(userChangeResult.second);
            sendResponsePacket(context);
            context.getSerializer().setCapability(context.getCapability());
            context.getSessionVariable().setResourceGroup(previousResourceGroup);
            context.setCurrentUserIdentity(previousUserIdentity);
            context.setCurrentRoleIds(previousRoleIds);
            context.setQualifiedUser(previousQualifiedUser);
            context.setCurrentCatalog(previousCatalog);
            context.setDatabase(previousDb);
            return false;
        }

        LOG.info("Command `Change user` succeeded, from [{}] to [{}]. ", previousQualifiedUser,
                context.getQualifiedUser());
        return true;
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

    public static void switchAuthPlugin(MysqlAuthPacket mysqlAuthPacket, ConnectContext context)
            throws AuthenticationException, IOException {
        String user = mysqlAuthPacket.getUser();
        String authPluginName = mysqlAuthPacket.getPluginName();

        // Older version mysql client does not send auth plugin info, like 5.1 version.
        if (authPluginName == null) {
            return;
        }

        String switchAuthPlugin = null;
        AuthenticationProvider provider = null;

        Map.Entry<UserIdentity, UserAuthenticationInfo> localUser = GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                .getBestMatchedUserIdentity(user, context.getMysqlChannel().getRemoteIp());
        if (localUser != null) {
            UserAuthenticationInfo authInfo = localUser.getValue();
            switchAuthPlugin = AuthPlugin.covertFromServerToClient(authInfo.getAuthPlugin());
            provider = AuthenticationProviderFactory.create(authInfo.getAuthPlugin(), authInfo.getAuthString());
        } else {
            for (String authMechanism : Config.authentication_chain) {
                if (authMechanism.equals(SecurityIntegration.AUTHENTICATION_CHAIN_MECHANISM_NATIVE)) {
                    continue;
                }

                //Because we only support Security Integration of the same type, we use the first non-Native type here.
                SecurityIntegration securityIntegration =
                        GlobalStateMgr.getCurrentState().getAuthenticationMgr().getSecurityIntegration(authMechanism);
                switchAuthPlugin = AuthPlugin.covertFromServerToClient(securityIntegration.getType());
                provider = securityIntegration.getAuthenticationProvider();
                break;
            }
        }

        if (provider == null) {
            return;
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        if (!authPluginName.equalsIgnoreCase(switchAuthPlugin)) {
            // AuthSwitchRequest Packet
            MysqlCodec.writeInt1(outputStream, (byte) 0xfe);
            MysqlCodec.writeNulTerminateString(outputStream, switchAuthPlugin);

            byte[] authSwitchRequestPacket =
                    provider.authSwitchRequestPacket(context.getAccessControlContext(), user,
                            context.getMysqlChannel().getRemoteIp());
            if (authSwitchRequestPacket != null) {
                MysqlCodec.writeBytes(outputStream, authSwitchRequestPacket);
            }
            MysqlCodec.writeInt1(outputStream, 0);
        } else {
            // AuthMoreData Packet
            byte[] authMoreDataPacket = provider.authMoreDataPacket(context.getAccessControlContext(), user,
                    context.getMysqlChannel().getRemoteIp());
            if (authMoreDataPacket != null) {
                MysqlCodec.writeInt1(outputStream, (byte) 0x01);
                MysqlCodec.writeBytes(outputStream, authMoreDataPacket);
            }
        }

        // mysql client may not support starrocks custom plugin, so it does not send a switch packet.
        if (!authPluginName.equalsIgnoreCase(switchAuthPlugin) && AuthPlugin.isStarRocksCustomAuthPlugin(switchAuthPlugin)) {
            return;
        }

        if (outputStream.size() > 0) {
            MysqlChannel channel = context.getMysqlChannel();
            channel.sendAndFlush(ByteBuffer.wrap(outputStream.toByteArray()));
            ByteBuffer authSwitchResponse = channel.fetchOnePacket();
            if (authSwitchResponse == null) {
                throw new AuthenticationException("read auth switch response failed for user " + user);
            }
            mysqlAuthPacket.setPluginName(switchAuthPlugin);
            mysqlAuthPacket.setAuthResponse(MysqlCodec.readEofString(authSwitchResponse));
        }
    }
}
