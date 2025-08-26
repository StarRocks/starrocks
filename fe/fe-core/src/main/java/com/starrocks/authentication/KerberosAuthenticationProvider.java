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
import com.starrocks.common.Config;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;

public class KerberosAuthenticationProvider implements AuthenticationProvider {
    // use to construct an Oid object from a string representation of its integer components.
    public static String NT_GSS_KRB5_PRINCIPAL_OID = "1.2.840.113554.1.2.2.1";
    public static String GSS_KRB5_MECH_OID = "1.2.840.113554.1.2.2";

    private final String userForAuthPlugin;

    public KerberosAuthenticationProvider(String userForAuthPlugin) {
        this.userForAuthPlugin = userForAuthPlugin;
    }

    @Override
    public void authenticate(ConnectContext context, UserIdentity userIdentity, byte[] authResponse)
            throws AuthenticationException {
        try {
            String spn = Config.authentication_kerberos_service_principal;
            String keytab = Config.authentication_kerberos_service_key_tab;
            String remoteUser = userIdentity.getUser() + "@" + userForAuthPlugin;

            GSSManager gssManager;
            Subject serverSubject = new Subject();
            File keytabFile = new File(keytab);
            if (!keytabFile.exists()) {
                throw new FileNotFoundException("Can not find file " + keytab);
            }

            Principal krbPrincipal = new KerberosPrincipal(spn);
            serverSubject.getPrincipals().add(krbPrincipal);

            KeyTab keytabInstance = KeyTab.getInstance(keytabFile);
            serverSubject.getPrivateCredentials().add(keytabInstance);

            try {
                gssManager = Subject.doAs(serverSubject, (PrivilegedExceptionAction<GSSManager>) GSSManager::getInstance);
            } catch (PrivilegedActionException ex) {
                throw ex.getException();
            }

            boolean result = Subject.doAs(serverSubject,
                    (PrivilegedExceptionAction<Boolean>) () -> runWithPrincipal(spn, authResponse, remoteUser, gssManager));

            if (!result) {
                throw new AuthenticationException(
                        "Failed to authenticate for [user: " + userIdentity.getUser() + "] by kerberos");
            }
        } catch (Exception e) {
            throw new AuthenticationException("Failed to authenticate for [user: " + userIdentity.getUser() + "] " +
                    "by kerberos, msg: " + e.getMessage());
        }
    }

    private static boolean runWithPrincipal(String spn,
                                            byte[] clientToken,
                                            String remoteUser,
                                            GSSManager gssManager) throws Exception {
        GSSContext gssContext = null;
        GSSCredential gssCreds = null;
        try {
            GSSName gssName = gssManager.createName(spn, new Oid(NT_GSS_KRB5_PRINCIPAL_OID));
            gssCreds = gssManager.createCredential(gssName,
                    GSSCredential.INDEFINITE_LIFETIME,
                    new Oid[] {new Oid(GSS_KRB5_MECH_OID)},
                    GSSCredential.ACCEPT_ONLY);
            gssContext = gssManager.createContext(gssCreds);
            gssContext.acceptSecContext(clientToken, 0, clientToken.length);

            if (!gssContext.isEstablished()) {
                throw new Exception("Failed to establish connection");
            }

            // Compare input upn with the actual one. Return success if the names match exactly.
            String clientPrincipal = gssContext.getSrcName().toString();
            if (clientPrincipal.equals(remoteUser)) {
                return true;
            } else {
                throw new Exception(String.format("User %s decrypted from token doesn't equal remote user %s",
                        remoteUser, clientPrincipal));
            }
        } finally {
            if (gssContext != null) {
                gssContext.dispose();
            }
            if (gssCreds != null) {
                gssCreds.dispose();
            }
        }
    }

    @Override
    public byte[] authMoreDataPacket(ConnectContext context, String user, String host) throws AuthenticationException {
        try {
            Map.Entry<UserIdentity, UserAuthenticationInfo> authenticationInfo =
                    GlobalStateMgr.getCurrentState().getAuthenticationMgr().getBestMatchedUserIdentity(user, host);
            if (authenticationInfo == null) {
                String msg = String.format("Can not find kerberos authentication with [user: %s, remoteIp: %s].", user, host);
                throw new Exception(msg);
            }
            String userRealm = authenticationInfo.getValue().getAuthString();
            String spnString = Config.authentication_kerberos_service_principal;

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] spn = spnString.getBytes(StandardCharsets.UTF_8);
            MysqlCodec.writeInt2(outputStream, spn.length);
            MysqlCodec.writeBytes(outputStream, spn);
            byte[] upnRealm = userRealm.getBytes(StandardCharsets.UTF_8);
            MysqlCodec.writeInt2(outputStream, upnRealm.length);
            MysqlCodec.writeBytes(outputStream, upnRealm);

            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
    }

    @Override
    public byte[] authSwitchRequestPacket(ConnectContext context, String user, String host) throws AuthenticationException {
        return authMoreDataPacket(context, user, host);
    }
}
