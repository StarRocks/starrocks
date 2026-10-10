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

package com.starrocks.service.arrow.flight.sql.session;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ArrowFlightSqlSessionManagerJwtTest {

    private static String signedJwt() throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("test_user")
                .issuer("https://issuer.example.com")
                .audience("starrocks")
                .build();
        SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
        jwt.sign(new MACSigner("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8)));
        return jwt.serialize();
    }

    @Test
    public void testIsJwt() throws Exception {
        Assertions.assertTrue(ArrowFlightSqlSessionManager.isJwt(signedJwt()));
        Assertions.assertFalse(ArrowFlightSqlSessionManager.isJwt("testPassword"));
        Assertions.assertFalse(ArrowFlightSqlSessionManager.isJwt("a.b.c"));
        Assertions.assertFalse(ArrowFlightSqlSessionManager.isJwt(""));
    }

    @Test
    public void testJwtPasswordIsAuthenticatedAsIdToken() throws Exception {
        List<String> calls = new ArrayList<>();
        // Plain UUID tokens: with proxy mode the token is prefixed with this FE's host, which a unit test lacks.
        new MockUp<GlobalVariable>() {
            @Mock
            public boolean isArrowFlightProxyEnabled() {
                return false;
            }
        };
        new MockUp<AuthenticationHandler>() {
            @Mock
            public UserIdentity authenticateWithIdToken(ConnectContext context, String user, String remoteHost,
                                                        String idToken) throws AuthenticationException {
                calls.add("idToken");
                throw new AuthenticationException("stop here");
            }

            @Mock
            public UserIdentity authenticateWithClearPassword(ConnectContext context, String user, String remoteHost,
                                                              String password) throws AuthenticationException {
                calls.add("clearPassword");
                throw new AuthenticationException("stop here");
            }
        };
        ArrowFlightSqlSessionManager sessionManager = new ArrowFlightSqlSessionManager();

        Assertions.assertThrows(FlightRuntimeException.class,
                () -> sessionManager.initializeSession("test_user", "10.1.2.3", signedJwt()));
        Assertions.assertThrows(FlightRuntimeException.class,
                () -> sessionManager.initializeSession("test_user", "10.1.2.3", "testPassword"));
        Assertions.assertEquals(List.of("idToken", "clearPassword"), calls);
    }
}
