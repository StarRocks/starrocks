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


package com.starrocks.http;

import com.starrocks.authentication.AuthChannel;
import com.starrocks.authentication.AuthCredentialKind;
import com.starrocks.authentication.AuthCredentials;
import com.starrocks.authentication.AuthFailureReason;
import com.starrocks.metric.MetricRepo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BasicActionTest {

    /**
     * TC-L3-46-01: the Basic branch must produce a PLAIN_PASSWORD credential tagged with the HTTP
     * channel, and must strip the legacy {@code user@cluster_id} suffix the same way it always did.
     */
    @Test
    public void testParseBasicCredential() {
        AuthCredentials cred = parseBasic("abc", "123", "127.0.0.1");
        assertEquals(AuthCredentialKind.PLAIN_PASSWORD, cred.kind());
        assertEquals(AuthChannel.HTTP, cred.channel());
        assertEquals("abc", cred.userDeclared());
        assertEquals("127.0.0.1", cred.remoteHost());
        assertEquals("123", cred.tokenString());

        cred = parseBasic("test@cluster_id", "", "192.168.19.10");
        assertEquals("test", cred.userDeclared());
        assertEquals("", cred.tokenString());
        assertEquals("192.168.19.10", cred.remoteHost());
    }

    /** TC-L3-46-02: the Bearer branch carries no declared user - the username comes from the token. */
    @Test
    public void testParseBearerCredential() {
        AuthCredentials cred = new BaseAction(null) {
            @Override
            public void execute(BaseRequest request, BaseResponse response) {
            }
        }.parseAuthCredentials(mockRequest("Bearer header.payload.signature", "10.1.2.3"));
        assertEquals(AuthCredentialKind.BEARER_TOKEN, cred.kind());
        assertEquals(AuthChannel.HTTP, cred.channel());
        Assertions.assertNull(cred.userDeclared());
        assertEquals("header.payload.signature", cred.tokenString());
    }

    /** TC-L3-46-03: missing header, unknown scheme and malformed Basic all yield null, i.e. a 401. */
    @Test
    public void testParseRejectsUnusableHeaders() {
        BaseAction action = new BaseAction(null) {
            @Override
            public void execute(BaseRequest request, BaseResponse response) {
            }
        };
        Assertions.assertNull(action.parseAuthCredentials(mockRequest(null, "10.1.2.3")));
        Assertions.assertNull(action.parseAuthCredentials(mockRequest("Foo xxx", "10.1.2.3")));
        Assertions.assertNull(action.parseAuthCredentials(mockRequest("Basic", "10.1.2.3")));
        // Base64 of "nocolon" - no ':' separator, so user and password cannot be split.
        Assertions.assertNull(action.parseAuthCredentials(mockRequest("Basic bm9jb2xvbg==", "10.1.2.3")));
        // Not Base64 at all: refused the same way, not with the decoder's IllegalArgumentException.
        Assertions.assertNull(action.parseAuthCredentials(mockRequest("Basic !!!not-base64!!!", "10.1.2.3")));
    }

    @Test
    public void testUnparsableBasicHeaderCountsAsMalformedFailure() {
        BaseAction action = new BaseAction(null) {
            @Override
            public void execute(BaseRequest request, BaseResponse response) {
            }
        };
        boolean savedInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            // A Basic header that yields no user and password is a failed login: one MALFORMED
            // failure each. "Basic " decodes to an empty string; bm9jb2xvbg== is "nocolon".
            for (String header : new String[] {"Basic", "Basic ", "Basic bm9jb2xvbg==",
                    "Basic !!!not-base64!!!"}) {
                long reasons = malformedFailures();
                long failures = httpPasswordFailures();
                Assertions.assertNull(action.parseAuthCredentials(mockRequest(header, "10.1.2.3")));
                assertEquals(reasons + 1, malformedFailures(), header);
                assertEquals(failures + 1, httpPasswordFailures(), header);
            }
            // No header at all, or a scheme we do not speak, is no login attempt: nothing counted.
            for (String header : new String[] {null, "Foo xxx"}) {
                long reasons = malformedFailures();
                Assertions.assertNull(action.parseAuthCredentials(mockRequest(header, "10.1.2.3")));
                assertEquals(reasons, malformedFailures(), String.valueOf(header));
            }
        } finally {
            MetricRepo.hasInit = savedInit;
        }
    }

    private static long malformedFailures() {
        return MetricRepo.COUNTER_AUTH_FAILURE_REASON.getMetric(AuthFailureReason.MALFORMED.label())
                .getValue();
    }

    private static long httpPasswordFailures() {
        return MetricRepo.COUNTER_AUTH_FAILURE.getMetric("http", "plain_password").getValue();
    }

    private static AuthCredentials parseBasic(String user, String password, String host) {
        String encoded = Base64.getEncoder().encodeToString(
                (user + ":" + password).getBytes(StandardCharsets.UTF_8));
        return new BaseAction(null) {
            @Override
            public void execute(BaseRequest request, BaseResponse response) {
            }
        }.parseAuthCredentials(mockRequest("Basic " + encoded, host));
    }

    private static BaseRequest mockRequest(String authHeader, String host) {
        BaseRequest request = mock(BaseRequest.class);
        when(request.getAuthorizationHeader()).thenReturn(authHeader);
        when(request.getHostString()).thenReturn(host);
        return request;
    }

    @Test
    public void testSanitizeHttpReqUri() throws Exception {
        String uri = "/api/bootstrap?cluster_id=123124&token=98sdf-80sdf";
        String result = WebUtils.sanitizeHttpReqUri(uri);
        System.out.println(result);
        Assertions.assertTrue(result.contains("token=*"));
        Assertions.assertTrue(result.contains("cluster_id=*"));

        uri = "/api/get_small_file?file_id=123124";
        result = WebUtils.sanitizeHttpReqUri(uri);
        Assertions.assertTrue(result.contains("file_id=*"));

        uri = "/api/metrics?all=true";
        result = WebUtils.sanitizeHttpReqUri(uri);
        Assertions.assertEquals(uri, result);

        Assertions.assertEquals("/api/metrics", WebUtils.sanitizeHttpReqUri("/api/metrics"));
    }

    @Test
    public void testSanitizeHttpReqUriParsesQueryAsActionsDo() throws Exception {
        // Each of these passes token=TopSecret to the action, which parses the query with QueryStringDecoder: ';' is
        // a separator too, a leading '=' in a name is skipped, and an escaped '&' is part of the value.
        String[] uris = {
                "/api/bootstrap?label=1;token=TopSecret",
                "/api/bootstrap?label=1&=token=TopSecret",
                "/api/bootstrap?label=1&token=Top%26Secret",
        };
        for (String uri : uris) {
            Assertions.assertEquals("/api/bootstrap?label=1&token=*", WebUtils.sanitizeHttpReqUri(uri), uri);
        }

        // The other parameters keep their order, and are encoded again; what is not parsed as a parameter, like the
        // fragment, is left out.
        Assertions.assertEquals("/api/get_small_file?label=a+b&file_id=*&flag=",
                WebUtils.sanitizeHttpReqUri("/api/get_small_file?label=a%20b&file_id=1&flag"));
        Assertions.assertEquals("http://fe:8030/api/bootstrap?token=*&token=*",
                WebUtils.sanitizeHttpReqUri("http://fe:8030/api/bootstrap?token=a&token=b#Secret"));
    }

}
