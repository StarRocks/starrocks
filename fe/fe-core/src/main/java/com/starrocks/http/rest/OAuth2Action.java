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

package com.starrocks.http.rest;

import com.nimbusds.jose.jwk.JWKSet;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.OAuth2AuthenticationProvider;
import com.starrocks.authentication.OAuth2Context;
import com.starrocks.authentication.OAuth2ResultMessage;
import com.starrocks.authentication.OpenIdConnectVerifier;
import com.starrocks.common.Config;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.StarRocksHttpClient;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.system.Frontend;
import io.netty.handler.codec.http.HttpMethod;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

public class OAuth2Action extends RestBaseAction {
    public OAuth2Action(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/oauth2", new OAuth2Action(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String authorizationCode = getSingleParameter(request, "code", r -> r);
        String connectionIdStr = getSingleParameter(request, "state", r -> r);
        long connectionId = Long.parseLong(connectionIdStr);

        NodeMgr nodeMgr = GlobalStateMgr.getCurrentState().getNodeMgr();
        int fid = (int) ((connectionId >> 24) & 0xFF);
        Frontend frontend = GlobalStateMgr.getCurrentState().getNodeMgr().getFrontend(fid);
        if (!nodeMgr.getMySelf().getNodeName().equals(frontend.getNodeName())) {
            String responseContent =
                    StarRocksHttpClient.redirect(frontend.getHost(), Config.http_port, request.getRequest());
            response.appendContent(responseContent);
            sendResult(request, response);
            return;
        }

        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
        ConnectContext context = connectScheduler.getContext(connectionId);
        if (context == null) {
            response.appendContent(OAuth2ResultMessage.generateLoginSuccessPage(
                    "Failed", "Not found connect context " + connectionId, "", String.valueOf(connectionId)));
            sendResult(request, response);
            return;
        }

        AuthenticationProvider authenticationProvider = context.getAuthenticationProvider();
        if (!(authenticationProvider instanceof OAuth2AuthenticationProvider)) {
            response.appendContent(OAuth2ResultMessage.generateLoginSuccessPage(
                    "Failed", "The authentication type is not OAuth2",
                    context.getQualifiedUser(), String.valueOf(connectionId)));
            sendResult(request, response);
            return;
        }

        try {
            OAuth2Context oAuth2Context = ((OAuth2AuthenticationProvider) authenticationProvider).getoAuth2Context();

            String oidcToken = getToken(authorizationCode, oAuth2Context, connectionId);
            JWKSet jwkSet = GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSet(oAuth2Context.jwksUrl());

            JSONObject authResponse = new JSONObject(oidcToken);
            String accessToken = authResponse.getString("access_token");
            String idToken = authResponse.getString("id_token");

            OpenIdConnectVerifier.verify(idToken, context.getQualifiedUser(), jwkSet,
                    oAuth2Context.principalFiled(),
                    oAuth2Context.requiredIssuer(),
                    oAuth2Context.requiredAudience());

            context.setAuthToken(idToken);

            response.appendContent(OAuth2ResultMessage.generateLoginSuccessPage(
                    "Successful",
                    "OAuth2 authentication successful, welcome to StarRocks.",
                    context.getQualifiedUser(), String.valueOf(connectionId)));
            sendResult(request, response);
        } catch (Exception e) {
            response.appendContent(OAuth2ResultMessage.generateLoginSuccessPage(
                    "Failed", e.getMessage(), context.getQualifiedUser(), String.valueOf(connectionId)));
            sendResult(request, response);
        }
    }

    public static String getToken(String authorizationCode, OAuth2Context oAuth2Context, Long connectionId)
            throws AuthenticationException {
        Map<Object, Object> postParams = Map.of(
                "grant_type", "authorization_code",
                "code", authorizationCode,
                "redirect_uri", oAuth2Context.redirectUrl(),
                "state=", connectionId,
                "client_id", oAuth2Context.clientId(),
                "client_secret", oAuth2Context.clientSecret()
        );

        HttpResponse<String> response;
        try {
            String requestBody = postParams.entrySet().stream()
                    .map(entry -> URLEncoder.encode(entry.getKey().toString(), StandardCharsets.UTF_8) + "=" +
                            URLEncoder.encode(entry.getValue().toString(), StandardCharsets.UTF_8))
                    .collect(Collectors.joining("&"));

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(oAuth2Context.tokenServerUrl()))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return response.body();
            } else {
                throw new AuthenticationException(response.statusCode() + " " + response.body());
            }
        } catch (InterruptedException | IOException e) {
            throw new AuthenticationException(e.getMessage());
        }
    }
}
