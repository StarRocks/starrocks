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

import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.a.NativeConstants;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;

import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * StarRocks 'authentication_oauth2_client' authentication plugin.
 */
public class AuthenticationOAuth2Client implements AuthenticationPlugin<NativePacketPayload> {
    public static String PLUGIN_NAME = "authentication_oauth2_client";

    private Long connectionId = null;
    private String sourceOfAuthData = PLUGIN_NAME;

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
        connectionId = prot.getServerSession().getCapabilities().getThreadId();
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean requiresConfidentiality() {
        return false;
    }

    @Override
    public boolean isReusable() {
        return false;
    }

    @Override
    public void setAuthenticationParameters(String user, String password) {
    }

    @Override
    public void setSourceOfAuthData(String sourceOfAuthData) {
        this.sourceOfAuthData = sourceOfAuthData;
    }

    @Override
    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();

        if (!this.sourceOfAuthData.equals(PLUGIN_NAME) || fromServer.getPayloadLength() == 0) {
            // Cannot do anything with whatever payload comes from the server,
            // so just skip this iteration and wait for a Protocol::AuthSwitchRequest or a Protocol::AuthNextFactor.
            return true;
        }

        // The URL a user’s browser will be redirected to in order to begin the OAuth2 authorization process
        int authServerUrlLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String authServerUrl =
                fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", authServerUrlLength);

        // The public identifier of the StarRocks client.
        int clientIdLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String clientId = fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", clientIdLength);

        // The URL to redirect to after OAuth2 authentication is successful.
        int redirectUrlLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String redirectUrl = fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", redirectUrlLength);

        // The connection ID of StarRocks must be included in the callback URL of OAuth2
        long connectionId = this.connectionId;

        String authUrl = authServerUrl +
                "?response_type=code" +
                "&client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8) +
                "&redirect_uri=" + URLEncoder.encode(redirectUrl, StandardCharsets.UTF_8) +
                "&state=" + connectionId +
                "&scope=openid";

        Desktop desktop = Desktop.getDesktop();
        try {
            desktop.browse(new URI(authUrl));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }

        NativePacketPayload packet = new NativePacketPayload(StringUtils.getBytes(""));
        packet.setPosition(packet.getPayloadLength());
        packet.writeInteger(NativeConstants.IntegerDataType.INT1, 0);
        packet.setPosition(0);

        toServer.add(packet);
        return true;
    }
}

