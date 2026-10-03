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

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.StringJoiner;

public class TokenSet implements Serializable {
    @SerializedName("access_token")
    private final String accessToken;
    @SerializedName("expires_in")
    private final long expiresIn;
    @SerializedName("id_token")
    private final String idToken;
    @SerializedName("refresh_token")
    private final String refreshToken;
    @SerializedName("scope")
    private final String scope;
    @SerializedName("token_type")
    private final String tokenType;
    @SerializedName("issued_token_type")
    private final String issuedTokenType;

    public TokenSet(String accessToken,
                    long expiresIn,
                    String idToken,
                    String refreshToken,
                    String scope,
                    String tokenType,
                    String issuedTokenType) {
        this.accessToken = accessToken;
        this.expiresIn = expiresIn;
        this.idToken = idToken;
        this.refreshToken = refreshToken;
        this.scope = scope;
        this.tokenType = tokenType;
        this.issuedTokenType = issuedTokenType;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public long getExpiresIn() {
        return expiresIn;
    }

    public String getIdToken() {
        return idToken;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public String getScope() {
        return scope;
    }

    public String getTokenType() {
        return tokenType;
    }

    public String getIssuedTokenType() {
        return issuedTokenType;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TokenSet.class.getSimpleName() + "[", "]")
                .add("accessToken='[REDACTED]'")
                .add("expiresIn='" + expiresIn + "'")
                .add("idToken='[REDACTED]'")
                .add("refreshToken='[REDACTED]'")
                .add("scope='" + scope + "'")
                .add("tokenType='" + tokenType + "'")
                .add("issuedTokenType='" + issuedTokenType + "'")
                .toString();
    }
}
