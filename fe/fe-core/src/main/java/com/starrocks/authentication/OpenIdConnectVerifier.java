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

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

public class OpenIdConnectVerifier {

    public static void verify(String idToken,
                              String userName,
                              JWKSet jwkSet,
                              String principalFiled,
                              String[] requiredIssuer,
                              String[] requiredAudience) throws AuthenticationException {
        try {
            SignedJWT signedJWT = verifyJWT(idToken, jwkSet);
            JWTClaimsSet claims = signedJWT.getJWTClaimsSet();
            String jwtUserName = resolveClaimValue(claims, principalFiled);

            if (jwtUserName == null) {
                throw new AuthenticationException("Can not get specified principal " + principalFiled);
            }

            if (!jwtUserName.equalsIgnoreCase(userName)) {
                throw new AuthenticationException("Login name " + userName + " is not matched to user " + jwtUserName);
            }

            Date exp = claims.getExpirationTime();
            if (exp != null && exp.before(new Date())) {
                throw new AuthenticationException("JWT expired at " + exp);
            }

            if (requiredIssuer != null && requiredIssuer.length != 0 &&
                    !Arrays.asList(requiredIssuer).contains(claims.getIssuer())) {
                throw new AuthenticationException("Issuer (iss) field " + claims.getIssuer() + " is invalid");
            }

            if (requiredAudience != null && requiredAudience.length != 0 &&
                    !Arrays.stream(requiredAudience).anyMatch(claims.getAudience()::contains)) {
                throw new AuthenticationException("Audience (aud) field " + claims.getAudience() + " is invalid");
            }
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Resolves a claim value from the JWT claims set.
     * If {@code principalField} starts with {@code /}, it is treated as an
     * <a href="https://datatracker.ietf.org/doc/html/rfc6901">RFC 6901 JSON Pointer</a>
     * and evaluated using Jackson's {@link JsonPointer}. Otherwise, a flat
     * top-level lookup is performed for backward compatibility.
     */
    static String resolveClaimValue(JWTClaimsSet claims, String principalField) throws ParseException {
        if (!principalField.startsWith("/")) {
            return claims.getStringClaim(principalField);
        }

        JsonNode tree = MAPPER.valueToTree(claims.toJSONObject());
        JsonNode result = tree.at(JsonPointer.compile(principalField));
        return result.isTextual() ? result.textValue() : null;
    }

    private static SignedJWT verifyJWT(String jwt, JWKSet jwkSet) throws AuthenticationException, ParseException, JOSEException {
        Preconditions.checkNotNull(jwt);

        SignedJWT signedJWT = SignedJWT.parse(jwt);
        String kid = signedJWT.getHeader().getKeyID();

        JWK jwk = jwkSet.getKeyByKeyId(kid);
        if (jwk == null) {
            throw new AuthenticationException("Cannot find public key for kid: " + kid);
        }

        RSAPublicKey publicKey = jwk.toRSAKey().toRSAPublicKey();
        RSASSAVerifier verifier = new RSASSAVerifier(publicKey);
        if (!signedJWT.verify(verifier)) {
            throw new AuthenticationException("JWT with kid " + kid + " is invalid");
        }
        return signedJWT;
    }
}
