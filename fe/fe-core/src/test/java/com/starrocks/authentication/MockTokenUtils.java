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

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.text.ParseException;
import java.util.Base64;
import java.util.Date;

public class MockTokenUtils {
    String getOpenIdConnect(String fileName) throws IOException {
        String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath();
        File file = new File(path + "/" + fileName);
        BufferedReader reader = new BufferedReader(new FileReader(file));

        StringBuilder sb = new StringBuilder();
        String tempStr;
        while ((tempStr = reader.readLine()) != null) {
            sb.append(tempStr);
        }

        return sb.toString();
    }

    String generateTestOIDCToken(long validity) throws Exception {
        MockJwkMgr mockJwkMgr = new MockJwkMgr();
        RSASSASigner signer = new RSASSASigner(mockJwkMgr.loadPrivateKey("jwks-private-key.pem"));
        JWKSet jwkSet = mockJwkMgr.getJwkSet("signer-jwks.json");

        JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                .issuer("http://localhost:38080/realms/master")
                .subject("8f7f0fa5-e1eb-45d0-8e82-8c89c1a45663")
                .audience("12345")
                .issueTime(new Date())
                .expirationTime(new Date(new Date().getTime() + validity))
                .claim("preferred_username", "harbor")
                .build();

        JWSHeader.Builder headerBuilder = new JWSHeader.Builder(JWSAlgorithm.RS256)
                .keyID(jwkSet.getKeys().get(0).getKeyID());
        SignedJWT signedJWT = new SignedJWT(
                new JWSHeader(headerBuilder.build()),
                claimsSet);
        signedJWT.sign(signer);

        return signedJWT.serialize();
    }

    static class MockJwkMgr extends JwkMgr {
        @Override
        public JWKSet getJwkSet(String jwksUrl) throws IOException, ParseException {
            String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath();
            InputStream jwksInputStream = new FileInputStream(path + "/" + jwksUrl);
            return JWKSet.load(jwksInputStream);
        }

        private static RSAPrivateKey loadPrivateKey(String privateKeyPem) throws IOException {
            String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath();
            try (FileInputStream fis = new FileInputStream(path + "/" + privateKeyPem)) {
                String keyContent = new String(fis.readAllBytes());
                String privateKeyPEM = keyContent
                        .replace("-----BEGIN PRIVATE KEY-----", "")
                        .replace("-----END PRIVATE KEY-----", "")
                        .replaceAll("\\R", "");
                byte[] decodedKey = Base64.getDecoder().decode(privateKeyPEM);

                PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decodedKey);
                KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                return (RSAPrivateKey) keyFactory.generatePrivate(keySpec);
            } catch (Exception e) {
                throw new IOException("Failed to load private key", e);
            }
        }
    }
}