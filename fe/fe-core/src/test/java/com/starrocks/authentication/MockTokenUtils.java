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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
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
import java.util.Map;

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

    public String generateTestOIDCToken(long validity) throws Exception {
        return generateTestOIDCToken(validity, "12345");
    }

    /** Same signer and claims as {@link #generateTestOIDCToken(long)}, with the audience chosen. */
    public String generateTestOIDCToken(long validity, String audience) throws Exception {
        return generateTestOIDCToken(validity, audience, "http://localhost:38080/realms/master");
    }

    /** Same signer and claims as {@link #generateTestOIDCToken(long)}, but with no {@code iss} claim at all. */
    public String generateTestOIDCTokenWithoutIssuer(long validity) throws Exception {
        return generateTestOIDCToken(validity, "12345", null);
    }

    // issuer null: the token carries no iss claim.
    private String generateTestOIDCToken(long validity, String audience, String issuer) throws Exception {
        MockJwkMgr mockJwkMgr = new MockJwkMgr();
        RSASSASigner signer = new RSASSASigner(mockJwkMgr.loadPrivateKey("jwks-private-key.pem"));
        JWKSet jwkSet = mockJwkMgr.getJwkSet("signer-jwks.json");

        JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                .issuer(issuer)
                .subject("8f7f0fa5-e1eb-45d0-8e82-8c89c1a45663")
                .audience(audience)
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

    /** Generate a fresh RSA JWK (with private material) for a given kid. */
    public static RSAKey newRsaKey(String kid) throws JOSEException {
        return new RSAKeyGenerator(2048).keyID(kid).generate();
    }

    /** Serialize the public half of an RSA JWK as a JWKS document (for {@link MockJwksServer}). */
    public String publicJwksJson(RSAKey key) {
        return new JWKSet(key.toPublicJWK()).toString();
    }

    /** Sign an RS256 token (iss configurable, aud=12345, preferred_username=harbor) with the given RSA key. */
    public String signRs256(RSAKey key, String issuer, long validityMs) throws Exception {
        JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                .issuer(issuer)
                .subject("8f7f0fa5-e1eb-45d0-8e82-8c89c1a45663")
                .audience("12345")
                .issueTime(new Date())
                .expirationTime(new Date(new Date().getTime() + validityMs))
                .claim("preferred_username", "harbor")
                .build();
        SignedJWT signedJWT = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claimsSet);
        signedJWT.sign(new RSASSASigner(key));
        return signedJWT.serialize();
    }

    /**
     * A well-formed RS256 token whose {@code iss} can be forged to match a target SI, but signed by
     * a key NOT present in the SI's JWKS. Routing by iss selects the SI, then signature
     * verification must fail (threat T1: a forged iss only routes to the wrong/right SI, never
     * bypasses verification).
     */
    public String generateTokenSignedByUnknownKey(String issuer, long validityMs) throws Exception {
        return signRs256(newRsaKey("unknown-kid"), issuer, validityMs);
    }

    /**
     * The classic RS256 -> HS256 algorithm-confusion token: HS256-signed using the RSA *public* key
     * bytes as the HMAC secret. A correct verifier must reject it because HS256 is not in the
     * allowed RSA/EC set (threat T7 / O6).
     */
    public String generateHmacConfusionToken(String issuer) throws Exception {
        RSAKey rsa = (RSAKey) new MockJwkMgr().getJwkSet("jwks.json").getKeys().get(0);
        byte[] secret = rsa.toRSAPublicKey().getEncoded();
        JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                .issuer(issuer)
                .audience("12345")
                .claim("preferred_username", "harbor")
                .expirationTime(new Date(new Date().getTime() + 3600_000))
                .build();
        SignedJWT signedJWT = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.HS256).keyID(rsa.getKeyID()).build(), claimsSet);
        signedJWT.sign(new MACSigner(secret));
        return signedJWT.serialize();
    }

    /**
     * Sign an RS256 token with the mock key, with arbitrary extra claims on top of the standard
     * ones. Nested claims are written as nested maps, which is exactly what Nimbus parses a nested
     * JSON object into, so a fixture here has the same shape as a real identity provider's token.
     */
    public String generateTokenWithClaims(Map<String, Object> extraClaims, String audience, long validityMs)
            throws Exception {
        MockJwkMgr mockJwkMgr = new MockJwkMgr();
        RSASSASigner signer = new RSASSASigner(MockJwkMgr.loadPrivateKey("jwks-private-key.pem"));
        JWKSet jwkSet = mockJwkMgr.getJwkSet("signer-jwks.json");

        JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder()
                .issuer("http://localhost:38080/realms/master")
                .subject("8f7f0fa5-e1eb-45d0-8e82-8c89c1a45663")
                .audience(audience)
                .issueTime(new Date())
                .expirationTime(new Date(new Date().getTime() + validityMs))
                .claim("preferred_username", "harbor");
        if (extraClaims != null) {
            extraClaims.forEach(builder::claim);
        }

        SignedJWT signedJWT = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(jwkSet.getKeys().get(0).getKeyID()).build(),
                builder.build());
        signedJWT.sign(signer);
        return signedJWT.serialize();
    }

    public static class MockJwkMgr extends JwkMgr {
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