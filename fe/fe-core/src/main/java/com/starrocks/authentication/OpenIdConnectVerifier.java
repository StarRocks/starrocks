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

<<<<<<< HEAD
    public static void verify(String idToken,
                              String userName,
                              JWKSet jwkSet,
                              String principalFiled,
                              String[] requiredIssuer,
                              String[] requiredAudience) throws AuthenticationException {
=======
    /**
     * Signature algorithms we accept. Anything outside this set (notably {@code none}, and HMAC
     * algorithms that would let an attacker sign with the public key) is rejected up front to
     * prevent algorithm-confusion attacks (threat T7).
     */
    private static final Set<JWSAlgorithm> ALLOWED_ALGORITHMS = Set.of(
            JWSAlgorithm.RS256, JWSAlgorithm.RS384, JWSAlgorithm.RS512,
            JWSAlgorithm.ES256, JWSAlgorithm.ES384, JWSAlgorithm.ES512);

    /**
     * Verify an OIDC id-token against a cached {@link JWKSource} (the path used by HTTP / Arrow
     * Flight Bearer and the MySQL JWT provider, with key rotation and outage tolerance handled by
     * the source).
     */
    public static JWTClaimsSet verify(String idToken,
                                      String userName,
                                      JWKSource<SecurityContext> jwkSource,
                                      String principalFiled,
                                      String[] requiredIssuer,
                                      String[] requiredAudience) throws AuthenticationException {
        try {
            SignedJWT signedJWT = verifySignature(idToken, jwkSource);
            checkClaims(signedJWT, userName, principalFiled, requiredIssuer, requiredAudience);
            // Handed back so a caller that also reads a claim (group resolution) works on the set
            // this method already parsed and verified, instead of parsing the token a second time.
            return signedJWT.getJWTClaimsSet();
        } catch (AuthenticationException e) {
            throw e;
        } catch (ParseException e) {
            throw new AuthenticationException(e.getMessage()).withReason(AuthFailureReason.MALFORMED);
        } catch (KeySourceException e) {
            // The key set could not be obtained at all (IdP unreachable past the outage window,
            // malformed JWKS document): nothing the token holder did. Transient, so the handler
            // neither remembers the token as rejected nor refuses it again once the IdP is back.
            throw new AuthenticationException(e.getMessage()).withReason(AuthFailureReason.INTERNAL).asTransient();
        } catch (JOSEException e) {
            throw new AuthenticationException(e.getMessage()).withReason(AuthFailureReason.SIGNATURE_FAIL);
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage()).withReason(AuthFailureReason.INTERNAL);
        }
    }

    /**
     * Overload kept for callers that already hold a {@link JWKSet} (the OAuth2 MySQL browser-flow
     * callback in {@code OAuth2Action}). It wraps the set as an immutable source and shares the
     * same verification core, so those callers also get RSA/EC support and the algorithm
     * allow-list.
     */
    public static JWTClaimsSet verify(String idToken,
                                      String userName,
                                      JWKSet jwkSet,
                                      String principalFiled,
                                      String[] requiredIssuer,
                                      String[] requiredAudience) throws AuthenticationException {
        return verify(idToken, userName, new ImmutableJWKSet<>(jwkSet), principalFiled, requiredIssuer,
                requiredAudience);
    }

    /**
     * Parse the principal claim out of a token <b>without</b> verifying its signature. Used only by
     * the Bearer path to learn the declared user name before the issuer-based routing has picked a
     * security integration; the real signature check still runs afterwards in {@link #verify}, so a
     * forged token cannot benefit.
     */
    public static String extractPrincipal(String idToken, String principalFiled) throws AuthenticationException {
>>>>>>> dc172843477... [Feature] [PRE-574] Read a security integration user's groups from a claim of the verified token (#63504)
        try {
            SignedJWT signedJWT = verifyJWT(idToken, jwkSet);
            JWTClaimsSet claims = signedJWT.getJWTClaimsSet();
            String jwtUserName = claims.getStringClaim(principalFiled);

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
