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

import com.starrocks.sql.analyzer.SemanticException;

import java.util.Map;
import java.util.Set;

public class JWTSecurityIntegration extends JWTBaseSecurityIntegration {

    public static final Set<String> REQUIRED_PROPERTIES = Set.of(
            SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, JWKS_URL, PRINCIPAL_FIELD);

    public JWTSecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() {
<<<<<<< HEAD
        String jwksUrl = propertyMap.get(JWTAuthenticationProvider.JWT_JWKS_URL);
        String principalFiled = propertyMap.get(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD);
        String commaSeparatedIssuer = propertyMap.get(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER);
        String[] requireIssuer = commaSeparatedIssuer == null ?
                new String[0] : COMMA_SPLIT.split(commaSeparatedIssuer);
        String commaSeperatedRequireAudiences = propertyMap.get(JWTAuthenticationProvider.JWT_REQUIRED_AUDIENCE);
        String[] requireAudiences = commaSeperatedRequireAudiences == null ?
                new String[0] : COMMA_SPLIT.split(commaSeperatedRequireAudiences.trim());
        return new JWTAuthenticationProvider(jwksUrl, principalFiled, requireIssuer, requireAudiences);
=======
        return new JWTAuthenticationProvider(getJwksUrl(), getPrincipalField(), getRequiredIssuer(),
                getRequiredAudience(), getGroupSource(), getGroupClaim(), name);
>>>>>>> dc172843477... [Feature] [PRE-574] Read a security integration user's groups from a claim of the verified token (#63504)
    }

    @Override
    public void checkProperty() throws SemanticException {
        REQUIRED_PROPERTIES.forEach(s -> {
            if (!propertyMap.containsKey(s)) {
                throw new SemanticException("missing required property: " + s);
            }
        });
        super.checkProperty();
    }
}
