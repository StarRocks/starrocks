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

import com.starrocks.common.DdlException;

import java.util.Map;

public class SecurityIntegrationFactory {

    public static SecurityIntegration createSecurityIntegration(
            String name, Map<String, String> propertyMap) throws DdlException {
        String type = propertyMap.get(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY);
        SecurityIntegrationType securityIntegrationType = getSecurityIntegrationType(type);
        SecurityIntegration integration;
        switch (securityIntegrationType) {
            case LDAP:
                integration = new LDAPSecurityIntegration(name, propertyMap);
                break;
            case CUSTOM:
                integration = new CustomSecurityIntegration(name, propertyMap);
                break;
            default:
                throw new DdlException("unsupported '" + type + "' type security integration");
        }
        integration.checkProperties();
        return integration;
    }

    public static SecurityIntegrationType getSecurityIntegrationType(String type) throws DdlException {
        try {
            return SecurityIntegrationType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new DdlException("unsupported security integration type '" + type + "'");
        }
    }

    public enum SecurityIntegrationType {
        LDAP, CUSTOM;
    }

}
