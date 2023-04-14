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

import java.util.Map;

public class SecurityIntegrationFactory {
    public static SecurityIntegration createSecurityIntegration(String name, Map<String, String> propertyMap)
            throws AuthenticationException {
        String type = propertyMap.get(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY);
        if (type.equals(SecurityIntegration.SECURITY_INTEGRATION_TYPE_LDAP)) {
            return new LDAPSecurityIntegration(name, propertyMap);
        } else {
            throw new AuthenticationException("unsupported '" + type + "' type security integration");
        }

    }
}
