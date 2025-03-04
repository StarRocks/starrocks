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
import com.google.common.collect.ImmutableSortedSet;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Map;

public class SecurityIntegrationFactory {
    private static final ImmutableSortedSet<String> SUPPORTED_AUTH_MECHANISM =
            ImmutableSortedSet.orderedBy(String.CASE_INSENSITIVE_ORDER)
                    .add(OIDCSecurityIntegration.TYPE)
                    .build();

    public static void checkSecurityIntegrationIsSupported(String securityIntegrationType) {
        if (!SUPPORTED_AUTH_MECHANISM.contains(securityIntegrationType)) {
            throw new SemanticException("unsupported security integration type '" + securityIntegrationType + "'");
        }
    }

    public static SecurityIntegration createSecurityIntegration(String name, Map<String, String> propertyMap) {
        String type = propertyMap.get(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY);
        checkSecurityIntegrationIsSupported(type);

        SecurityIntegration securityIntegration = null;
        if (type.equalsIgnoreCase(OIDCSecurityIntegration.TYPE)) {
            securityIntegration = new OIDCSecurityIntegration(name, propertyMap);
        }
        Preconditions.checkArgument(securityIntegration != null);
        return securityIntegration;
    }
}
