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

package com.starrocks.epack.privilege;

import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.privilege.PrivilegeException;

import java.util.Map;
import java.util.Objects;

public class RoleMappingFactory {
    public static RoleMapping createRoleMapping(String name, Map<String, String> propertyMap,
                                                String integrationType)
            throws PrivilegeException {
        if (Objects.equals(integrationType, SecurityIntegration.SECURITY_INTEGRATION_TYPE_LDAP)) {
            return new LDAPRoleMapping(name, propertyMap, integrationType);
        } else {
            throw new PrivilegeException("unsupported '" + integrationType + "' type when creating role mapping '" + name + "'");
        }

    }
}
