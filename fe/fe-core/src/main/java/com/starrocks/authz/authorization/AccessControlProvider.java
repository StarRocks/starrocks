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

package com.starrocks.authz.authorization;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.sql.analyzer.AuthorizerStmtVisitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AccessControlProvider {
    protected final AuthorizerStmtVisitor privilegeCheckerVisitor;
    public final Map<String, AccessController> catalogToAccessControl;

    public AccessControlProvider(AuthorizerStmtVisitor privilegeCheckerVisitor, AccessController accessControl) {
        this.privilegeCheckerVisitor = privilegeCheckerVisitor;

        this.catalogToAccessControl = new ConcurrentHashMap<>();
        this.catalogToAccessControl.put(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, accessControl);
    }

    public AuthorizerStmtVisitor getPrivilegeCheckerVisitor() {
        return privilegeCheckerVisitor;
    }

    public AccessController getAccessControlOrDefault(String catalogName) {
        if (catalogName == null) {
            return catalogToAccessControl.get(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        }

        AccessController catalogAccessController = catalogToAccessControl.get(catalogName);
        if (catalogAccessController != null) {
            return catalogAccessController;
        } else {
            return catalogToAccessControl.get(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        }
    }

    public void setAccessControl(String catalog, AccessController accessControl) {
        catalogToAccessControl.put(catalog, accessControl);
    }

    public void removeAccessControl(String catalog) {
        catalogToAccessControl.remove(catalog);
    }
}
