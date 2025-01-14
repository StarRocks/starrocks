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
package com.starrocks.privilege.cauthz;

import com.starrocks.common.Config;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ExternalAccessController;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.cauthz.CauthzAccessResourceImpl;
import com.starrocks.privilege.cauthz.CauthzAuthorizer;
import com.starrocks.privilege.cauthz.CauthzStarRocksAccessRequest;
import com.starrocks.sql.ast.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CauthzAccessController is an implementation of AccessController that leverages
 * an implementation of CauthzAuthorizer interface instantiated at runtime to
 * perform authorization checks. 
 */
public abstract class CauthzAccessController extends ExternalAccessController implements AccessTypeConverter {
    private static final Logger LOG = LoggerFactory.getLogger(CauthzAccessController.class);
    protected CauthzAuthorizer authorizer;

    public CauthzAccessController() {
        super(false);
        authorizer = initCauthzAuthorizer();
    }

    private CauthzAuthorizer initCauthzAuthorizer() throws RuntimeException {
        String cauthzAuthorizationClassName = Config.cauthz_authorization_class_name;
        try {
            CauthzAuthorizer authorizer = Class.forName(cauthzAuthorizationClassName)
                            .asSubclass(CauthzAuthorizer.class).newInstance();
            authorizer.init();
            return authorizer;
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate CauthzAuthorizer implementation: " 
                    + cauthzAuthorizationClassName, e);
        }
    }

    protected void hasPermission(CauthzAccessResourceImpl resource, UserIdentity user, 
            PrivilegeType privilegeType) throws AccessDeniedException {
        String accessType = convertToAccessType(privilegeType);
        CauthzStarRocksAccessRequest request = new CauthzStarRocksAccessRequest(resource, user, accessType);

        if (!authorizer.authorize(request)) {
            throw new AccessDeniedException("User " + user + " does not have " + accessType + " privilege on " + resource);
        }
    }
}