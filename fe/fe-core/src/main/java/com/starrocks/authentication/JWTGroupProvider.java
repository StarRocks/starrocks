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

import com.nimbusds.jwt.SignedJWT;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JWTGroupProvider extends GroupProvider {
    private static final Logger LOG = LogManager.getLogger(JWTGroupProvider.class);

    public static final String TYPE = "jwt";

    /**
     * Path of the claim to read roles from
     */
    public static final String JWT_CLAIM = "jwt_claim";

    public static final Set<String> REQUIRED_PROPERTIES = new HashSet<>(Arrays.asList(JWT_CLAIM));

    public JWTGroupProvider(String name, Map<String, String> properties) {
        super(name, properties);
    }

    @Override
    public void init() throws DdlException {
        // Nothing to do.
    }

    @Override
    public void destroy() {
        // Nothing to do.
    }

    @Override
    public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
        final String authToken = ConnectContext.get().getAuthToken();
        
        final Set<String> groups = new HashSet<>();
        
        // if not logged through JWT
        if (authToken == null) {
            return groups;
        }
        
        SignedJWT signedJWT;
        try {
            signedJWT = SignedJWT.parse(authToken);
        } catch (ParseException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Could not parse token", e);
            }
            return groups;
        }
        
        Object value = signedJWT.getPayload().toJSONObject();
        for (String path : getJwtClaim().split("\\.")) {
            if (value instanceof Map map) {
                value = map.get(path);
            } else {
                // claim is not reachable
                return groups;
            }
        }
        
        if (value instanceof List l) {
            groups.addAll(l);
        } else if (value instanceof String s) {
            groups.add(s);
        }
        
        return groups;
    }

    @Override
    public void checkProperty() throws SemanticException {
        REQUIRED_PROPERTIES.forEach(s -> {
            if (!properties.containsKey(s)) {
                throw new SemanticException("missing required property: " + s);
            }
        });
    }

    public String getJwtClaim() {
        return properties.get(JWT_CLAIM);
    }
}
