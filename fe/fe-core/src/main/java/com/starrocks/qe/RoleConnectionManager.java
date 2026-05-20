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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectScheduler.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.collect.Maps;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authorization.RolePrivilegeCollectionV2;
import com.starrocks.common.CloseableLock;
import com.starrocks.common.ErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RoleConnectionManager {
    private static final Logger LOG = LogManager.getLogger(RoleConnectionManager.class);
    private final Map<String, Integer> connCountByRole = Maps.newConcurrentMap();
    private final ReentrantLock connLock = new ReentrantLock();
    private final List<RoleConnectionLimit> roleConnectionLimits;
    private final Integer maxConnections;

    public RoleConnectionManager(int maxConnections, String config) {
        this.maxConnections = maxConnections;
        this.roleConnectionLimits = parse(config);
    }

    public void unregisterConnection(ConnectContext context) {
        try (CloseableLock ignored = CloseableLock.lock(this.connLock)) {
            getUserRoles(context).forEach(role -> connCountByRole.computeIfPresent(role, (k, v) -> v > 1 ? v - 1 : null));
        }
    }

    public void registerConnection(ConnectContext ctx) throws AuthenticationException {
        Set<String> userRoles = getUserRoles(ctx);

        if (userRoles.isEmpty() || roleConnectionLimits.isEmpty()) {
            return;
        }

        Map<String, Integer> maxLimitForRoles = getMaxLimitForRoles(userRoles);

        try (CloseableLock ignored = CloseableLock.lock(this.connLock)) {
            for (String role : userRoles) {
                Integer currentConnection = getCurrentConnection(role);
                Integer limit = maxLimitForRoles.get(role);

                if (currentConnection >= limit) {
                    String userErrMsg = String.format("Role-based connection limit reached "
                                + "[user=%s, role=%s, maxAllowed=%s, activeConnections=%s, node=%s, connectionId=%s]",
                            ctx.getQualifiedUser(),
                            role,
                            limit,
                            currentConnection,
                            ctx.getGlobalStateMgr().getNodeMgr().getSelfNode(),
                            ctx.getConnectionId());
                    LOG.info("{}, details: userRoles={}", userErrMsg, userRoles);
                    throw new AuthenticationException(ErrorCode.ERR_GROUP_MAX_CONNECTION, role, limit, currentConnection);
                }
            }
        } finally {
            // Register the connection, since it has already been created before we apply any role based restrictions.
            this.registerConnection(userRoles);
        }
    }

    private Map<String, Integer> getMaxLimitForRoles(Set<String> userRoles) {
        Map<String, Integer> result = new HashMap<>(userRoles.size());

        for (String role : userRoles) {
            result.put(role, maxConnections);

            for (RoleConnectionLimit rcl : roleConnectionLimits) {
                if (rcl.matches(role)) {
                    result.put(role, rcl.maxConnections);
                    break;
                }
            }
        }

        return result;
    }

    private Integer getCurrentConnection(String role) {
        return connCountByRole.getOrDefault(role, 0);
    }

    private void registerConnection(Set<String> roles) {
        roles.forEach(role -> connCountByRole.merge(role, 1, Integer::sum));
    }

    private Set<String> getUserRoles(ConnectContext context) {
        if (context.getCurrentUserIdentity() != null) {
            return context.getCurrentRoleIds()
                    .stream()
                    .map(context.getGlobalStateMgr().getAuthorizationMgr()::getRolePrivilegeCollection)
                    .filter(Objects::nonNull)
                    .map(RolePrivilegeCollectionV2::getName)
                    .collect(Collectors.toSet());
        }

        return Set.of();
    }

    private record RoleConnectionLimit(Pattern rolePattern, int maxConnections, int specificity) {

        public boolean matches(String role) {
            return rolePattern.matcher(role).matches();
        }
    }

    private static List<RoleConnectionLimit> parse(String config) {
        if (StringUtils.isBlank(config)) {
            return List.of();
        }

        return Stream.of(config.split(","))
            .map(String::trim)
            .map(RoleConnectionManager::parseEntry)
            .sorted(Comparator.comparingInt(RoleConnectionLimit::specificity).reversed())
            .toList();
    }

    private static RoleConnectionLimit parseEntry(String entry) {
        int sep = entry.indexOf(':');

        if (sep <= 0 || sep == entry.length() - 1) {
            throw new IllegalArgumentException("Invalid role connection limit entry: " + entry);
        }

        String regex = wildcardToRegex(entry.substring(0, sep).trim());
        int specificity = calculateSpecificity(regex);
        int maxConnections = Integer.parseInt(entry.substring(sep + 1).trim());

        return new RoleConnectionLimit(Pattern.compile(regex), maxConnections, specificity);
    }

    /**
     * Converts wildcard patterns like ROLE_AD_* → ROLE_AD_.*
     */
    private static String wildcardToRegex(String value) {
        return value.replace(".", "\\.").replace("*", ".*");
    }

    /**
     * Higher number = more specific
     * Removes wildcards and regex noise
     */
    private static int calculateSpecificity(String pattern) {
        return pattern.replace(".", "").length();
    }
}
