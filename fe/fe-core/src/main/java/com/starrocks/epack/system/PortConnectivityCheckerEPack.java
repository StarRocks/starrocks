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

package com.starrocks.epack.system;

import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.common.Pair;
import com.starrocks.epack.authentication.AuthenticationMgrEPack;
import com.starrocks.epack.authentication.LDAPSecurityIntegration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.PortConnectivityChecker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

public class PortConnectivityCheckerEPack extends PortConnectivityChecker {
    private static final Logger LOG = LogManager.getLogger(PortConnectivityCheckerEPack.class);

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();

        Set<SecurityIntegration> integrations = ((AuthenticationMgrEPack) GlobalStateMgr
                .getCurrentState().getAuthenticationMgr()).getAllSecurityIntegrations();

        List<Future<Pair<LDAPSecurityIntegration, Boolean>>> futureList = new ArrayList<>(integrations.size());
        for (SecurityIntegration integration : integrations) {
            if (integration instanceof LDAPSecurityIntegration ldapSecurityIntegration) {
                Pair<String, Integer> hostPort = ldapSecurityIntegration.getHostAndPort();
                if (hostPort == null) {
                    LOG.warn("failed to get host and port for security integration: {}", integration.getName());
                    continue;
                }
                futureList.add(executor.submit(() ->
                        Pair.create(ldapSecurityIntegration, isPortConnectable(hostPort.first, hostPort.second))));
            }
        }

        for (Future<Pair<LDAPSecurityIntegration, Boolean>> future : futureList) {
            try {
                Pair<LDAPSecurityIntegration, Boolean> result = future.get();
                LDAPSecurityIntegration ldapSecurityIntegration = result.first;
                ldapSecurityIntegration.setNetWorkReachable(result.second);
            } catch (Exception e) {
                LOG.warn("checking for connectivity of security integration failed", e);
            }
        }
    }
}
