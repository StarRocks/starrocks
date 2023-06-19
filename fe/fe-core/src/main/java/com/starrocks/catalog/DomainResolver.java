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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/DomainResolver.java

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

package com.starrocks.catalog;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.mysql.privilege.Auth;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * DomainResolver resolve the domain name saved in user property to list of IPs,
 * and refresh password entries in user priv table, periodically.
 */
public class DomainResolver extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(DomainResolver.class);
    // this is only available in BAIDU, for resolving BNS
    private static final String BNS_RESOLVER_TOOLS_PATH = "/usr/bin/get_instance_by_service";

    private Auth auth;
    private AuthenticationMgr authenticationManager;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public DomainResolver(Auth auth) {
        super("domain resolver", 10L * 1000);
        this.auth = auth;
        this.authenticationManager = null;
    }

    public DomainResolver(AuthenticationMgr authenticationManager) {
        super("domain resolver", 10L * 1000);
        this.auth = null;
        this.authenticationManager = authenticationManager;
    }

    /**
     * if a follower has just transfered to leader, or if it is replaying a AuthUpgrade journal.
     * this function will be called to switch from using Auth to using AuthenticationManager.
     */
    public void setAuthenticationManager(AuthenticationMgr manager) {
        lock.writeLock().lock();
        try {
            this.auth = null;
            this.authenticationManager = manager;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 'public' for test
    @Override
    public void runAfterCatalogReady() {
        lock.readLock().lock();
        try {
            // domain names
            Set<String> allDomains;
            if (auth != null) {
                allDomains = Sets.newHashSet();
                auth.getAllDomains(allDomains);
            } else {
                allDomains = authenticationManager.getAllHostnames();
            }

            // resolve domain name
            Map<String, Set<String>> resolvedIPsMap = Maps.newHashMap();
            for (String domain : allDomains) {
                LOG.debug("begin to resolve domain: {}", domain);
                Set<String> resolvedIPs = Sets.newHashSet();
                if (!resolveWithBNS(domain, resolvedIPs) && !resolveWithDNS(domain, resolvedIPs)) {
                    continue;
                }
                LOG.debug("get resolved ip of domain {}: {}", domain, resolvedIPs);

                resolvedIPsMap.put(domain, resolvedIPs);
            }

            // refresh user priv table by resolved IPs
            if (auth != null) {
                auth.refreshUserPrivEntriesByResolvedIPs(resolvedIPsMap);
            } else {
                authenticationManager.setHostnameToIpSet(resolvedIPsMap);
            }
        } finally {
            lock.readLock().unlock();
        }

    }

    /**
     * Check if domain name is valid
     *
     * @param domainName: currently is the user's whitelist bns or dns name
     * @return true of false
     */
    public boolean isValidDomain(String domainName) {
        if (Strings.isNullOrEmpty(domainName)) {
            LOG.warn("Domain name is null or empty");
            return false;
        }
        Set<String> ipSet = Sets.newHashSet();
        if (!resolveWithDNS(domainName, ipSet) && !resolveWithBNS(domainName, ipSet)) {
            return false;
        }
        return true;
    }

    /**
     * resolve domain name with dns
     */
    public boolean resolveWithDNS(String domainName, Set<String> resolvedIPs) {
        InetAddress[] address;
        try {
            address = InetAddress.getAllByName(domainName);
        } catch (UnknownHostException e) {
            LOG.warn("unknown domain name " + domainName + " with dns: " + e.getMessage());
            return false;
        }

        for (InetAddress addr : address) {
            resolvedIPs.add(addr.getHostAddress());
        }
        return true;
    }

    public boolean resolveWithBNS(String domainName, Set<String> resolvedIPs) {
        File binaryFile = new File(BNS_RESOLVER_TOOLS_PATH);
        if (!binaryFile.exists()) {
            LOG.info("{} does not exist", BNS_RESOLVER_TOOLS_PATH);
            return false;
        }

        final StringBuilder cmdBuilder = new StringBuilder();
        cmdBuilder.append(BNS_RESOLVER_TOOLS_PATH).append(" -a ").append(domainName);
        Process process = null;
        BufferedReader bufferedReader = null;
        String str = null;
        String ip = null;
        try {
            process = Runtime.getRuntime().exec(cmdBuilder.toString());
            bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((str = bufferedReader.readLine()) != null) {
                ip = str.split(" ")[1];
                resolvedIPs.add(ip);
            }
            final int exitCode = process.waitFor();
            // mean something error
            if (exitCode != 0) {
                LOG.warn("failed to execute cmd: {}, exit code: {}", cmdBuilder.toString(), exitCode);
                resolvedIPs.clear();
                return false;
            }
            return true;
        } catch (IOException | InterruptedException e) {
            LOG.warn("failed to revole domain with BNS", e);
            resolvedIPs.clear();
            return false;
        } finally {
            if (process != null) {
                process.destroy();
            }
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                LOG.error("Close bufferedReader error! " + e);
            }
        }
    }

}
