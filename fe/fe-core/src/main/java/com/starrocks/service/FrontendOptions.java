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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/service/FrontendOptions.java

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

package com.starrocks.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.starrocks.common.Config;
import com.starrocks.common.util.NetUtils;
import com.starrocks.persist.Storage;
import inet.ipaddr.IPAddressString;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;

public class FrontendOptions {

    public enum HostType {
        FQDN,
        IP,
        NOT_SPECIFIED,
    }

    private static final Logger LOG = LogManager.getLogger(FrontendOptions.class);

    private static final String PRIORITY_CIDR_SEPARATOR = ";";

    private static final String HOST_TYPE = "hostType";
    private static final String ROLE_FILE_PATH = "/image/ROLE";

    @VisibleForTesting
    static final List<String> PRIORITY_CIDRS = Lists.newArrayList();
    private static InetAddress localAddr = InetAddress.getLoopbackAddress();
    private static boolean useFqdn = false;

    public static void init(String[] args) throws UnknownHostException {
        localAddr = null;
        if (!"0.0.0.0".equals(Config.frontend_address)) {
            if (!InetAddressValidator.getInstance().isValid(Config.frontend_address)) {
                throw new UnknownHostException("invalid frontend_address: " + Config.frontend_address);
            }
            localAddr = InetAddress.getByName(Config.frontend_address);
            LOG.info("use configured address. {}", localAddr);
            return;
        }

        List<InetAddress> hosts = NetUtils.getHosts();
        if (hosts.isEmpty()) {
            LOG.error("fail to get localhost");
            System.exit(-1);
        }

        HostType specifiedHostType = HostType.NOT_SPECIFIED;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-host_type")) {
                if (i + 1 >= args.length) {
                    System.out.println("-host_type need parameter FQDN or IP");
                    System.exit(-1);
                }
                String inputHostType = args[i + 1];
                try {
                    inputHostType = inputHostType.toUpperCase();
                    specifiedHostType = HostType.valueOf(inputHostType);
                } catch (Exception e) {
                    System.out.println("-host_type need parameter FQDN or IP");
                    System.exit(-1);
                }
            }   
        } 

        if (specifiedHostType == HostType.FQDN) {
            initAddrUseFqdn(hosts);
            return;
        }
        if (specifiedHostType == HostType.IP) {
            initAddrUseIp(hosts);
            return;
        }

        // Check if it is a new cluster, new clusters start with IP by default
        String roleFilePath = Config.meta_dir + ROLE_FILE_PATH;
        File roleFile = new File(roleFilePath);
        if (!roleFile.exists()) {
            initAddrUseIp(hosts);
            return;
        }
        
        Properties prop = new Properties();
        String fileStoredHostType;
        try (FileInputStream in = new FileInputStream(roleFile)) {
            prop.load(in);
        } catch (IOException e) {
            LOG.error("failed to read role file");
            System.exit(-1);
        }
        fileStoredHostType = prop.getProperty(HOST_TYPE, null);
        
        // Check if the ROLE file has property 'hostType'
        // If it not has property 'hostType', start with IP
        // If it has property 'hostType' & hostType = IP, start with IP
        if (Strings.isNullOrEmpty(fileStoredHostType) || fileStoredHostType.equals(HostType.IP.toString())) {
            initAddrUseIp(hosts);
            return;
        }
        // If it has property 'hostType' & hostType = FQDN, start with FQDN
        initAddrUseFqdn(hosts);
    }

    @VisibleForTesting
    static void initAddrUseFqdn(List<InetAddress> addrs) {
        useFqdn = true;
        analyzePriorityCidrs();
        String fqdn = null;

        if (PRIORITY_CIDRS.isEmpty()) {
            // Get FQDN from local host by default.
            try {
                InetAddress localHost = InetAddress.getLocalHost();
                fqdn = localHost.getCanonicalHostName();
                String ip = localHost.getHostAddress();
                LOG.info("Get FQDN from local host by default, FQDN: {}, ip: {}, v6: {}", fqdn, ip,
                        localHost instanceof Inet6Address);
            } catch (UnknownHostException e) {
                LOG.error("failed to get FQDN from local host, will exit", e);
                System.exit(-1);
            }
            if (fqdn == null) {
                LOG.error("priority_networks is not set and we cannot get FQDN from local host");
                System.exit(-1);
            }
            // Try to resolve addr from FQDN
            InetAddress uncheckedInetAddress = null;
            try {
                uncheckedInetAddress = InetAddress.getByName(fqdn);
            } catch (UnknownHostException e) {
                LOG.error("failed to parse FQDN: {}, message: {}", fqdn, e.getMessage(), e);
                System.exit(-1);
            }
            if (null == uncheckedInetAddress) {
                LOG.error("failed to parse FQDN: {}", fqdn);
                System.exit(-1);
            }
            // Check whether the InetAddress obtained via FQDN is bound to some network interface
            boolean hasInetAddr = false;
            for (InetAddress addr : addrs) {
                LOG.info("Try to match addr in fqdn mode, ip: {}, FQDN: {}",
                        addr.getHostAddress(), addr.getCanonicalHostName());
                if (addr.getCanonicalHostName()
                        .equals(uncheckedInetAddress.getCanonicalHostName())) {
                    hasInetAddr = true;
                    break;
                }
            }
            if (hasInetAddr) {
                localAddr = uncheckedInetAddress;
                LOG.info("Using FQDN from local host by default, FQDN: {}, ip: {}, v6: {}",
                        localAddr.getCanonicalHostName(),
                        localAddr.getHostAddress(),
                        localAddr instanceof Inet6Address);
            } else {
                LOG.error("Cannot find a network interface matching FQDN: {}", fqdn);
                System.exit(-1);
            }
        } else {
            LOG.info("using priority_networks in fqdn mode to decide whether ipv6 or ipv4 is preferred");
            for (InetAddress addr : addrs) {
                String hostAddr = addr.getHostAddress();
                String canonicalHostName = addr.getCanonicalHostName();
                LOG.info("Try to match addr in fqdn mode, ip: {}, FQDN: {}", hostAddr, canonicalHostName);
                if (isInPriorNetwork(hostAddr)) {
                    localAddr = addr;
                    fqdn = canonicalHostName;
                    LOG.info("Using FQDN from matched addr, FQDN: {}, ip: {}, v6: {}",
                            fqdn, hostAddr, addr instanceof Inet6Address);
                    break;
                }
                LOG.info("skip addr {} not belonged to priority networks in FQDN mode", addr);
            }
            if (fqdn == null) {
                LOG.error("priority_networks has been set and we cannot find matched addr, will exit");
                System.exit(-1);
            }
        }

        // double-check the reverse resolve
        String canonicalHostName = localAddr.getCanonicalHostName();
        if (!canonicalHostName.equals(fqdn)) {
            LOG.error("The FQDN of the parsed address [{}] is not the same as " + 
                    "the FQDN obtained from the host [{}]", canonicalHostName, fqdn);
            System.exit(-1);
        }
    }

    @VisibleForTesting
    static void initAddrUseIp(List<InetAddress> hosts) {
        useFqdn = false;
        analyzePriorityCidrs();

        InetAddress loopBack = null;
        boolean hasMatchedIp = false;

        // If `priority_networks` is configured, find a possible ip matching the configuration first.
        // Otherwise, find other usable ip as if `priority_networks` is not configured.
        if (!PRIORITY_CIDRS.isEmpty()) {
            for (InetAddress addr : hosts) {
                LOG.info("check ip address: {}", addr);
                // Whether to use IPv4 or IPv6, it's configured by CIDR format.
                // If both IPv4 and IPv6 are configured, the config order decides priority.
                if (isInPriorNetwork(addr.getHostAddress())) {
                    localAddr = addr;
                    hasMatchedIp = true;
                    break;
                }
                LOG.info("skip ip not belonged to priority networks: {}", addr);
            }
            // If all ips not match the priority_networks then print the warning log
            if (!hasMatchedIp) {
                LOG.warn("ip address range configured for priority_networks does not include the current IP address, " +
                        "will try other usable ip");
            }
        }

        if (localAddr == null) {
            for (InetAddress addr : hosts) {
                LOG.info("check ip address: {}", addr);
                if (addr.isLoopbackAddress()) {
                    loopBack = addr;
                } else {
                    if (Config.net_use_ipv6_when_priority_networks_empty) {
                        if (addr instanceof Inet6Address) {
                            localAddr = addr;
                        }
                    } else if (addr instanceof Inet4Address) {
                        localAddr = addr;
                    }
                    if (localAddr != null) {
                        // Use the first one found.
                        break;
                    }
                }
            }
        }

        // nothing found, use loopback addr
        if (localAddr == null) {
            localAddr = loopBack;
        }
        LOG.info("Use IP init local addr, IP: {}", localAddr);
    }

    public static void saveStartType() {
        try {
            Storage storage = new Storage(Config.meta_dir + "/image");
            String hostType = useFqdn ? HostType.FQDN.toString() : HostType.IP.toString();
            storage.writeFeStartFeHostType(hostType);
        } catch (IOException e) {
            LOG.error("fail to write fe start host type:" + e.getMessage());
            System.exit(-1);
        }
    }

    public static boolean isUseFqdn() {
        return useFqdn;
    }

    public static String getLocalHostAddress() {
        if (useFqdn) {
            return localAddr.getCanonicalHostName();
        }
        return InetAddresses.toAddrString(localAddr);
    }

    public static String getHostname() {
        return localAddr.getHostName();
    }

    private static void analyzePriorityCidrs() {
        String priorCidrs = Config.priority_networks;
        if (Strings.isNullOrEmpty(priorCidrs)) {
            PRIORITY_CIDRS.clear();
            return;
        }
        LOG.info("configured prior_cidrs value: {}", priorCidrs);

        String[] cidrList = priorCidrs.split(PRIORITY_CIDR_SEPARATOR);
        List<String> priorNetworks = Lists.newArrayList(cidrList);
        PRIORITY_CIDRS.addAll(priorNetworks);
    }

    @VisibleForTesting
    static boolean isInPriorNetwork(String ip) {
        ip = ip.trim();
        for (String cidr : PRIORITY_CIDRS) {
            cidr = cidr.trim();
            IPAddressString network = new IPAddressString(cidr);
            IPAddressString address = new IPAddressString(ip);
            if (network.contains(address)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isBindIPV6() {
        return localAddr instanceof Inet6Address;
    }
}

