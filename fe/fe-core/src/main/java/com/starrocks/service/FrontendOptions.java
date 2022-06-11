// This file is made available under Elastic License 2.0.
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
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.NetUtils;
import com.starrocks.persist.Storage;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Inet4Address;
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
    static final List<String> priorityCidrs = Lists.newArrayList();
    private static InetAddress localAddr = InetAddress.getLoopbackAddress();
    private static boolean useFqdn = false;

    public static void init(String[] args) throws UnknownHostException {
        localAddr = null;
        if (!"0.0.0.0".equals(Config.frontend_address)) {
            if (!InetAddressValidator.getInstance().isValidInet4Address(Config.frontend_address)) {
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

        if (!Config.enable_fqdn_func) {
            initAddrUseIp(hosts);
            return;
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
        String roleFilePath = Config.meta_dir + ROLE_FILE_PATH;
        File roleFile = new File(roleFilePath);
        if (!roleFile.exists()) {
            initAddrUseFqdn(hosts);
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
        if (null == fileStoredHostType || fileStoredHostType.equals(HostType.IP.toString())) {
            initAddrUseIp(hosts);
            return;
        }
        initAddrUseFqdn(hosts);
    }

    @VisibleForTesting
    static void initAddrUseFqdn(List<InetAddress> hosts) throws UnknownHostException {
        useFqdn = true;
        InetAddress uncheckedLocalAddr = InetAddress.getLocalHost();
        if (null == uncheckedLocalAddr) {
            LOG.error("get a null localhost when start fe use fqdn");
            System.exit(-1);
        }
        String uncheckedFqdn = uncheckedLocalAddr.getCanonicalHostName();
        if (null == uncheckedFqdn) {
            LOG.error("get a null canonicalHostName when start fe use fqdn");
            System.exit(-1);
        }
        String uncheckeddIp = InetAddress.getByName(uncheckedFqdn).getHostAddress();
        boolean hasInetAddr = false;
        for (InetAddress addr : hosts) {
            if (uncheckeddIp.equals(addr.getHostAddress())) {
                hasInetAddr = true;
            }
        }
        if (hasInetAddr) {
            localAddr = uncheckedLocalAddr;
        } else {
            LOG.error("fail to find right localhost when start fe use fqdn");
            System.exit(-1);
        }
    }

    @VisibleForTesting
    static void initAddrUseIp(List<InetAddress> hosts) {
        useFqdn = false;
        analyzePriorityCidrs();
        // if not set frontend_address, get a non-loopback ip

        InetAddress loopBack = null;
        boolean hasMatchedIp = false;
        for (InetAddress addr : hosts) {
            LOG.debug("check ip address: {}", addr);
            if (addr instanceof Inet4Address) {
                if (addr.isLoopbackAddress()) {
                    loopBack = addr;
                } else if (!priorityCidrs.isEmpty()) {
                    if (isInPriorNetwork(addr.getHostAddress())) {
                        localAddr = addr;
                        hasMatchedIp = true;
                        break;
                    }
                } else {
                    localAddr = addr;
                    break;
                }
            }
        }
        //if all ips not match the priority_networks then print the warning log
        if (!priorityCidrs.isEmpty() && !hasMatchedIp) {
            LOG.warn("ip address range configured for priority_networks does not include the current IP address");
        }
        // nothing found, use loopback addr
        if (localAddr == null) {
            localAddr = loopBack;
        }
        LOG.info("local address: {}.", localAddr);
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

    public static InetAddress getLocalHost() {
        return localAddr;
    }

    public static boolean isUseFqdn() {
        return useFqdn;
    }

    public static String getLocalHostAddress() {
        if (useFqdn) {
            return localAddr.getCanonicalHostName();
        }
        return localAddr.getHostAddress();
    }

    public static String getHostname() {
        return localAddr.getHostName();
    }

    public static String getHostnameByIp(String ip) {
        String hostName = FeConstants.null_string;
        try {
            InetAddress address = InetAddress.getByName(ip);
            hostName = address.getHostName();
        } catch (UnknownHostException e) {
            LOG.info("unknown host for {}", ip, e);
            hostName = "unknown";
        }
        return hostName;
    }

    private static void analyzePriorityCidrs() {
        String priorCidrs = Config.priority_networks;
        if (Strings.isNullOrEmpty(priorCidrs)) {
            return;
        }
        LOG.info("configured prior_cidrs value: {}", priorCidrs);

        String[] cidrList = priorCidrs.split(PRIORITY_CIDR_SEPARATOR);
        List<String> priorNetworks = Lists.newArrayList(cidrList);
        priorityCidrs.addAll(priorNetworks);
    }

    @VisibleForTesting
    static boolean isInPriorNetwork(String ip) {
        ip = ip.trim();
        for (String cidr : priorityCidrs) {
            cidr = cidr.trim();
            if (!cidr.contains("/")) {
                // it is not valid CIDR, compare ip directly.
                if (cidr.equals(ip)) {
                    return true;
                }
            } else {
                SubnetUtils subnetUtils = new SubnetUtils(cidr);
                subnetUtils.setInclusiveHostCount(true);
                SubnetUtils.SubnetInfo subnetInfo = subnetUtils.getInfo();
                if (subnetInfo.isInRange(ip)) {
                    return true;
                }
            }
        }
        return false;
    }
}

