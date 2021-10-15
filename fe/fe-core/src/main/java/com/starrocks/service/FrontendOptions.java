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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.NetUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class FrontendOptions {
    private static final Logger LOG = LogManager.getLogger(FrontendOptions.class);

    private static final String PRIORITY_CIDR_SEPARATOR = ";";

    private static final List<String> priorityCidrs = Lists.newArrayList();
    private static InetAddress localAddr = InetAddress.getLoopbackAddress();

    public static void init() throws UnknownHostException {
        localAddr = null;
        if (!"0.0.0.0".equals(Config.frontend_address)) {
            if (!InetAddressValidator.getInstance().isValidInet4Address(Config.frontend_address)) {
                throw new UnknownHostException("invalid frontend_address: " + Config.frontend_address);
            }
            localAddr = InetAddress.getByName(Config.frontend_address);
            LOG.info("use configured address. {}", localAddr);
            return;
        }

        analyzePriorityCidrs();

        // if not set frontend_address, get a non-loopback ip
        List<InetAddress> hosts = new ArrayList<>();
        NetUtils.getHosts(hosts);
        if (hosts.isEmpty()) {
            LOG.error("fail to get localhost");
            System.exit(-1);
        }

        InetAddress loopBack = null;
        for (InetAddress addr : hosts) {
            LOG.debug("check ip address: {}", addr);
            if (addr instanceof Inet4Address) {
                if (addr.isLoopbackAddress()) {
                    loopBack = addr;
                } else if (!priorityCidrs.isEmpty()) {
                    if (isInPriorNetwork(addr.getHostAddress())) {
                        localAddr = addr;
                        break;
                    }
                } else {
                    localAddr = addr;
                    break;
                }
            }
        }

        // nothing found, use loopback addr
        if (localAddr == null) {
            localAddr = loopBack;
        }
        LOG.info("local address: {}.", localAddr);
    }

    public static InetAddress getLocalHost() {
        return localAddr;
    }

    public static String getLocalHostAddress() {
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
        String prior_cidrs = Config.priority_networks;
        if (Strings.isNullOrEmpty(prior_cidrs)) {
            return;
        }
        LOG.info("configured prior_cidrs value: {}", prior_cidrs);

        String[] cidrList = prior_cidrs.split(PRIORITY_CIDR_SEPARATOR);
        List<String> priorNetworks = Lists.newArrayList(cidrList);
        priorityCidrs.addAll(priorNetworks);
    }

    private static boolean isInPriorNetwork(String ip) {
        ip = ip.trim();
        for (String cidr : priorityCidrs) {
            cidr = cidr.trim();
            if (!cidr.contains("/")) {
                // it is not valid CIDR, compare ip directly.
                if (cidr.equals(ip)) {
                    return true;
                }
            } else {
                SubnetUtils.SubnetInfo subnetInfo = new SubnetUtils(cidr).getInfo();
                if (subnetInfo.isInRange(ip)) {
                    return true;
                }
            }
        }
        return false;
    }

}

