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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/NetUtils.java

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

package com.starrocks.common.util;

import com.google.common.base.Strings;
import com.starrocks.common.structure.Pair;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class NetUtils {

    public static List<InetAddress> getHosts() {
        Enumeration<NetworkInterface> n = null;
        List<InetAddress> hosts = new ArrayList<>();
        try {
            n = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e1) {
            throw new RuntimeException("failed to get network interfaces");
        }

        while (n.hasMoreElements()) {
            NetworkInterface e = n.nextElement();
            Enumeration<InetAddress> a = e.getInetAddresses();
            while (a.hasMoreElements()) {
                InetAddress addr = a.nextElement();
                hosts.add(addr);
            }
        }
        return hosts;
    }

    public static boolean isPortUsing(String host, int port) throws UnknownHostException {
        boolean flag = false;
        try (Socket socket = new Socket(host, port)) {
            flag = true;
        } catch (IOException e) {
            // do nothing
        }
        return flag;
    }

    public static Pair<String, String> getIpAndFqdnByHost(String host) throws UnknownHostException {

        String ip = "";
        String fqdn = "";
        if (InetAddressValidator.getInstance().isValidInet4Address(host)) {
            // ipOrFqdn is ip
            ip = host;
        } else {
            // ipOrFqdn is fqdn
            ip = InetAddress.getByName(host).getHostAddress();
            if (Strings.isNullOrEmpty(ip)) {
                throw new UnknownHostException("got a wrong ip");
            }
            fqdn = host;
        }
        return new Pair<>(ip, fqdn);
    }

    public static boolean checkAccessibleForAllPorts(String host, List<Integer> ports) {
        boolean accessible = true;
        int timeout = 1000; // Timeout in milliseconds
        for (Integer port : ports) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), timeout);
            } catch (IOException e) {
                accessible = false;
                break;
            }
        }
        return accessible;
    }
}
