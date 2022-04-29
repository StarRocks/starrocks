// This file is made available under Elastic License 2.0.
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.List;

public class NetUtils {

    public static void getHosts(List<InetAddress> hosts) {
        Enumeration<NetworkInterface> n = null;

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

    public static boolean validIPAddress(String hostname) {
        String[] nums = hostname.split("\\.", -1);
        for (String x : nums) {
            if (x.length() == 0 || x.length() > 3) { 
                return false; 
            }
            if (x.charAt(0) == '0' && x.length() != 1) { 
                return false; 
            }
            for (char ch : x.toCharArray()) {
                if (!Character.isDigit(ch)) { 
                    return false;
                }
            }
            if (Integer.parseInt(x) > 255) { 
                return false; 
            }
        }
        return true;
    }
}
