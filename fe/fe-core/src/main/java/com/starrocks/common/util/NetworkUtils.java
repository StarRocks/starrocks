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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/KafkaUtil.java

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

import com.google.common.net.InetAddresses;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class NetworkUtils {
    public static class NetworkAddress {
        @SerializedName("h")
        public String hostname;
        @SerializedName("p")
        public int port;

        public NetworkAddress() {

        }

        public NetworkAddress(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof NetworkAddress
                    && this.hostname.equals(((NetworkAddress) obj).hostname)
                    && this.port == ((NetworkAddress) obj).port;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hostname, port);
        }

        @Override
        public String toString() {
            return hostname + ":" + port;
        }
    }

    private static boolean isFQDN(String host) {
        return !InetAddresses.isInetAddress(host);
    }

    /**
     * Try to resolve the fqdn to ip if necessary
     * @param host fqdn
     * @return ip
     * @throws RuntimeException
     */
    public static String resolveFQDNIfNecessary(String host) throws RuntimeException {
        if (Config.query_coordinator_enable_fqdn_resolve && isFQDN(host)) {
            try {
                InetAddress address = InetAddress.getByName(host);
                return address.getHostAddress();
            } catch (UnknownHostException e) {
                throw new FQDNResolveException("resolve fqdn " + host + "failed, " + e.getMessage(), e);
            }
        }
        return host;
    }
}

