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

package com.starrocks.common.util;

import org.apache.commons.validator.routines.InetAddressValidator;

import java.net.InetAddress;
import java.net.UnknownHostException;

// This class is used to translate hostname to IP.
// Because JVM has DNS cache already, this class doesn't maintain its own cache.
public class DnsCache {
    public static String lookup(String hostname) throws UnknownHostException {
        if (InetAddressValidator.getInstance().isValidInet4Address(hostname)) {
            return hostname;
        }
        return InetAddress.getByName(hostname).getHostAddress();
    }

    // This function will try to look up the given hostname. If there is an exception, this function will
    // return the input hostname directly.
    public static String tryLookup(String hostname) {
        try {
            return lookup(hostname);
        } catch (UnknownHostException e) {
            return hostname;
        }
    }
}
