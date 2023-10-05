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

import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.net.InetAddress;
import java.net.UnknownHostException;

// This class is used to translate hostname to IP.
// Because JVM has DNS cache already, this class doesn't maintain its own cache.
public class DnsCache {
    public static TNetworkAddress lookup(TNetworkAddress address) throws UnknownHostException {
        if (InetAddressValidator.getInstance().isValidInet4Address(address.hostname)) {
            return address;
        }
        return new TNetworkAddress(InetAddress.getByName(address.hostname).getHostAddress(), address.port);
    }
}
