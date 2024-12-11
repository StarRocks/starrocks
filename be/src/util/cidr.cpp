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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/cidr.cpp

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

#include "util/cidr.h"

#include <arpa/inet.h>
<<<<<<< HEAD
=======
#include <sys/socket.h>

#include <algorithm>
#include <cstddef>
#include <exception>
#include <iterator>
#include <ostream>
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

#include "common/logging.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"

namespace starrocks {

CIDR::CIDR() = default;

<<<<<<< HEAD
void CIDR::reset() {
    _address = 0;
    _netmask = 0xffffffff;
}

bool CIDR::reset(const std::string& cidr_str) {
    reset();

    // check if have mask
    std::string cidr_format_str = cidr_str;
    size_t have_mask = cidr_str.find('/');
    if (have_mask == string::npos) {
        cidr_format_str.assign(cidr_str + "/32");
    }
    VLOG(2) << "cidr format str: " << cidr_format_str;

    std::vector<std::string> cidr_items = strings::Split(cidr_format_str, "/");
    if (cidr_items.size() != 2) {
        LOG(WARNING) << "wrong CIDR format. network=" << cidr_str;
        return false;
    }

    if (cidr_items[1].empty()) {
        LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str;
        return false;
    }

    uint32_t mask_length = 0;
    if (!safe_strtou32(cidr_items[1].c_str(), &mask_length)) {
        LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str;
        return false;
    }
    if (mask_length > 32) {
        LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str << ", mask_length=" << mask_length;
        return false;
    }

    uint32_t address = 0;
    if (!ip_to_int(cidr_items[0], &address)) {
        LOG(WARNING) << "wrong CIDR IP value. network=" << cidr_str;
        return false;
    }
    _address = address;

    _netmask = 0xffffffff;
    _netmask = _netmask << (32 - mask_length);
=======
constexpr std::uint8_t kIPv4Bits = 32;
constexpr std::uint8_t kIPv6Bits = 128;

bool CIDR::reset(const std::string& cidr_str) {
    auto slash = std::find(std::begin(cidr_str), std::end(cidr_str), '/');
    auto ip = (slash == std::end(cidr_str)) ? cidr_str : cidr_str.substr(0, slash - std::begin(cidr_str));

    if (inet_pton(AF_INET, ip.c_str(), _address.data())) {
        _family = AF_INET;
        _netmask_len = kIPv4Bits;
    } else if (inet_pton(AF_INET6, ip.c_str(), _address.data())) {
        _family = AF_INET6;
        _netmask_len = kIPv6Bits;
    } else {
        LOG(WARNING) << "Wrong CIDRIP format. network = " << cidr_str;
        return false;
    }

    if (slash == std::end(cidr_str)) {
        return true;
    }

    std::size_t pos;
    std::string suffix = std::string(slash + 1, std::end(cidr_str));
    int len;
    try {
        len = std::stoi(suffix, &pos);
    } catch (const std::exception& e) {
        LOG(WARNING) << "Wrong CIDR format. network = " << cidr_str << ", reason = " << e.what();
        return false;
    }

    if (pos != suffix.size()) {
        LOG(WARNING) << "Wrong CIDR format. network = " << cidr_str;
        return false;
    }

    if (len < 0 || len > _netmask_len) {
        LOG(WARNING) << "Wrong CIDR format. network = " << cidr_str;
        return false;
    }
    _netmask_len = len;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    return true;
}

bool CIDR::ip_to_int(const std::string& ip_str, uint32_t* value) {
<<<<<<< HEAD
    struct in_addr addr;
    int flag = inet_aton(ip_str.c_str(), &addr);
    if (flag == 0) {
        return false;
    }
    *value = ntohl(addr.s_addr);
    return true;
}

bool CIDR::contains(uint32_t ip_int) {
    if ((_address & _netmask) == (ip_int & _netmask)) {
=======
    struct in_addr addr_v4;
    int flag = inet_aton(ip_str.c_str(), &addr_v4);
    if (flag == 1) {
        *value = ntohl(addr_v4.s_addr);
        return true;
    }
    struct in6_addr addr_v6;
    flag = inet_pton(AF_INET6, ip_str.c_str(), &addr_v6);
    if (flag == 1) {
        if (IN6_IS_ADDR_V4MAPPED(&addr_v6)) {
            *value = ntohl(*(reinterpret_cast<uint32_t*>(&addr_v6.s6_addr[12])));
            return true;
        }
        *value = static_cast<uint32_t>(addr_v6.s6_addr32[3]);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return true;
    }
    return false;
}

<<<<<<< HEAD
bool CIDR::contains(const std::string& ip) {
    uint32_t ip_int = 0;
    if (!ip_to_int(ip, &ip_int)) {
        return false;
    }

    return contains(ip_int);
=======
bool CIDR::contains(const CIDR& ip) const {
    if ((_family != ip._family) || (_netmask_len > ip._netmask_len)) {
        return false;
    }
    auto bytes = _netmask_len / 8;
    auto cidr_begin = _address.cbegin();
    auto ip_begin = ip._address.cbegin();
    if (!std::equal(cidr_begin, cidr_begin + bytes, ip_begin, ip_begin + bytes)) {
        return false;
    }
    if ((_netmask_len % 8) == 0) {
        return true;
    }
    auto mask = (0xFF << (8 - (_netmask_len % 8))) & 0xFF;
    return (_address[bytes] & mask) == (ip._address[bytes] & mask);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
}

} // end namespace starrocks
