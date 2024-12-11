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

#include "service/backend_options.h"

#include <algorithm>
<<<<<<< HEAD
=======
#include <ostream>
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "util/cidr.h"
#include "util/network_util.h"

namespace starrocks {

static const std::string PRIORITY_CIDR_SEPARATOR = ";";

std::string BackendOptions::_s_localhost;
std::vector<CIDR> BackendOptions::_s_priority_cidrs;
TBackend BackendOptions::_backend;
<<<<<<< HEAD
=======
bool BackendOptions::_bind_ipv6 = false;
const char* _service_bind_address = "0.0.0.0";
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

bool BackendOptions::_is_cn = false;

bool BackendOptions::is_cn() {
    return _is_cn;
}

bool BackendOptions::init(bool is_cn) {
    _is_cn = is_cn;
    if (!analyze_priority_cidrs()) {
        return false;
    }
    std::vector<InetAddress> hosts;
<<<<<<< HEAD
    Status status = get_hosts_v4(&hosts);

    if (!status.ok()) {
        LOG(FATAL) << status.get_error_msg();
=======
    Status status = get_hosts(&hosts);

    if (!status.ok()) {
        LOG(FATAL) << status.message();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return false;
    }

    if (hosts.empty()) {
        LOG(FATAL) << "failed to get host";
        return false;
    }

    std::string loopback;
    auto addr_it = hosts.begin();
<<<<<<< HEAD
    for (; addr_it != hosts.end(); ++addr_it) {
        if ((*addr_it).is_address_v4()) {
            VLOG(2) << "check ip=" << addr_it->get_host_address_v4();
            if ((*addr_it).is_loopback_v4()) {
                loopback = addr_it->get_host_address_v4();
            } else if (!_s_priority_cidrs.empty()) {
                if (is_in_prior_network(addr_it->get_host_address_v4())) {
                    _s_localhost = addr_it->get_host_address_v4();
                    break;
                }
            } else {
                _s_localhost = addr_it->get_host_address_v4();
                break;
            }
=======
    // If `priority_networks` is configured, find a possible ip matching the configuration first.
    // Otherwise, find other usable ip as if `priority_networks` is not configured.
    if (!_s_priority_cidrs.empty()) {
        for (; addr_it != hosts.end(); ++addr_it) {
            LOG(INFO) << "check ip = " << addr_it->get_host_address();
            // Whether to use IPv4 or IPv6, it's configured by CIDR format.
            // If both IPv4 and IPv6 are configured, the config order decides priority.
            if (is_in_prior_network(addr_it->get_host_address())) {
                _s_localhost = addr_it->get_host_address();
                _bind_ipv6 = addr_it->is_ipv6();
                break;
            }
            LOG(INFO) << "skip ip not belonged to priority networks: " << addr_it->get_host_address();
        }
        if (_s_localhost.empty()) {
            LOG(WARNING) << "ip address range configured for priority_networks does not include the current IP address,"
                         << " will try other usable ip";
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }

    if (_s_localhost.empty()) {
<<<<<<< HEAD
        LOG(INFO) << "fail to find one valid non-loopback address, use loopback address.";
=======
        for (; addr_it != hosts.end(); ++addr_it) {
            LOG(INFO) << "check ip = " << addr_it->get_host_address();
            if (addr_it->is_loopback()) {
                loopback = addr_it->get_host_address();
                _bind_ipv6 = addr_it->is_ipv6();
            } else {
                bool is_ipv6 = addr_it->is_ipv6();
                _bind_ipv6 = is_ipv6;
                if (config::net_use_ipv6_when_priority_networks_empty) {
                    if (is_ipv6) {
                        _s_localhost = addr_it->get_host_address();
                    }
                } else if (!is_ipv6) {
                    _s_localhost = addr_it->get_host_address();
                }
                if (!_s_localhost.empty()) {
                    // Use the first found one.
                    break;
                }
            }
        }
    }

    if (_bind_ipv6) {
        _service_bind_address = "[::0]";
    }

    if (_s_localhost.empty()) {
        LOG(WARNING) << "failed to find one valid non-loopback address, use loopback address.";
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        _s_localhost = loopback;
    }
    LOG(INFO) << "localhost " << _s_localhost;
    return true;
}

std::string BackendOptions::get_localhost() {
    return _s_localhost;
}

<<<<<<< HEAD
=======
bool BackendOptions::is_bind_ipv6() {
    return _bind_ipv6;
}

const char* BackendOptions::get_service_bind_address() {
    return _service_bind_address;
}

const char* BackendOptions::get_service_bind_address_without_bracket() {
    if (_bind_ipv6) {
        return "::0";
    }
    return _service_bind_address;
}

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
TBackend BackendOptions::get_localBackend() {
    _backend.__set_host(_s_localhost);
    _backend.__set_be_port(config::be_port);
    _backend.__set_http_port(config::be_http_port);
    return _backend;
}

void BackendOptions::set_localhost(const std::string& host) {
    _s_localhost = host;
}

bool BackendOptions::analyze_priority_cidrs() {
    if (config::priority_networks == "") {
        return true;
    }
    LOG(INFO) << "priority cidrs in conf: " << config::priority_networks;

    std::vector<std::string> cidr_strs = strings::Split(config::priority_networks, PRIORITY_CIDR_SEPARATOR);

    for (auto& cidr_str : cidr_strs) {
        CIDR cidr;
        if (!cidr.reset(cidr_str)) {
            LOG(FATAL) << "wrong cidr format. cidr_str=" << cidr_str;
            return false;
        }
        _s_priority_cidrs.push_back(cidr);
    }
    return true;
}

bool BackendOptions::is_in_prior_network(const std::string& ip) {
    for (auto& cidr : _s_priority_cidrs) {
<<<<<<< HEAD
        if (cidr.contains(ip)) {
=======
        CIDR _ip;
        _ip.reset(ip);
        if (cidr.contains(_ip)) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            return true;
        }
    }
    return false;
}

} // namespace starrocks
