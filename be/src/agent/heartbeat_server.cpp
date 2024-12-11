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
//   https://github.com/apache/incubator-doris/blob/master/be/src/agent/heartbeat_server.cpp

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

#include "agent/heartbeat_server.h"

#include <fmt/format.h>
#include <thrift/TProcessor.h>

#include <atomic>
#include <ctime>
#include <fstream>
<<<<<<< HEAD

#include "agent/master_info.h"
=======
#include <sstream>

#include "agent/master_info.h"
#include "common/process_exit.h"
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
#include "common/status.h"
#include "gen_cpp/HeartbeatService.h"
#include "runtime/heartbeat_flags.h"
#include "service/backend_options.h"
#include "storage/storage_engine.h"
#include "util/debug_util.h"
#include "util/network_util.h"
#include "util/thrift_server.h"

using std::fstream;
using std::nothrow;
using std::string;
using std::vector;
using apache::thrift::transport::TProcessor;

namespace starrocks {
<<<<<<< HEAD
extern std::atomic<bool> k_starrocks_exit;
extern std::atomic<bool> k_starrocks_exit_quick;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

static int64_t reboot_time = 0;

HeartbeatServer::HeartbeatServer() : _olap_engine(StorageEngine::instance()) {}

void HeartbeatServer::init_cluster_id_or_die() {
    auto info = get_master_info();
    info.cluster_id = _olap_engine->effective_cluster_id();
    bool r = update_master_info(info);
    CHECK(r) << "Fail to update master info";
}

void HeartbeatServer::heartbeat(THeartbeatResult& heartbeat_result, const TMasterInfo& master_info) {
    //print heartbeat in every minute
<<<<<<< HEAD
    LOG_EVERY_N(INFO, 12) << "get heartbeat from FE."
                          << "host:" << master_info.network_address.hostname
                          << ", port:" << master_info.network_address.port << ", cluster id:" << master_info.cluster_id
                          << ", run_mode:" << master_info.run_mode << ", counter:" << google::COUNTER;

    // do heartbeat
    StatusOr<CmpResult> res = compare_master_info(master_info);
=======
    LOG_EVERY_N(INFO, 12) << "get heartbeat from FE. host:" << master_info.network_address.hostname
                          << ", port:" << master_info.network_address.port << ", cluster id:" << master_info.cluster_id
                          << ", run_mode:" << master_info.run_mode << ", counter:" << google::COUNTER;

    if (master_info.encrypted != config::enable_transparent_data_encryption) {
        LOG(FATAL) << "inconsistent encryption config, FE encrypted:" << master_info.encrypted
                   << " BE/CN:" << config::enable_transparent_data_encryption;
    }

    StatusOr<CmpResult> res;
    // reject master's heartbeat when exit
    if (process_exit_in_progress()) {
        res = Status::Shutdown("BE is shutting down");
    } else {
        res = compare_master_info(master_info);
    }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    res.status().to_thrift(&heartbeat_result.status);
    if (!res.ok()) {
        MasterInfoPtr ptr;
        if (get_master_info(&ptr)) {
<<<<<<< HEAD
            LOG(WARNING) << "Fail to handle heartbeat: " << res.status() << " cached master info: " << *ptr
                         << " received master info: " << master_info;
=======
            LOG(WARNING) << "Fail to handle heartbeat: " << res.status()
                         << " cached master info: " << print_master_info(*ptr)
                         << " received master info: " << print_master_info(master_info);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        } else {
            LOG(WARNING) << "Fail to handle heartbeat: " << res.status();
        }
    } else if (*res == kNeedUpdate) {
<<<<<<< HEAD
        LOG(INFO) << "Updating master info: " << master_info;
=======
        LOG(INFO) << "Updating master info: " << print_master_info(master_info);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        bool r = update_master_info(master_info);
        LOG_IF(WARNING, !r) << "Fail to update master info, maybe the master info has been updated by another thread "
                               "with a larger epoch";
    } else if (*res == kNeedUpdateAndReport) {
<<<<<<< HEAD
        LOG(INFO) << "Updating master info: " << master_info;
=======
        LOG(INFO) << "Updating master info: " << print_master_info(master_info);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        bool r = update_master_info(master_info);
        LOG_IF(WARNING, !r) << "Fail to update master info, maybe the master info has been updated by another thread "
                               "with a larger epoch";
        if (r) {
            LOG(INFO) << "Master FE is changed or restarted. report tablet and disk info immediately";
            _olap_engine->trigger_report();
        }
    } else {
        DCHECK_EQ(kUnchanged, *res);
        // nothing to do
    }

    if (master_info.__isset.disabled_disks) {
        _olap_engine->disable_disks(master_info.disabled_disks);
    }

    if (master_info.__isset.decommissioned_disks) {
        _olap_engine->decommission_disks(master_info.decommissioned_disks);
    }

    static auto num_hardware_cores = static_cast<int32_t>(CpuInfo::num_cores());
    if (res.ok()) {
        heartbeat_result.backend_info.__set_be_port(config::be_port);
        heartbeat_result.backend_info.__set_http_port(config::be_http_port);
        heartbeat_result.backend_info.__set_be_rpc_port(-1);
        heartbeat_result.backend_info.__set_brpc_port(config::brpc_port);
<<<<<<< HEAD
=======
        heartbeat_result.backend_info.__set_arrow_flight_port(config::arrow_flight_port);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
#ifdef USE_STAROS
        heartbeat_result.backend_info.__set_starlet_port(config::starlet_port);
        if (StorageEngine::instance()->get_store_num() != 0) {
            heartbeat_result.backend_info.__set_is_set_storage_path(true);
        } else {
            heartbeat_result.backend_info.__set_is_set_storage_path(false);
        }
#endif
        heartbeat_result.backend_info.__set_version(get_short_version());
        heartbeat_result.backend_info.__set_num_hardware_cores(num_hardware_cores);
<<<<<<< HEAD
=======
        heartbeat_result.backend_info.__set_mem_limit_bytes(GlobalEnv::GetInstance()->process_mem_tracker()->limit());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        if (reboot_time == 0) {
            std::time_t currTime = std::time(nullptr);
            reboot_time = static_cast<int64_t>(currTime);
        }
        heartbeat_result.backend_info.__set_reboot_time(reboot_time);
    }
<<<<<<< HEAD

    // Temporary solution for modifying the default value of disable_column_pool in lower versions
    // to avoid behavior changes.
    if (!config::disable_column_pool && master_info.run_mode == TRunMode::SHARED_DATA) {
        LOG(INFO) << "config::disable_column_pool is set true in shared_data mode";
        config::disable_column_pool = true;
    }
=======
}

std::string HeartbeatServer::print_master_info(const TMasterInfo& master_info) const {
    std::ostringstream out;
    if (!master_info.__isset.token) {
        master_info.printTo(out);
    } else {
        TMasterInfo master_info_copy(master_info);
        master_info_copy.__set_token("<hidden>");
        master_info_copy.printTo(out);
    }
    return out.str();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
}

StatusOr<HeartbeatServer::CmpResult> HeartbeatServer::compare_master_info(const TMasterInfo& master_info) {
    static const char* LOCALHOST = "127.0.0.1";
<<<<<<< HEAD

    // reject master's heartbeat when exit
    if (k_starrocks_exit.load(std::memory_order_relaxed) || k_starrocks_exit_quick.load(std::memory_order_relaxed)) {
        return Status::InternalError("BE is shutting down");
    }
=======
    static const char* LOCALHOST_IPV6 = "::1";
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    MasterInfoPtr curr_master_info;
    if (!get_master_info(&curr_master_info)) {
        return Status::InternalError("Fail to get local master info");
    }

    if (master_info.epoch < curr_master_info->epoch) {
        return Status::InternalError("Out-dated epoch");
    }

    if (master_info.__isset.token && curr_master_info->__isset.token && master_info.token != curr_master_info->token) {
        return Status::InternalError("Unmatched token");
    }

    if (curr_master_info->cluster_id != -1 && curr_master_info->cluster_id != master_info.cluster_id) {
        return Status::InternalError("Unmatched cluster id");
    }

<<<<<<< HEAD
    if ((master_info.network_address.hostname == LOCALHOST) && (master_info.backend_ip != LOCALHOST)) {
        return Status::InternalError("FE heartbeat with localhost ip but BE is not deployed on the same machine");
=======
    if ((master_info.network_address.hostname == LOCALHOST)) {
        if (!(master_info.backend_ip == LOCALHOST || master_info.backend_ip == LOCALHOST_IPV6)) {
            return Status::InternalError("FE heartbeat with localhost ip but BE is not deployed on the same machine");
        }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

#ifndef USE_STAROS
    if (master_info.run_mode == TRunMode::SHARED_DATA) {
        LOG_EVERY_N(ERROR, 12)
                << "This program is not compiled with SHARED_DATA support, but FE is running in SHARED_DATA mode!";
        return Status::InternalError("Backend service binary was not compiled with SHARED_DATA support!");
    }
#endif

    if (master_info.__isset.backend_ip) {
<<<<<<< HEAD
        if (master_info.backend_ip != BackendOptions::get_localhost()) {
            LOG(WARNING) << master_info.backend_ip << " not equal to to backend localhost "
                         << BackendOptions::get_localhost();
            bool fe_saved_is_valid_ip = is_valid_ip(master_info.backend_ip);
            if (fe_saved_is_valid_ip && is_valid_ip(BackendOptions::get_localhost())) {
                return Status::InternalError("FE saved address not match backend address");
            }

=======
        //master_info.backend_ip may be an IP or domain name, and it should be renamed 'backend_host', as it requires compatibility with historical versions, the name is still 'backend_ ip'
        if (master_info.backend_ip != BackendOptions::get_localhost()) {
            LOG(WARNING) << master_info.backend_ip << " not equal to to backend localhost "
                         << BackendOptions::get_localhost();
            // step1: check master_info.backend_ip is IP or FQDN
            bool fe_saved_is_valid_ip = is_valid_ip(master_info.backend_ip);
            if (fe_saved_is_valid_ip && is_valid_ip(BackendOptions::get_localhost())) {
                // if master_info.backend_ip is IP,and not equal with BackendOptions::get_localhost(),return error
                return Status::InternalError("FE saved address not match backend address");
            }

            //step2: resolve FQDN to IP
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            std::string ip;
            if (fe_saved_is_valid_ip) {
                ip = master_info.backend_ip;
            } else {
<<<<<<< HEAD
                ip = hostname_to_ip(master_info.backend_ip);
                if (ip.empty()) {
                    std::stringstream err_msg;
                    err_msg << "Can not get ip from fqdn, fqdn is: " << master_info.backend_ip;
                    LOG(WARNING) << err_msg.str();
                    return Status::InternalError(err_msg.str());
                }
            }

            std::vector<InetAddress> hosts;
            RETURN_IF_ERROR(get_hosts_v4(&hosts));
            if (hosts.empty()) {
                return Status::InternalError("get_hosts_v4 is empty");
            }

            bool set_new_localhost = false;

            for (auto& host : hosts) {
                if (host.is_address_v4() && host.get_host_address_v4() == ip) {
=======
                Status status = hostname_to_ip(master_info.backend_ip, ip, BackendOptions::is_bind_ipv6());
                if (!status.ok()) {
                    LOG(WARNING) << "Can not get ip from fqdn, fqdn is: " << master_info.backend_ip
                                 << ", binding ipv6: " << BackendOptions::is_bind_ipv6()
                                 << ", status: " << status.to_string();
                    return status;
                }
                LOG(INFO) << "resolved from fqdn: " << master_info.backend_ip << " to ip: " << ip;
            }

            //step3: get all ips of the interfaces on this machine
            std::vector<InetAddress> hosts;
            RETURN_IF_ERROR(get_hosts(&hosts));
            if (hosts.empty()) {
                std::stringstream err_msg;
                err_msg << "the status was not ok when get_hosts.";
                LOG(WARNING) << err_msg.str();
                return Status::InternalError(err_msg.str());
            }

            //step4: check if the IP of FQDN belongs to the current machine and update BackendOptions._s_localhost
            bool set_new_localhost = false;

            for (auto& host : hosts) {
                if (host.get_host_address() == ip) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    BackendOptions::set_localhost(master_info.backend_ip);
                    set_new_localhost = true;
                    break;
                }
            }

            if (!set_new_localhost) {
                return Status::InternalError("Unmatched backend ip");
            }

            LOG(INFO) << "update localhost done, the new localhost is " << BackendOptions::get_localhost();
        }
    }

    // Check cluster id
    if (curr_master_info->cluster_id == -1) {
        // write and update cluster id
        if (_olap_engine->get_need_write_cluster_id()) {
            LOG(INFO) << "Received first heartbeat. updating cluster id";
            RETURN_IF_ERROR(_olap_engine->set_cluster_id(master_info.cluster_id));
        }
    }

    if (master_info.__isset.heartbeat_flags) {
        HeartbeatFlags* heartbeat_flags = ExecEnv::GetInstance()->heartbeat_flags();
        heartbeat_flags->update(master_info.heartbeat_flags);
    }

    if (curr_master_info->network_address != master_info.network_address) {
        return kNeedUpdateAndReport;
    }
    if (*curr_master_info != master_info) {
        return kNeedUpdate;
    }
    return kUnchanged;
}

StatusOr<std::unique_ptr<ThriftServer>> create_heartbeat_server(ExecEnv* exec_env, uint32_t server_port,
                                                                uint32_t worker_thread_num) {
    auto* heartbeat_server = new HeartbeatServer();
    heartbeat_server->init_cluster_id_or_die();

    std::shared_ptr<HeartbeatServer> handler(heartbeat_server);
    std::shared_ptr<TProcessor> server_processor(new HeartbeatServiceProcessor(handler));
    return std::make_unique<ThriftServer>("heartbeat", server_processor, server_port, exec_env->metrics(),
                                          worker_thread_num);
}
} // namespace starrocks
