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

#include "engine_replication_task.h"

#include <fmt/format.h>
#include <sys/stat.h>

#include <filesystem>
#include <set>

#include "agent/agent_common.h"
#include "agent/finish_task.h"
#include "agent/master_info.h"
#include "agent/task_signatures_manager.h"
#include "common/status.h"
#include "fs/fs.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/Types_constants.h"
#include "gutil/strings/split.h"
#include "gutil/strings/stringpiece.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "runtime/client_cache.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/snapshot_manager.h"
#include "storage/tablet_updates.h"
#include "util/defer_op.h"
#include "util/string_parser.hpp"
#include "util/thrift_rpc_helper.h"

using std::set;
using std::stringstream;
using strings::Substitute;
using strings::Split;
using strings::SkipWhitespace;

using namespace fmt::literals;

namespace starrocks {

const std::string HTTP_REQUEST_PREFIX = "/api/_tablet/_download";
const uint32_t DOWNLOAD_FILE_MAX_RETRY = 3;
const uint32_t LIST_REMOTE_FILE_TIMEOUT = 15;
const uint32_t GET_LENGTH_TIMEOUT = 10;

EngineReplicationTask::EngineReplicationTask(MemTracker* mem_tracker, const TReplicationRequest& replication_req,
                                             int64_t signature, std::vector<string>* error_msgs,
                                             std::vector<TTabletInfo>* tablet_infos, AgentStatus* res_status)
        : _replication_req(replication_req),
          _error_msgs(error_msgs),
          _tablet_infos(tablet_infos),
          _res_status(res_status),
          _signature(signature) {
    _mem_tracker = std::make_unique<MemTracker>(-1, "replication task", mem_tracker);
}

Status EngineReplicationTask::execute() {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker.get());
    auto tablet_manager = StorageEngine::instance()->tablet_manager();
    // Prevent the snapshot directory from been removed by the path GC worker.
    tablet_manager->register_clone_tablet(_replication_req.tablet_id);
    DeferOp op([&] {
        tls_thread_status.set_mem_tracker(prev_tracker);
        tablet_manager->unregister_clone_tablet(_replication_req.tablet_id);
    });
    auto tablet = tablet_manager->get_tablet(_replication_req.tablet_id, false);
    if (tablet != nullptr) {
        std::shared_lock rlock(tablet->get_migration_lock());
        if (Tablet::check_migrate(tablet)) {
            return Status::Corruption("Fail to check migrate tablet");
        }
        auto st = _do_clone(tablet.get());
        _set_tablet_info(st, false);
    } else {
        auto st = _do_clone(nullptr);
        _set_tablet_info(st, true);
    }

    return Status::OK();
}

static string version_list_to_string(const std::vector<Version>& versions) {
    std::ostringstream str;
    size_t last = 0;
    for (size_t i = last + 1; i <= versions.size(); i++) {
        if (i == versions.size() || versions[last].second + 1 != versions[i].first) {
            if (versions[last].first == versions[i - 1].second) {
                str << versions[last].first << ",";
            } else {
                str << versions[last].first << "-" << versions[i - 1].second << ",";
            }
            last = i;
        }
    }
    return str.str();
}

static inline Status convert_tablet_meta_file(const std::string& tablet_snapshot_path,
                                              const TReplicationRequest& request) {
    std::string src_header_file_path = tablet_snapshot_path + std::to_string(request.src_tablet_id) + ".hdr";
    if (fs::path_exist(src_header_file_path)) {
        TabletMeta tablet_meta;
        Status status = tablet_meta.create_from_file(src_header_file_path);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to load tablet meta from " << src_header_file_path << ", " << status;
            return status;
        }

        TabletMetaPB tablet_meta_pb;
        tablet_meta.to_meta_pb(&tablet_meta_pb);
        tablet_meta_pb.set_table_id(request.table_id);
        tablet_meta_pb.set_partition_id(request.partition_id);
        tablet_meta_pb.set_tablet_id(request.tablet_id);

        std::string header_file_path = tablet_snapshot_path + std::to_string(request.tablet_id) + ".hdr";
        status = TabletMeta::save(header_file_path, tablet_meta_pb);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to save tablet meta pb to " << header_file_path << ", " << status;
            return status;
        }

        if (request.tablet_id != request.src_tablet_id) {
            status = fs::remove(src_header_file_path);
            if (!status.ok()) {
                LOG(WARNING) << "Fail to remove tablet meta " << src_header_file_path << ", " << status;
            }
        }

        return status;
    }

    // TODO: convert meta file and dcgs_snapshot file
    return Status::OK();
}

Status EngineReplicationTask::_do_clone(Tablet* tablet) {
    Status status;
    // try to repair a tablet with missing version
    if (tablet != nullptr) {
        string download_path = tablet->schema_hash_path() + CLONE_PREFIX;
        DeferOp defer([&]() { (void)fs::remove_all(download_path); });

        std::vector<Version> missed_versions;
        tablet->calc_missed_versions(_replication_req.snapshot_version, &missed_versions);
        if (missed_versions.size() == 0) {
            LOG(INFO) << "Cloning existing tablet skipped, no missing version. tablet:" << tablet->tablet_id()
                      << " type:" << KeysType_Name(tablet->keys_type())
                      << " version:" << _replication_req.snapshot_version;
            return Status::OK();
        }
        LOG(INFO) << "Cloning existing tablet. "
                  << " tablet:" << _replication_req.tablet_id << " type:" << KeysType_Name(tablet->keys_type())
                  << " version:" << _replication_req.snapshot_version
                  << " missed_versions=" << version_list_to_string(missed_versions);
        status = _clone_copy(*tablet->data_dir(), download_path, _error_msgs, &missed_versions);
        bool incremental_clone = true;
        if (!status.ok()) {
            LOG(INFO) << "Fail to do incremental clone: " << status
                      << ". switched to fully clone. tablet_id=" << tablet->tablet_id();
            incremental_clone = false;
            status = _clone_copy(*tablet->data_dir(), download_path, _error_msgs, nullptr);
        }

        if (status.ok()) {
            status = _finish_clone(tablet, download_path, _replication_req.snapshot_version, incremental_clone);
        }

        if (!status.ok()) {
            LOG(WARNING) << "Fail to load snapshot:" << status << ". tablet:" << tablet->tablet_id();
            _error_msgs->push_back(status.to_string());
        }
        return status;
    } else {
        LOG(ERROR) << "Tablet not found. tablet: " << _replication_req.tablet_id;
        return Status::NotFound("Tablet not found, tablet: " + std::to_string(_replication_req.tablet_id));
    }
}

void EngineReplicationTask::_set_tablet_info(Status status, bool is_new_tablet) {
    // Get clone tablet info
    if (status.ok()) {
        TTabletInfo tablet_info;
        tablet_info.__set_tablet_id(_replication_req.tablet_id);
        tablet_info.__set_schema_hash(_replication_req.schema_hash);
        status = StorageEngine::instance()->tablet_manager()->report_tablet_info(&tablet_info);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to report tablet info after clone."
                         << " tablet id=" << _replication_req.tablet_id
                         << " schema hash=" << _replication_req.schema_hash << " signature=" << _signature;
            _error_msgs->push_back("clone success, but get tablet info failed.");
        } else if (_replication_req.__isset.snapshot_version &&
                   tablet_info.version < _replication_req.snapshot_version) {
            LOG(WARNING) << "Fail to clone tablet. tablet_id:" << _replication_req.tablet_id
                         << ", schema_hash:" << _replication_req.schema_hash << ", signature:" << _signature
                         << ", version:" << tablet_info.version
                         << ", expected_version: " << _replication_req.snapshot_version;
            // if it is a new tablet and clone failed, then remove the tablet
            // if it is incremental clone, then must not drop the tablet
            if (is_new_tablet) {
                // we need to check if this cloned table's version is what we expect.
                // if not, maybe this is a stale remaining table which is waiting for drop.
                // we drop it.
                LOG(WARNING) << "Dropping the stale tablet. tablet_id:" << _replication_req.tablet_id
                             << ", schema_hash:" << _replication_req.schema_hash << ", signature:" << _signature
                             << ", version:" << tablet_info.version
                             << ", expected_version: " << _replication_req.snapshot_version;
                Status drop_status =
                        StorageEngine::instance()->tablet_manager()->drop_tablet(_replication_req.tablet_id);
                if (!drop_status.ok() && !drop_status.is_not_found()) {
                    // just log
                    LOG(WARNING) << "Fail to drop stale cloned table. tablet id=" << _replication_req.tablet_id;
                }
            }
            status = Status::InternalError("new version less than committed version");
        } else {
            LOG(INFO) << "clone get tablet info success. tablet_id:" << _replication_req.tablet_id
                      << ", schema_hash:" << _replication_req.schema_hash << ", signature:" << _signature
                      << ", version:" << tablet_info.version
                      << ", min_readable_version:" << tablet_info.min_readable_version;
            _tablet_infos->push_back(tablet_info);
        }
    }
    *_res_status = status.ok() ? STARROCKS_SUCCESS : STARROCKS_ERROR;
}

Status EngineReplicationTask::_clone_copy(DataDir& data_dir, const string& local_data_path,
                                          std::vector<string>* error_msgs, const std::vector<Version>* missed_versions,
                                          const std::vector<int64_t>* missing_version_ranges) {
    std::string local_path = local_data_path + "/";

    int timeout_s = 0;
    if (_replication_req.__isset.timeout_s) {
        timeout_s = _replication_req.timeout_s;
    }
    Status st;
    for (auto& src : _replication_req.src_backends) {
        // Make snapshot in remote olap engine
        std::string snapshot_path;
        st = _make_snapshot(src.host, src.be_port, _replication_req.src_tablet_id, _replication_req.schema_hash,
                            _replication_req.snapshot_version, timeout_s, missed_versions, missing_version_ranges,
                            &snapshot_path);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to make snapshot from " << src.host << ": " << st.to_string()
                         << " tablet:" << _replication_req.tablet_id
                         << " src tablet:" << _replication_req.src_tablet_id;
            error_msgs->push_back("make snapshot failed. backend_ip: " + src.host);
            continue;
        }

        std::string download_url =
                strings::Substitute("http://$0:$1$2?token=$3&type=V2&file=$4/$5/$6/", src.host, src.http_port,
                                    HTTP_REQUEST_PREFIX, _replication_req.src_token, snapshot_path,
                                    _replication_req.src_tablet_id, _replication_req.schema_hash);

        st = _download_files(&data_dir, download_url, local_path);
        (void)_release_snapshot(src.host, src.be_port, snapshot_path);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to download snapshot from " << download_url << ": " << st.to_string()
                         << " tablet:" << _replication_req.tablet_id
                         << " src tablet:" << _replication_req.src_tablet_id;
            error_msgs->push_back("download snapshot failed. backend_ip: " + src.host);
            continue;
        }

        st = convert_tablet_meta_file(local_path, _replication_req);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to convert tablet meta file: " << st.to_string()
                         << " tablet:" << _replication_req.tablet_id;
            continue;
        }

        // Is it really necessary to assign a new rowset id for each rowset?
        // The `RowsetId` was generated by the `UniqueRowsetIdGenerator` with UUID
        // as part of its field, should be enough to avoid the conflicts with each other.
        st = SnapshotManager::instance()->convert_rowset_ids(local_path, _replication_req.tablet_id,
                                                             _replication_req.schema_hash);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to convert rowset ids: " << st.to_string()
                         << " tablet:" << _replication_req.tablet_id;
            error_msgs->push_back("convert rowset id failed. backend_ip: " + src.host);
            continue;
        }
        LOG(INFO) << "Cloned snapshot from " << download_url << " to " << local_data_path;
        break;
    }
    return st;
}

Status EngineReplicationTask::_make_snapshot(const std::string& ip, int port, TTableId tablet_id,
                                             TSchemaHash schema_hash, TVersion version, int timeout_s,
                                             const std::vector<Version>* missed_versions,
                                             const std::vector<int64_t>* missing_version_ranges,
                                             std::string* snapshot_path) {
    bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
    if (bg_worker_stopped) {
        return Status::InternalError("Process is going to quit. The snapshot will stop.");
    }

    TSnapshotRequest request;
    request.__set_tablet_id(tablet_id);
    request.__set_schema_hash(schema_hash);
    request.__set_preferred_snapshot_format(g_Types_constants.TPREFER_SNAPSHOT_REQ_VERSION);
    if (missed_versions != nullptr) {
        DCHECK(!missed_versions->empty());
        request.__isset.missing_version = true;
        for (auto& version : *missed_versions) {
            // NOTE: assume missing version composed of singleton delta.
            DCHECK_EQ(version.first, version.second);
            request.missing_version.push_back(version.first);
        }
    }
    if (missing_version_ranges != nullptr) {
        DCHECK(!missing_version_ranges->empty());
        request.__isset.missing_version_ranges = true;
        for (auto v : *missing_version_ranges) {
            request.missing_version_ranges.push_back(v);
        }
    }
    if (version > 0) {
        request.__set_version(version);
    }
    if (timeout_s > 0) {
        request.__set_timeout(timeout_s);
    }

    TAgentResult result;
    // snapshot will hard link all required rowsets' segment files, the number of files may be very large(>1000),
    // so it may take some time to process this rpc, so we increase rpc timeout from 5s to 20s to reduce the chance
    // of timeout for now, we may need a smart way to estimate the time of make_snapshot in future
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<BackendServiceClient>(
            ip, port, [&request, &result](BackendServiceConnection& client) { client->make_snapshot(result, request); },
            config::make_snapshot_rpc_timeout_ms));
    if (result.status.status_code != TStatusCode::OK) {
        return {result.status};
    }

    if (result.__isset.snapshot_path) {
        *snapshot_path = result.snapshot_path;
        if (snapshot_path->at(snapshot_path->length() - 1) != '/') {
            snapshot_path->append("/");
        }
    } else {
        return Status::InternalError("success snapshot without snapshot path");
    }

    if (result.snapshot_format != g_Types_constants.TSNAPSHOT_REQ_VERSION2) {
        LOG(WARNING) << "Unsupported snapshot format version: " << result.snapshot_format << ", from: " << ip
                     << ", tablet: " << tablet_id;
        return Status::NotSupported("Unsupported snapshot format version");
    }
    return Status::OK();
}

Status EngineReplicationTask::_release_snapshot(const std::string& ip, int port, const std::string& snapshot_path) {
    bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
    if (bg_worker_stopped) {
        return Status::InternalError("Process is going to quit. The snapshot will stop.");
    }

    TAgentResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<BackendServiceClient>(
            ip, port, [&snapshot_path, &result](BackendServiceConnection& client) {
                client->release_snapshot(result, snapshot_path);
            }));
    return {result.status};
}

Status EngineReplicationTask::_download_files(DataDir* data_dir, const std::string& remote_url_prefix,
                                              const std::string& local_path) {
    bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
    if (bg_worker_stopped) {
        return Status::InternalError("Process is going to quit. The download will stop.");
    }

    // Check local path exist, if exist, remove it, then create the dir
    // local_file_full_path = tabletid/clone. for a specific tablet, there should be only one folder
    // if this folder exists, then should remove it
    // for example, BE clone from BE 1 to download file 1 with version (2,2), but clone from BE 1 failed
    // then it will try to clone from BE 2, but it will find the file 1 already exist, but file 1 with same
    // name may have different versions.
    RETURN_IF_ERROR(fs::remove_all(local_path));
    RETURN_IF_ERROR(fs::create_directories(local_path));

    // Get remote dir file list
    string file_list_str;
    auto list_files_cb = [&remote_url_prefix, &file_list_str](HttpClient* client) {
        RETURN_IF_ERROR(client->init(remote_url_prefix));
        client->set_timeout_ms(LIST_REMOTE_FILE_TIMEOUT * 1000);
        RETURN_IF_ERROR(client->execute(&file_list_str));
        return Status::OK();
    };
    RETURN_IF_ERROR(HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, list_files_cb));

    // Parse file name and size
    const char* const FILE_DELIMETER_IN_DIR_RESPONSE = "\n";
    const char* const FILE_NAME_SIZE_DELIMETER = "|";
    std::vector<string> file_name_list;
    std::vector<int64_t> file_size_list;

    bool use_file_name_and_size_format = file_list_str.find(FILE_NAME_SIZE_DELIMETER) != string::npos;
    if (use_file_name_and_size_format) {
        for (auto file_str : strings::Split(file_list_str, FILE_DELIMETER_IN_DIR_RESPONSE, strings::SkipWhitespace())) {
            std::vector<string> list = strings::Split(file_str, FILE_NAME_SIZE_DELIMETER);
            if (list.size() != 2) {
                return Status::InternalError(fmt::format("invalid directory entry {}", file_str.as_string()));
            }

            StringParser::ParseResult result;
            std::string& file_size_str = list[1];
            auto file_size = StringParser::string_to_int<int64_t>(file_size_str.data(), file_size_str.size(), &result);
            if (result != StringParser::PARSE_SUCCESS || file_size < 0) {
                return Status::InternalError("wrong file size.");
            }

            file_name_list.emplace_back(std::move(list[0]));
            file_size_list.emplace_back(file_size);
        }
    } else {
        file_name_list = strings::Split(file_list_str, FILE_DELIMETER_IN_DIR_RESPONSE, strings::SkipWhitespace());
    }

    // If the header file is not exist, the table could't loaded by olap engine.
    // Avoid of data is not complete, we copy the header file at last.
    // The header file's name is end of .hdr.
    for (int i = 0; i < file_name_list.size() - 1; ++i) {
        bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
        if (bg_worker_stopped) {
            return Status::InternalError("Process is going to quit. The download will stop.");
        }
        StringPiece sp(file_name_list[i]);
        if (sp.ends_with(".hdr")) {
            std::swap(file_name_list[i], file_name_list[file_name_list.size() - 1]);
            if (use_file_name_and_size_format) {
                std::swap(file_size_list[i], file_size_list[file_size_list.size() - 1]);
            }
            break;
        }
    }

    // Get copy from remote
    uint64_t total_file_size = 0;
    MonotonicStopWatch watch;
    watch.start();
    for (int i = 0; i < file_name_list.size(); ++i) {
        std::string& file_name = file_name_list[i];
        auto remote_file_url = remote_url_prefix + file_name;

        uint64_t file_size = 0;
        if (use_file_name_and_size_format) {
            file_size = file_size_list[i];
        } else {
            auto get_file_size_cb = [&remote_file_url, &file_size](HttpClient* client) {
                RETURN_IF_ERROR(client->init(remote_file_url));
                client->set_timeout_ms(GET_LENGTH_TIMEOUT * 1000);
                RETURN_IF_ERROR(client->head());
                file_size = client->get_content_length();
                return Status::OK();
            };
            RETURN_IF_ERROR(HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, get_file_size_cb));
        }

        // check disk capacity
        if (data_dir->capacity_limit_reached(file_size)) {
            return Status::InternalError("Disk reach capacity limit");
        }

        total_file_size += file_size;
        uint64_t estimate_timeout = file_size / config::download_low_speed_limit_kbps / 1024;
        if (estimate_timeout < config::download_low_speed_time) {
            estimate_timeout = config::download_low_speed_time;
        }

        std::string local_file_path = local_path + file_name;

        VLOG(1) << "Downloading " << remote_file_url << " to " << local_path << ". bytes=" << file_size
                << " timeout=" << estimate_timeout;

        auto download_cb = [&remote_file_url, estimate_timeout, &local_file_path, file_size](HttpClient* client) {
            RETURN_IF_ERROR(client->init(remote_file_url));
            client->set_timeout_ms(estimate_timeout * 1000);
            RETURN_IF_ERROR(client->download(local_file_path));

            // Check file length
            uint64_t local_file_size = std::filesystem::file_size(local_file_path);
            if (local_file_size != file_size) {
                LOG(WARNING) << "Fail to download " << remote_file_url << ". file_size=" << local_file_size << "/"
                             << file_size;
                return Status::InternalError("mismatched file size");
            }
            chmod(local_file_path.c_str(), S_IRUSR | S_IWUSR);
            return Status::OK();
        };
        RETURN_IF_ERROR(HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, download_cb));
    } // Clone files from remote backend

    uint64_t total_time_ms = watch.elapsed_time() / 1000 / 1000;
    total_time_ms = total_time_ms > 0 ? total_time_ms : 0;
    double copy_rate = 0.0;
    if (total_time_ms > 0) {
        copy_rate = total_file_size / ((double)total_time_ms) / 1000;
    }
    LOG(INFO) << "Copied tablet " << _signature << " files=" << file_name_list.size() << ". bytes=" << total_file_size
              << " cost=" << total_time_ms << " ms"
              << " rate=" << copy_rate << " MB/s";
    return Status::OK();
}

Status EngineReplicationTask::_finish_clone(Tablet* tablet, const string& clone_dir, int64_t snapshot_version,
                                            bool incremental_clone) {
    bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
    if (bg_worker_stopped) {
        return Status::InternalError("Process is going to quit. The clone will stop.");
    }

    if (tablet->updates() != nullptr) {
        return _finish_clone_primary(tablet, clone_dir);
    }
    Status res;
    std::vector<std::string> linked_success_files;

    // clone and compaction operation should be performed sequentially
    tablet->obtain_base_compaction_lock();
    DeferOp base_compaction_lock_release_guard([&tablet]() { tablet->release_base_compaction_lock(); });

    tablet->obtain_cumulative_lock();
    DeferOp cumulative_lock_release_guard([&tablet]() { tablet->release_cumulative_lock(); });

    tablet->obtain_push_lock();
    DeferOp push_lock_release_guard([&tablet]() { tablet->release_push_lock(); });

    tablet->obtain_header_wrlock();
    DeferOp header_wrlock_release_guard([&tablet]() { tablet->release_header_lock(); });

    do {
        // load src header
        std::string header_file = strings::Substitute("$0/$1.hdr", clone_dir, tablet->tablet_id());
        std::string dcgs_snapshot_file = strings::Substitute("$0/$1.dcgs_snapshot", clone_dir, tablet->tablet_id());
        TabletMeta cloned_tablet_meta;
        res = cloned_tablet_meta.create_from_file(header_file);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to load load tablet meta from " << header_file;
            break;
        }

        DeltaColumnGroupSnapshotPB dcg_snapshot_pb;
        bool has_dcgs_snapshot_file = fs::path_exist(dcgs_snapshot_file);
        if (has_dcgs_snapshot_file) {
            res = DeltaColumnGroupListHelper::parse_snapshot(dcgs_snapshot_file, dcg_snapshot_pb);
            if (!res.ok()) {
                LOG(WARNING) << "Fail to load load dcg snapshot from " << dcgs_snapshot_file;
                break;
            }
        }

        // remove the cloned meta file
        (void)fs::remove(header_file);
        // remove the cloned dcgs snapshot file
        if (has_dcgs_snapshot_file) {
            (void)fs::remove(dcgs_snapshot_file);
        }

        std::set<std::string> clone_files;
        res = fs::list_dirs_files(clone_dir, nullptr, &clone_files);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to list directory " << clone_dir << ": " << res;
            break;
        }

        std::set<string> local_files;
        std::string tablet_dir = tablet->schema_hash_path();
        res = fs::list_dirs_files(tablet_dir, nullptr, &local_files);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to list tablet directory " << tablet_dir << ": " << res;
            break;
        }

        // link files from clone dir, if file exists, skip it
        for (const string& clone_file : clone_files) {
            bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
            if (bg_worker_stopped) {
                return Status::InternalError("Process is going to quit. The clone will stop.");
            }

            if (local_files.find(clone_file) != local_files.end()) {
                VLOG(3) << "find same file when clone, skip it. "
                        << "tablet=" << tablet->full_name() << ", clone_file=" << clone_file;
                continue;
            }

            std::string from = strings::Substitute("$0/$1", clone_dir, clone_file);
            std::string to = strings::Substitute("$0/$1", tablet_dir, clone_file);
            res = FileSystem::Default()->link_file(from, to);
            if (!res.ok()) {
                LOG(WARNING) << "Fail to link " << from << " to " << to << ": " << res;
                break;
            }
            linked_success_files.emplace_back(std::move(to));
        }
        if (!res.ok()) {
            break;
        }
        LOG(INFO) << "Linked " << clone_files.size() << " files from " << clone_dir << " to " << tablet_dir;

        std::vector<RowsetMetaSharedPtr> rs_to_clone;
        if (incremental_clone) {
            res = _clone_incremental_data(tablet, cloned_tablet_meta, snapshot_version);
        } else {
            res = _clone_full_data(tablet, const_cast<TabletMeta*>(&cloned_tablet_meta), rs_to_clone);
        }

        // if full clone success, need to update cumulative layer point
        if (!incremental_clone && res.ok()) {
            tablet->set_cumulative_layer_point(-1);
        }

        // recover dcg meta
        if (has_dcgs_snapshot_file && rs_to_clone.size() != 0) {
            auto data_dir = tablet->data_dir();
            rocksdb::WriteBatch wb;
            for (const auto& rs_meta : rs_to_clone) {
                int idx = 0;
                for (const auto& rowset_id : dcg_snapshot_pb.rowset_id()) {
                    if (rowset_id != rs_meta->rowset_id().to_string()) {
                        ++idx;
                        continue;
                    }
                    // dcgs for each segment
                    auto& dcg_list_pb = dcg_snapshot_pb.dcg_lists(idx);
                    DeltaColumnGroupList dcgs;
                    RETURN_IF_ERROR(
                            DeltaColumnGroupListSerializer::deserialize_delta_column_group_list(dcg_list_pb, &dcgs));

                    if (dcgs.size() == 0) {
                        continue;
                    }

                    RETURN_IF_ERROR(TabletMetaManager::put_delta_column_group(
                            data_dir, &wb, dcg_snapshot_pb.tablet_id(idx), dcg_snapshot_pb.rowset_id(idx),
                            dcg_snapshot_pb.segment_id(idx), dcgs));
                    ++idx;
                }
            }
            res = data_dir->get_meta()->write_batch(&wb);
            if (!res.ok()) {
                std::stringstream ss;
                ss << "save dcgs meta failed, tablet id: " << tablet->tablet_id();
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
        }
    } while (false);

    // clear linked files if errors happen
    if (!res.ok()) {
        (void)fs::remove(linked_success_files);
    }

    return res;
}

Status EngineReplicationTask::_clone_incremental_data(Tablet* tablet, const TabletMeta& cloned_tablet_meta,
                                                      int64_t snapshot_version) {
    bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
    if (bg_worker_stopped) {
        return Status::InternalError("Process is going to quit. The clone should will stop.");
    }

    LOG(INFO) << "begin to incremental clone. tablet=" << tablet->full_name()
              << ", snapshot_version=" << snapshot_version;

    std::vector<Version> missed_versions;
    tablet->calc_missed_versions_unlocked(snapshot_version, &missed_versions);

    std::vector<Version> versions_to_delete;
    std::vector<RowsetMetaSharedPtr> rowsets_to_clone;

    VLOG(3) << "get missed versions again when finish incremental clone. "
            << "tablet=" << tablet->full_name() << ", snapshot_version=" << snapshot_version
            << ", missed_versions_size=" << missed_versions.size();

    // check missing versions exist in clone src
    for (Version version : missed_versions) {
        RowsetMetaSharedPtr inc_rs_meta = cloned_tablet_meta.acquire_inc_rs_meta_by_version(version);
        if (inc_rs_meta == nullptr) {
            LOG(WARNING) << "missed version is not found in cloned tablet meta."
                         << ", missed_version=" << version.first << "-" << version.second;
            return Status::NotFound(strings::Substitute("version not found"));
        }

        rowsets_to_clone.push_back(inc_rs_meta);
    }

    // clone_data to tablet
    Status st = tablet->revise_tablet_meta(rowsets_to_clone, versions_to_delete);
    LOG(INFO) << "finish to incremental clone. [tablet=" << tablet->full_name() << " status=" << st << "]";
    return st;
}

Status EngineReplicationTask::_clone_full_data(Tablet* tablet, TabletMeta* cloned_tablet_meta,
                                               std::vector<RowsetMetaSharedPtr>& rs_to_clone) {
    bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
    if (bg_worker_stopped) {
        return Status::InternalError("Process is going to quit. The clone will stop.");
    }

    Version cloned_max_version = cloned_tablet_meta->max_version();
    LOG(INFO) << "begin to full clone. tablet=" << tablet->full_name()
              << ", cloned_max_version=" << cloned_max_version.first << "-" << cloned_max_version.second;
    std::vector<Version> versions_to_delete;
    std::vector<RowsetMetaSharedPtr> rs_metas_found_in_src;
    // check local versions
    for (auto& rs_meta : tablet->tablet_meta()->all_rs_metas()) {
        Version local_version(rs_meta->start_version(), rs_meta->end_version());
        LOG(INFO) << "check local delta when full clone."
                  << "tablet=" << tablet->full_name() << ", local_version=" << local_version.first << "-"
                  << local_version.second;

        // if local version cross src latest, clone failed
        // if local version is : 0-0, 1-1, 2-10, 12-14, 15-15,16-16
        // cloned max version is 13-13, this clone is failed, because could not
        // fill local data by using cloned data.
        // It should not happen because if there is a hole, the following delta will not
        // do compaction.
        if (local_version.first <= cloned_max_version.second && local_version.second > cloned_max_version.second) {
            LOG(WARNING) << "stop to full clone, version cross src latest."
                         << "tablet=" << tablet->full_name() << ", local_version=" << local_version.first << "-"
                         << local_version.second;
            return Status::InternalError("clone version conflict with local version");

        } else if (local_version.second <= cloned_max_version.second) {
            // if local version smaller than src, check if existed in src, will not clone it
            bool existed_in_src = false;

            // if delta labeled with local_version is same with the specified version in clone header,
            // there is no necessity to clone it.
            for (auto& rs_meta : cloned_tablet_meta->all_rs_metas()) {
                if (rs_meta->version().first == local_version.first &&
                    rs_meta->version().second == local_version.second) {
                    existed_in_src = true;
                    break;
                }
            }

            if (existed_in_src) {
                cloned_tablet_meta->delete_rs_meta_by_version(local_version, &rs_metas_found_in_src);
                LOG(INFO) << "Delta has already existed in local header, no need to clone."
                          << "tablet=" << tablet->full_name() << ", version='" << local_version.first << "-"
                          << local_version.second;
            } else {
                // Delta labeled in local_version is not existed in clone header,
                // some overlapping delta will be cloned to replace it.
                // And also, the specified delta should deleted from local header.
                versions_to_delete.push_back(local_version);
                LOG(INFO) << "Delete delta not included by the clone header, should delete it from "
                             "local header."
                          << "tablet=" << tablet->full_name() << ","
                          << ", version=" << local_version.first << "-" << local_version.second;
            }
        }
    }
    std::vector<RowsetMetaSharedPtr> rowsets_to_clone;
    for (auto& rs_meta : cloned_tablet_meta->all_rs_metas()) {
        rowsets_to_clone.push_back(rs_meta);
        LOG(INFO) << "Delta to clone."
                  << "tablet=" << tablet->full_name() << ", version=" << rs_meta->version().first << "-"
                  << rs_meta->version().second;
    }
    rs_to_clone = rowsets_to_clone;

    // clone_data to tablet
    Status st = tablet->revise_tablet_meta(rowsets_to_clone, versions_to_delete);
    LOG(INFO) << "finish to full clone. tablet=" << tablet->full_name() << ", res=" << st;
    // in previous step, copy all files from CLONE_DIR to tablet dir
    // but some rowset is useless, so that remove them here
    for (auto& rs_meta_ptr : rs_metas_found_in_src) {
        RowsetSharedPtr rowset_to_remove;
        if (auto s = RowsetFactory::create_rowset(cloned_tablet_meta->tablet_schema_ptr(), tablet->schema_hash_path(),
                                                  rs_meta_ptr, &rowset_to_remove);
            !s.ok()) {
            LOG(WARNING) << "failed to init rowset to remove: " << rs_meta_ptr->rowset_id().to_string();
            continue;
        }
        if (auto ost = rowset_to_remove->remove(); !ost.ok()) {
            LOG(WARNING) << "failed to remove rowset " << rs_meta_ptr->rowset_id().to_string() << ", res=" << ost;
        }
    }
    return st;
}

Status EngineReplicationTask::_finish_clone_primary(Tablet* tablet, const std::string& clone_dir) {
    bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
    if (bg_worker_stopped) {
        return Status::InternalError("Process is going to quit. The snapshot will stop.");
    }

    auto meta_file = strings::Substitute("$0/meta", clone_dir);
    auto res = SnapshotManager::instance()->parse_snapshot_meta(meta_file);
    if (!res.ok()) {
        return res.status();
    }
    auto snapshot_meta = std::move(res).value();

    RETURN_IF_ERROR(SnapshotManager::instance()->assign_new_rowset_id(&snapshot_meta, clone_dir));

    // check all files in /clone and /tablet
    std::set<std::string> clone_files;
    RETURN_IF_ERROR(fs::list_dirs_files(clone_dir, nullptr, &clone_files));
    clone_files.erase("meta");

    std::set<std::string> local_files;
    const std::string& tablet_dir = tablet->schema_hash_path();
    RETURN_IF_ERROR(fs::list_dirs_files(tablet_dir, nullptr, &local_files));

    // Files that are found in both |clone_files| and |local_files|.
    std::vector<std::string> duplicate_files;
    std::set_intersection(clone_files.begin(), clone_files.end(), local_files.begin(), local_files.end(),
                          std::back_inserter(duplicate_files));
    for (const auto& fname : duplicate_files) {
        ASSIGN_OR_RETURN(auto md5sum1, fs::md5sum(clone_dir + "/" + fname));
        ASSIGN_OR_RETURN(auto md5sum2, fs::md5sum(tablet_dir + "/" + fname));
        if (md5sum1 != md5sum2) {
            LOG(WARNING) << "duplicated file `" << fname << "` with different md5sum";
            return Status::InternalError("duplicate file with different md5");
        }
        clone_files.erase(fname);
        local_files.erase(fname);
    }

    auto fs = FileSystem::Default();
    std::set<std::string> tablet_files;
    for (const std::string& filename : clone_files) {
        std::string from = clone_dir + "/" + filename;
        std::string to = tablet_dir + "/" + filename;
        tablet_files.insert(to);
        RETURN_IF_ERROR(fs->link_file(from, to));
    }
    LOG(INFO) << "Linked " << clone_files.size() << " files from " << clone_dir << " to " << tablet_dir;
    // Note that |snapshot_meta| may be modified by `load_snapshot`.
    Status st = tablet->updates()->load_snapshot(snapshot_meta);
    if (!st.ok()) {
        Status clear_st;
        for (const std::string& filename : tablet_files) {
            clear_st = fs::delete_file(filename);
            if (!st.ok()) {
                LOG(WARNING) << "remove tablet file:" << filename << " failed, status:" << clear_st;
            }
        }
    }

    int64_t expired_stale_sweep_endtime = UnixSeconds() - config::tablet_rowset_stale_sweep_time_sec;
    tablet->updates()->remove_expired_versions(expired_stale_sweep_endtime);
    LOG(INFO) << "Loaded snapshot of tablet " << tablet->tablet_id() << ", removing directory " << clone_dir;
    st = fs::remove_all(clone_dir);
    LOG_IF(WARNING, !st.ok()) << "Fail to remove clone directory " << clone_dir << ": " << st;
    return st;
}

} // namespace starrocks
