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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/snapshot_loader.cpp

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

#include "runtime/snapshot_loader.h"

#include <cstdint>
#include <filesystem>
#include <set>

#include "agent/master_info.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "fs/fs_broker.h"
#include "fs/fs_util.h"
#include "gen_cpp/FileBrokerService_types.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/TFileBrokerService.h"
#include "runtime/broker_mgr.h"
#include "runtime/exec_env.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

#ifdef BE_TEST
inline BrokerServiceClientCache* client_cache(ExecEnv* env) {
    static BrokerServiceClientCache s_client_cache;
    return &s_client_cache;
}

inline const std::string& client_id(ExecEnv* env, const TNetworkAddress& addr) {
    static std::string s_client_id = "starrocks_unit_test";
    return s_client_id;
}
#else
inline BrokerServiceClientCache* client_cache(ExecEnv* env) {
    return env->broker_client_cache();
}

inline const std::string& client_id(ExecEnv* env, const TNetworkAddress& addr) {
    return env->broker_mgr()->get_client_id(addr);
}
#endif

SnapshotLoader::SnapshotLoader(ExecEnv* env, int64_t job_id, int64_t task_id)
        : _env(env), _job_id(job_id), _task_id(task_id) {}

Status SnapshotLoader::upload(const std::map<std::string, std::string>& src_to_dest_path, const TUploadReq& upload,
                              std::map<int64_t, std::vector<std::string>>* tablet_files) {
    if (!upload.__isset.use_broker || upload.use_broker) {
        LOG(INFO) << "begin to upload snapshot files. num: " << src_to_dest_path.size()
                  << ", broker addr: " << upload.broker_addr << ", job: " << _job_id << ", task" << _task_id;
    } else {
        LOG(INFO) << "begin to upload snapshot files. num: " << src_to_dest_path.size() << ", job: " << _job_id
                  << ", task" << _task_id;
    }

    // check if job has already been cancelled
    int tmp_counter = 1;
    RETURN_IF_ERROR(_report_every(0, &tmp_counter, 0, 0, TTaskType::type::UPLOAD));

    Status status = Status::OK();
    // 1. validate local tablet snapshot paths
    RETURN_IF_ERROR(_check_local_snapshot_paths(src_to_dest_path, true));

    // 2. get broker client
    std::unique_ptr<BrokerServiceConnection> client;
    std::unique_ptr<FileSystem> fs;
    if (!upload.__isset.use_broker || upload.use_broker) {
        client = std::make_unique<BrokerServiceConnection>(client_cache(_env), upload.broker_addr,
                                                           config::broker_write_timeout_seconds * 1000, &status);
        if (!status.ok()) {
            std::stringstream ss;
            ss << "failed to get broker client. "
               << "broker addr: " << upload.broker_addr << ". msg: " << status.get_error_msg();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
    } else {
        std::string random_dest_path = src_to_dest_path.begin()->second;
        auto maybe_fs = FileSystem::CreateUniqueFromString(random_dest_path, FSOptions(&upload));
        if (!maybe_fs.ok()) {
            return Status::InternalError("fail to create file system");
        }
        fs = std::move(maybe_fs.value());
    }

    // 3. for each src path, upload it to remote storage
    // we report to frontend for every 10 files, and we will cancel the job if
    // the job has already been cancelled in frontend.
    int report_counter = 0;
    int total_num = src_to_dest_path.size();
    int finished_num = 0;
    for (const auto& iter : src_to_dest_path) {
        const std::string& src_path = iter.first;
        const std::string& dest_path = iter.second;

        int64_t tablet_id = 0;
        int32_t schema_hash = 0;
        RETURN_IF_ERROR(_get_tablet_id_and_schema_hash_from_file_path(src_path, &tablet_id, &schema_hash));

        // 2.1 get existing files from remote path
        std::map<std::string, FileStat> remote_files;
        if (!upload.__isset.use_broker || upload.use_broker) {
            RETURN_IF_ERROR(_get_existing_files_from_remote(*client, dest_path, upload.broker_prop, &remote_files));
        } else {
            RETURN_IF_ERROR(_get_existing_files_from_remote_without_broker(fs, dest_path, &remote_files));
        }
        for (auto& tmp : remote_files) {
            VLOG(2) << "get remote file: " << tmp.first << ", checksum: " << tmp.second.md5;
        }

        // 2.2 list local files
        std::vector<std::string> local_files;
        std::vector<std::string> local_files_with_checksum;
        RETURN_IF_ERROR(_get_existing_files_from_local(src_path, &local_files));

        // 2.3 iterate local files
        for (auto& local_file : local_files) {
            RETURN_IF_ERROR(_report_every(10, &report_counter, finished_num, total_num, TTaskType::type::UPLOAD));

            // calc md5sum of localfile
            ASSIGN_OR_RETURN(auto md5sum, fs::md5sum(src_path + "/" + local_file));
            VLOG(2) << "get file checksum: " << local_file << ": " << md5sum;
            local_files_with_checksum.push_back(local_file + "." + md5sum);

            // check if this local file need upload
            bool need_upload = false;
            auto find = remote_files.find(local_file);
            if (find != remote_files.end()) {
                if (md5sum != find->second.md5) {
                    // remote storage file exist, but with different checksum
                    LOG(WARNING) << "remote file checksum is invalid. remote: " << find->first << ", local: " << md5sum;
                    // TODO(cmy): save these files and delete them later
                    need_upload = true;
                }
            } else {
                need_upload = true;
            }

            if (!need_upload) {
                VLOG(2) << "file exist in remote path, no need to upload: " << local_file;
                continue;
            }

            // upload
            // open broker writer. file name end with ".part"
            // it will be renamed to ".md5sum" after upload finished
            auto full_remote_file = dest_path + "/" + local_file;
            auto tmp_broker_file_name = full_remote_file + ".part";
            auto local_file_path = src_path + "/" + local_file;
            std::unique_ptr<WritableFile> remote_writable_file;
            WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
            if (!upload.__isset.use_broker || upload.use_broker) {
                BrokerFileSystem fs_broker(upload.broker_addr, upload.broker_prop);
                ASSIGN_OR_RETURN(remote_writable_file, fs_broker.new_writable_file(opts, tmp_broker_file_name));
            } else {
                ASSIGN_OR_RETURN(remote_writable_file, fs->new_writable_file(opts, tmp_broker_file_name));
            }
            ASSIGN_OR_RETURN(auto input_file, FileSystem::Default()->new_sequential_file(local_file_path));
            auto res = fs::copy(input_file.get(), remote_writable_file.get(), 1024 * 1024);
            if (!res.ok()) {
                return res.status();
            }
            LOG(INFO) << "finished to write file via broker. file: " << local_file_path << ", length: " << *res;
            RETURN_IF_ERROR(remote_writable_file->close());
            // rename file to end with ".md5sum"
            if (!upload.__isset.use_broker || upload.use_broker) {
                RETURN_IF_ERROR(_rename_remote_file(*client, full_remote_file + ".part",
                                                    full_remote_file + "." + md5sum, upload.broker_prop));
            } else {
                RETURN_IF_ERROR(_rename_remote_file_without_broker(fs, full_remote_file + ".part",
                                                                   full_remote_file + "." + md5sum));
            }
        } // end for each tablet's local files

        tablet_files->emplace(tablet_id, local_files_with_checksum);
        finished_num++;
        LOG(INFO) << "finished to write tablet to remote. local path: " << src_path << ", remote path: " << dest_path;
    } // end for each tablet path

    LOG(INFO) << "finished to upload snapshots. job: " << _job_id << ", task id: " << _task_id;
    return status;
}

/*
 * Download snapshot files from remote.
 * After downloaded, the local dir should contains all files existing in remote,
 * may also contains severval useless files.
 */
Status SnapshotLoader::download(const std::map<std::string, std::string>& src_to_dest_path,
                                const TDownloadReq& download, std::vector<int64_t>* downloaded_tablet_ids) {
    if (!download.__isset.use_broker || download.use_broker) {
        LOG(INFO) << "begin to download snapshot files. num: " << src_to_dest_path.size()
                  << ", broker addr: " << download.broker_addr << ", job: " << _job_id << ", task id: " << _task_id;
    } else {
        LOG(INFO) << "begin to download snapshot files. num: " << src_to_dest_path.size() << ", job: " << _job_id
                  << ", task id: " << _task_id;
    }

    // check if job has already been cancelled
    int tmp_counter = 1;
    RETURN_IF_ERROR(_report_every(0, &tmp_counter, 0, 0, TTaskType::type::DOWNLOAD));

    Status status = Status::OK();
    // 1. validate local tablet snapshot paths
    RETURN_IF_ERROR(_check_local_snapshot_paths(src_to_dest_path, false));

    // 2. get broker client
    std::unique_ptr<BrokerServiceConnection> client;
    std::unique_ptr<FileSystem> fs;
    std::vector<TNetworkAddress> broker_addrs;
    if (!download.__isset.use_broker || download.use_broker) {
        client = std::make_unique<BrokerServiceConnection>(client_cache(_env), download.broker_addr,
                                                           config::broker_write_timeout_seconds * 1000, &status);
        if (!status.ok()) {
            std::stringstream ss;
            ss << "failed to get broker client. "
               << "broker addr: " << download.broker_addr << ". msg: " << status.get_error_msg();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        broker_addrs.push_back(download.broker_addr);
    } else {
        std::string random_src_path = src_to_dest_path.begin()->first;
        auto maybe_fs = FileSystem::CreateUniqueFromString(random_src_path, FSOptions(&download));
        if (!maybe_fs.ok()) {
            return Status::InternalError("fail to create file system");
        }
        fs = std::move(maybe_fs.value());
    }

    // 3. for each src path, download it to local storage
    int report_counter = 0;
    int total_num = src_to_dest_path.size();
    int finished_num = 0;
    for (const auto& iter : src_to_dest_path) {
        const std::string& remote_path = iter.first;
        const std::string& local_path = iter.second;

        int64_t local_tablet_id = 0;
        int32_t schema_hash = 0;
        RETURN_IF_ERROR(_get_tablet_id_and_schema_hash_from_file_path(local_path, &local_tablet_id, &schema_hash));
        downloaded_tablet_ids->push_back(local_tablet_id);

        int64_t remote_tablet_id = 0;
        RETURN_IF_ERROR(_get_tablet_id_from_remote_path(remote_path, &remote_tablet_id));
        VLOG(2) << "get local tablet id: " << local_tablet_id << ", schema hash: " << schema_hash
                << ", remote tablet id: " << remote_tablet_id;

        // 1. get local files
        std::vector<std::string> local_files;
        RETURN_IF_ERROR(_get_existing_files_from_local(local_path, &local_files));

        // 2. get remote files
        std::map<std::string, FileStat> remote_files;
        if (!download.__isset.use_broker || download.use_broker) {
            RETURN_IF_ERROR(_get_existing_files_from_remote(*client, remote_path, download.broker_prop, &remote_files));
        } else {
            RETURN_IF_ERROR(_get_existing_files_from_remote_without_broker(fs, remote_path, &remote_files));
        }
        if (remote_files.empty()) {
            std::stringstream ss;
            ss << "get nothing from remote path: " << remote_path;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(local_tablet_id);
        if (tablet == nullptr) {
            std::stringstream ss;
            ss << "failed to get local tablet: " << local_tablet_id;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        DataDir* data_dir = tablet->data_dir();

        for (auto& iter : remote_files) {
            RETURN_IF_ERROR(_report_every(10, &report_counter, finished_num, total_num, TTaskType::type::DOWNLOAD));

            bool need_download = false;
            const std::string& remote_file = iter.first;
            const FileStat& file_stat = iter.second;
            auto find = std::find(local_files.begin(), local_files.end(), remote_file);
            if (find == local_files.end()) {
                // remote file does not exist in local, download it
                need_download = true;
            } else {
                if (_end_with(remote_file, ".hdr")) {
                    // this is a header file, download it.
                    need_download = true;
                } else {
                    // check checksum
                    auto local_md5sum = fs::md5sum(local_path + "/" + remote_file);
                    if (!local_md5sum.ok()) {
                        LOG(WARNING) << "failed to get md5sum of local file: " << remote_file
                                     << ". msg: " << local_md5sum.status() << ". download it";
                        need_download = true;
                    } else {
                        VLOG(2) << "get local file checksum: " << remote_file << ": " << *local_md5sum;
                        if (file_stat.md5 != *local_md5sum) {
                            // file's checksum does not equal, download it.
                            need_download = true;
                        }
                    }
                }
            }

            if (!need_download) {
                LOG(INFO) << "remote file already exist in local, no need to download."
                          << ", file: " << remote_file;
                continue;
            }

            // begin to download
            std::string full_remote_file = remote_path + "/" + remote_file + "." + file_stat.md5;
            std::string local_file_name;
            // we need to replace the tablet_id in remote file name with local tablet id
            RETURN_IF_ERROR(_replace_tablet_id(remote_file, local_tablet_id, &local_file_name));
            std::string full_local_file = local_path + "/" + local_file_name;
            LOG(INFO) << "begin to download from " << full_remote_file << " to " << full_local_file;
            size_t file_len = file_stat.size;

            // check disk capacity
            if (data_dir->capacity_limit_reached(file_len)) {
                return Status::InternalError("capacity limit reached");
            }

            std::unique_ptr<SequentialFile> remote_sequential_file;
            if (!download.__isset.use_broker || download.use_broker) {
                BrokerFileSystem fs_broker(download.broker_addr, download.broker_prop);
                ASSIGN_OR_RETURN(remote_sequential_file, fs_broker.new_sequential_file(full_remote_file));
            } else {
                ASSIGN_OR_RETURN(remote_sequential_file, fs->new_sequential_file(full_remote_file));
            }
            // remove file which will be downloaded now.
            // this file will be added to local_files if it be downloaded successfully.
            // The Restore process of Primary key tablet may get a empty local_files at the begining.
            // Because we just generate the download path but not any other file.
            if (!local_files.empty()) {
                local_files.erase(find);
            }

            // 3. open local file for write
            WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
            ASSIGN_OR_RETURN(auto local_file, FileSystem::Default()->new_writable_file(opts, full_local_file));

            auto res = fs::copy(remote_sequential_file.get(), local_file.get(), 1024 * 1024);
            if (!res.ok()) {
                return res.status();
            }
            RETURN_IF_ERROR(local_file->close());

            // 5. check md5 of the downloaded file
            ASSIGN_OR_RETURN(auto downloaded_md5sum, fs::md5sum(full_local_file));
            VLOG(2) << "get downloaded file checksum: " << full_local_file << ": " << downloaded_md5sum;
            if (downloaded_md5sum != file_stat.md5) {
                std::stringstream ss;
                ss << "invalid md5 of downloaded file: " << full_local_file << ", expected: " << file_stat.md5
                   << ", get: " << downloaded_md5sum;
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }

            // local_files always keep the updated local files
            local_files.push_back(local_file_name);
            LOG(INFO) << "finished to download file via broker. file: " << full_local_file << ", length: " << file_len;
        } // end for all remote files

        // finally, delete local files which are not in remote
        for (const auto& local_file : local_files) {
            // replace the tablet id in local file name with the remote tablet id,
            // in order to compare the file name.
            std::string new_name;
            Status st = _replace_tablet_id(local_file, remote_tablet_id, &new_name);
            if (!st.ok()) {
                LOG(WARNING) << "failed to replace tablet id. unknown local file: " << st.get_error_msg()
                             << ". ignore it";
                continue;
            }
            VLOG(2) << "new file name after replace tablet id: " << new_name;
            const auto& find = remote_files.find(new_name);
            if (find != remote_files.end()) {
                continue;
            }

            // delete
            std::string full_local_file = local_path + "/" + local_file;
            VLOG(2) << "begin to delete local snapshot file: " << full_local_file << ", it does not exist in remote";
            if (remove(full_local_file.c_str()) != 0) {
                LOG(WARNING) << "failed to delete unknown local file: " << full_local_file << ", ignore it";
            }
        }

        finished_num++;
    } // end for src_to_dest_path

    LOG(INFO) << "finished to download snapshots. job: " << _job_id << ", task id: " << _task_id;
    return status;
}

Status SnapshotLoader::primary_key_move(const std::string& snapshot_path, const TabletSharedPtr& tablet,
                                        bool overwrite) {
    CHECK(tablet->updates() != nullptr);
    std::string tablet_path = tablet->schema_hash_path();
    std::string store_path = tablet->data_dir()->path();
    LOG(INFO) << "begin to move snapshot files. from: " << snapshot_path << ", to: " << tablet_path
              << ", store: " << store_path << ", job: " << _job_id << ", task id: " << _task_id;

    Status status = Status::OK();

    // validate snapshot_path and schema_hash_path
    int64_t snapshot_tablet_id = 0;
    int32_t snapshot_schema_hash = 0;
    RETURN_IF_ERROR(
            _get_tablet_id_and_schema_hash_from_file_path(snapshot_path, &snapshot_tablet_id, &snapshot_schema_hash));

    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    RETURN_IF_ERROR(_get_tablet_id_and_schema_hash_from_file_path(tablet_path, &tablet_id, &schema_hash));

    if (tablet_id != snapshot_tablet_id || schema_hash != snapshot_schema_hash) {
        std::stringstream ss;
        ss << "path does not match. snapshot: " << snapshot_path << ", tablet path: " << tablet_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    DataDir* store = StorageEngine::instance()->get_store(store_path);
    if (store == nullptr) {
        std::stringstream ss;
        ss << "failed to get store by path: " << store_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    std::filesystem::path tablet_dir(tablet_path);
    std::filesystem::path snapshot_dir(snapshot_path);
    if (!std::filesystem::exists(tablet_dir)) {
        std::stringstream ss;
        ss << "tablet path does not exist: " << tablet_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    if (!std::filesystem::exists(snapshot_dir)) {
        std::stringstream ss;
        ss << "snapshot path does not exist: " << snapshot_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    auto meta_file = strings::Substitute("$0/meta", snapshot_path);
    auto res = SnapshotManager::instance()->parse_snapshot_meta(meta_file);
    if (!res.ok()) {
        return res.status();
    }
    auto snapshot_meta = std::move(res).value();

    for (auto& rowset_meta : snapshot_meta.rowset_metas()) {
        rowset_meta.set_tablet_id(tablet_id);
    }
    snapshot_meta.tablet_meta().set_tablet_id(tablet_id);

    RETURN_IF_ERROR(SnapshotManager::instance()->assign_new_rowset_id(&snapshot_meta, snapshot_path));

    if (overwrite) {
        // check all files in /clone and /tablet
        std::set<std::string> snapshot_files;
        RETURN_IF_ERROR(fs::list_dirs_files(snapshot_path, nullptr, &snapshot_files));
        snapshot_files.erase("meta");

        // 1. simply delete the old dir and replace it with the snapshot dir
        try {
            // This remove seems saft enough, because we already get
            // tablet id and schema hash from this path, which
            // means this path is a valid path.
            std::filesystem::remove_all(tablet_dir);
            VLOG(2) << "remove dir: " << tablet_dir;
            std::filesystem::create_directory(tablet_dir);
            VLOG(2) << "re-create dir: " << tablet_dir;
        } catch (const std::filesystem::filesystem_error& e) {
            std::stringstream ss;
            ss << "failed to move tablet path: " << tablet_path << ". err: " << e.what();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // link files one by one
        // files in snapshot dir will be moved in snapshot clean process
        std::vector<std::string> linked_files;
        for (const std::string& file : snapshot_files) {
            std::string full_src_path = snapshot_path + "/" + file;
            std::string full_dest_path = tablet_path + "/" + file;
            if (link(full_src_path.c_str(), full_dest_path.c_str()) != 0) {
                LOG(WARNING) << "failed to link file from " << full_src_path << " to " << full_dest_path
                             << ", err: " << std::strerror(errno);

                // clean the already linked files
                for (auto& linked_file : linked_files) {
                    remove(linked_file.c_str());
                }

                return Status::InternalError("move tablet failed");
            }
            linked_files.push_back(full_dest_path);
            VLOG(2) << "link file from " << full_src_path << " to " << full_dest_path;
        }

    } else {
        LOG(FATAL) << "only support overwrite now";
    }

    RETURN_IF_ERROR(tablet->updates()->load_snapshot(snapshot_meta, true));
    tablet->updates()->remove_expired_versions(time(nullptr));
    LOG(INFO) << "Loaded snapshot of tablet " << tablet->tablet_id() << ", removing directory " << snapshot_path;
    auto st = fs::remove_all(snapshot_path);
    LOG_IF(WARNING, !st.ok()) << "Fail to remove clone directory " << snapshot_path << ": " << st;
    return Status::OK();
}

// move the snapshot files in snapshot_path
// to schema_hash_path
// If overwrite, just replace the schema_hash_path with snapshot_path,
// else: (TODO)
//
// MUST hold tablet's header lock, push lock, cumulative lock and base compaction lock
Status SnapshotLoader::move(const std::string& snapshot_path, const TabletSharedPtr& tablet, bool overwrite) {
    std::string tablet_path = tablet->schema_hash_path();
    std::string store_path = tablet->data_dir()->path();
    LOG(INFO) << "begin to move snapshot files. from: " << snapshot_path << ", to: " << tablet_path
              << ", store: " << store_path << ", job: " << _job_id << ", task id: " << _task_id;

    Status status = Status::OK();

    // validate snapshot_path and schema_hash_path
    int64_t snapshot_tablet_id = 0;
    int32_t snapshot_schema_hash = 0;
    RETURN_IF_ERROR(
            _get_tablet_id_and_schema_hash_from_file_path(snapshot_path, &snapshot_tablet_id, &snapshot_schema_hash));

    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    RETURN_IF_ERROR(_get_tablet_id_and_schema_hash_from_file_path(tablet_path, &tablet_id, &schema_hash));

    if (tablet_id != snapshot_tablet_id || schema_hash != snapshot_schema_hash) {
        std::stringstream ss;
        ss << "path does not match. snapshot: " << snapshot_path << ", tablet path: " << tablet_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    DataDir* store = StorageEngine::instance()->get_store(store_path);
    if (store == nullptr) {
        std::stringstream ss;
        ss << "failed to get store by path: " << store_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    std::filesystem::path tablet_dir(tablet_path);
    std::filesystem::path snapshot_dir(snapshot_path);
    if (!std::filesystem::exists(tablet_dir)) {
        std::stringstream ss;
        ss << "tablet path does not exist: " << tablet_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    if (!std::filesystem::exists(snapshot_dir)) {
        std::stringstream ss;
        ss << "snapshot path does not exist: " << snapshot_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    // rename the rowset ids and tabletid info in rowset meta
    status = SnapshotManager::instance()->convert_rowset_ids(snapshot_path, tablet_id, schema_hash);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to convert rowset id: " << status.to_string() << ". snapshot path: " << snapshot_path
                     << " tablet path: " << tablet_path;
        return status;
    }

    if (overwrite) {
        std::vector<std::string> snapshot_files;
        RETURN_IF_ERROR(_get_existing_files_from_local(snapshot_path, &snapshot_files));

        // 1. simply delete the old dir and replace it with the snapshot dir
        try {
            // This remove seems saft enough, because we already get
            // tablet id and schema hash from this path, which
            // means this path is a valid path.
            std::filesystem::remove_all(tablet_dir);
            VLOG(2) << "remove dir: " << tablet_dir;
            std::filesystem::create_directory(tablet_dir);
            VLOG(2) << "re-create dir: " << tablet_dir;
        } catch (const std::filesystem::filesystem_error& e) {
            std::stringstream ss;
            ss << "failed to move tablet path: " << tablet_path << ". err: " << e.what();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // link files one by one
        // files in snapshot dir will be moved in snapshot clean process
        std::vector<std::string> linked_files;
        for (auto& file : snapshot_files) {
            std::string full_src_path = snapshot_path + "/" + file;
            std::string full_dest_path = tablet_path + "/" + file;
            if (link(full_src_path.c_str(), full_dest_path.c_str()) != 0) {
                LOG(WARNING) << "failed to link file from " << full_src_path << " to " << full_dest_path
                             << ", err: " << std::strerror(errno);

                // clean the already linked files
                for (auto& linked_file : linked_files) {
                    remove(linked_file.c_str());
                }

                return Status::InternalError("move tablet failed");
            }
            linked_files.push_back(full_dest_path);
            VLOG(2) << "link file from " << full_src_path << " to " << full_dest_path;
        }

    } else {
        LOG(FATAL) << "only support overwrite now";
    }

    // snapshot loader not need to change tablet uid
    // fixme: there is no header now and can not call load_one_tablet here
    // reload header
    status = StorageEngine::instance()->tablet_manager()->load_tablet_from_dir(store, tablet_id, schema_hash,
                                                                               tablet_path, true);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to reload header of tablet. tablet_id=" << tablet_id << " err=" << status.to_string();
        return status;
    }
    LOG(INFO) << "finished to reload header of tablet: " << tablet_id;

    return status;
}

bool SnapshotLoader::_end_with(const std::string& str, const std::string& match) {
    if (str.size() >= match.size() && str.compare(str.size() - match.size(), match.size(), match) == 0) {
        return true;
    }
    return false;
}

Status SnapshotLoader::_get_tablet_id_and_schema_hash_from_file_path(const std::string& src_path, int64_t* tablet_id,
                                                                     int32_t* schema_hash) {
    // path should be like: /path/.../tablet_id/schema_hash
    // we try to extract tablet_id from path
    size_t pos = src_path.find_last_of('/');
    if (pos == std::string::npos || pos == src_path.length() - 1) {
        return Status::InternalError("failed to get tablet id from path: " + src_path);
    }

    std::string schema_hash_str = src_path.substr(pos + 1);
    std::stringstream ss1;
    ss1 << schema_hash_str;
    ss1 >> *schema_hash;

    // skip schema hash part
    size_t pos2 = src_path.find_last_of('/', pos - 1);
    if (pos2 == std::string::npos) {
        return Status::InternalError("failed to get tablet id from path: " + src_path);
    }

    std::string tablet_str = src_path.substr(pos2 + 1, pos - pos2);
    std::stringstream ss2;
    ss2 << tablet_str;
    ss2 >> *tablet_id;

    VLOG(2) << "get tablet id " << *tablet_id << ", schema hash: " << *schema_hash << " from path: " << src_path;
    return Status::OK();
}

Status SnapshotLoader::_check_local_snapshot_paths(const std::map<std::string, std::string>& src_to_dest_path,
                                                   bool check_src) {
    for (const auto& pair : src_to_dest_path) {
        std::string path;
        if (check_src) {
            path = pair.first;
        } else {
            path = pair.second;
        }
        ASSIGN_OR_RETURN(auto is_dir, fs::is_directory(path));
        if (!is_dir) {
            std::stringstream ss;
            ss << "snapshot path is not directory: " << path;
            LOG(WARNING) << ss.str();
            return Status::RuntimeError(ss.str());
        }
    }
    LOG(INFO) << "all local snapshot paths are existing. num: " << src_to_dest_path.size();
    return Status::OK();
}

Status SnapshotLoader::_get_existing_files_from_remote(BrokerServiceConnection& client, const std::string& remote_path,
                                                       const std::map<std::string, std::string>& broker_prop,
                                                       std::map<std::string, FileStat>* files) {
    try {
        // get existing files from remote path
        TBrokerListResponse list_rep;
        TBrokerListPathRequest list_req;
        list_req.__set_version(TBrokerVersion::VERSION_ONE);
        list_req.__set_path(remote_path + "/*");
        list_req.__set_isRecursive(false);
        list_req.__set_properties(broker_prop);
        list_req.__set_fileNameOnly(true); // we only need file name, not abs path

        try {
            client->listPath(list_rep, list_req);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->listPath(list_rep, list_req);
        }

        if (list_rep.opStatus.statusCode == TBrokerOperationStatusCode::FILE_NOT_FOUND) {
            LOG(INFO) << "path does not exist: " << remote_path;
            return Status::OK();
        } else if (list_rep.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
            std::stringstream ss;
            ss << "failed to list files from remote path: " << remote_path << ", msg: " << list_rep.opStatus.message;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        LOG(INFO) << "finished to list files from remote path. file num: " << list_rep.files.size();

        // split file name and checksum
        for (const auto& file : list_rep.files) {
            if (file.isDir) {
                // this is not a file
                continue;
            }

            const std::string& file_name = file.path;
            size_t pos = file_name.find_last_of('.');
            if (pos == std::string::npos || pos == file_name.size() - 1) {
                // Not found checksum separator, ignore this file
                continue;
            }

            FileStat stat = {std::string(file_name, 0, pos), std::string(file_name, pos + 1), file.size};
            files->emplace(std::string(file_name, 0, pos), stat);
            VLOG(2) << "split remote file: " << std::string(file_name, 0, pos)
                    << ", checksum: " << std::string(file_name, pos + 1);
        }

        LOG(INFO) << "finished to split files. valid file num: " << files->size();

    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "failed to list files in remote path: " << remote_path << ", msg: " << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    return Status::OK();
}

Status SnapshotLoader::_get_existing_files_from_remote_without_broker(const std::unique_ptr<FileSystem>& fs,
                                                                      const std::string& remote_path,
                                                                      std::map<std::string, FileStat>* files) {
    int64_t file_num = 0;
    Status st;
    st.update(fs->iterate_dir2(remote_path, [&](std::string_view name, const FileMeta& meta) {
        if (UNLIKELY(!meta.has_is_dir())) {
            st.update(Status::InternalError("Unable to recognize the file type"));
            return false;
        }
        file_num++;
        if (meta.is_dir()) {
            return true;
        }
        if (UNLIKELY(!meta.has_size())) {
            st.update(Status::InternalError("Unable to get file size"));
            return false;
        }
        size_t pos = name.find_last_of('.');
        if (pos == std::string::npos || pos == name.size() - 1) {
            // Not found checksum separator, ignore this file
            return true;
        }
        std::string name_part(name.data(), name.data() + pos);
        std::string md5_part(name.data() + pos + 1, name.data() + name.size());
        FileStat stat = {name_part, md5_part, meta.size()};
        files->emplace(name_part, stat);
        VLOG(2) << "split remote file: " << name_part << ", checksum: " << md5_part;
        return true;
    }));

    if (!st.ok() && !st.is_not_found()) {
        LOG(WARNING) << "failed to list files in remote path: " << remote_path << ", msg: " << st;
        return st;
    }

    LOG(INFO) << "finished to split files. total file num: " << file_num << " valid file num: " << files->size();

    return Status::OK();
}

Status SnapshotLoader::_get_existing_files_from_local(const std::string& local_path,
                                                      std::vector<std::string>* local_files) {
    Status status = FileSystem::Default()->get_children(local_path, local_files);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "failed to list files in local path: " << local_path << ", msg: " << status.get_error_msg();
        LOG(WARNING) << ss.str();
        return status;
    }
    LOG(INFO) << "finished to list files in local path: " << local_path << ", file num: " << local_files->size();
    return Status::OK();
}

Status SnapshotLoader::_rename_remote_file(BrokerServiceConnection& client, const std::string& orig_name,
                                           const std::string& new_name,
                                           const std::map<std::string, std::string>& broker_prop) {
    try {
        TBrokerOperationStatus op_status;
        TBrokerRenamePathRequest rename_req;
        rename_req.__set_version(TBrokerVersion::VERSION_ONE);
        rename_req.__set_srcPath(orig_name);
        rename_req.__set_destPath(new_name);
        rename_req.__set_properties(broker_prop);

        try {
            client->renamePath(op_status, rename_req);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->renamePath(op_status, rename_req);
        }

        if (op_status.statusCode != TBrokerOperationStatusCode::OK) {
            std::stringstream ss;
            ss << "Fail to rename file: " << orig_name << " to: " << new_name << " msg:" << op_status.message;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Fail to rename file: " << orig_name << " to: " << new_name << " msg:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    LOG(INFO) << "finished to rename file. orig: " << orig_name << ", new: " << new_name;

    return Status::OK();
}

Status SnapshotLoader::_rename_remote_file_without_broker(const std::unique_ptr<FileSystem>& fs,
                                                          const std::string& orig_name, const std::string& new_name) {
    Status status = fs->rename_file(orig_name, new_name);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "Fail to rename file: " << orig_name << " to: " << new_name << " msg:" << status.message();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    LOG(INFO) << "finished to rename file. orig: " << orig_name << ", new: " << new_name;

    return Status::OK();
}

void SnapshotLoader::_assemble_file_name(const std::string& snapshot_path, const std::string& tablet_path,
                                         int64_t tablet_id, int64_t start_version, int64_t end_version,
                                         int64_t vesion_hash, int32_t seg_num, const std::string& suffix,
                                         std::string* snapshot_file, std::string* tablet_file) {
    std::stringstream ss1;
    ss1 << snapshot_path << "/" << tablet_id << "_" << start_version << "_" << end_version << "_" << vesion_hash << "_"
        << seg_num << suffix;
    *snapshot_file = ss1.str();

    std::stringstream ss2;
    ss2 << tablet_path << "/" << tablet_id << "_" << start_version << "_" << end_version << "_" << vesion_hash << "_"
        << seg_num << suffix;
    *tablet_file = ss2.str();

    VLOG(2) << "assemble file name: " << *snapshot_file << ", " << *tablet_file;
}

Status SnapshotLoader::_replace_tablet_id(const std::string& file_name, int64_t tablet_id, std::string* new_file_name) {
    // eg:
    // 10007.hdr
    // 10007_2_2_0_0.idx
    // 10007_2_2_0_0.dat
    if (_end_with(file_name, ".hdr")) {
        std::stringstream ss;
        ss << tablet_id << ".hdr";
        *new_file_name = ss.str();
        return Status::OK();
    } else if (_end_with(file_name, ".idx") || _end_with(file_name, ".dat") || _end_with(file_name, "meta") ||
               _end_with(file_name, ".del")) {
        *new_file_name = file_name;
        return Status::OK();
    } else {
        return Status::InternalError("invalid tablet file name: " + file_name);
    }
}

Status SnapshotLoader::_get_tablet_id_from_remote_path(const std::string& remote_path, int64_t* tablet_id) {
    // eg:
    // bos://xxx/../__tbl_10004/__part_10003/__idx_10004/__10005
    size_t pos = remote_path.find_last_of('_');
    if (pos == std::string::npos) {
        return Status::InternalError("invalid remove file path: " + remote_path);
    }

    std::string tablet_id_str = remote_path.substr(pos + 1);
    std::stringstream ss;
    ss << tablet_id_str;
    ss >> *tablet_id;

    return Status::OK();
}

// only return CANCELLED if FE return that job is cancelled.
// otherwise, return OK
Status SnapshotLoader::_report_every(int report_threshold, int* counter, int32_t finished_num, int32_t total_num,
                                     TTaskType::type type) {
    ++*counter;
    if (*counter <= report_threshold) {
        return Status::OK();
    }

    LOG(INFO) << "report to frontend. job id: " << _job_id << ", task id: " << _task_id
              << ", finished num: " << finished_num << ", total num:" << total_num;

    TNetworkAddress master_addr = get_master_address();

    TSnapshotLoaderReportRequest request;
    request.job_id = _job_id;
    request.task_id = _task_id;
    request.task_type = type;
    request.__set_finished_num(finished_num);
    request.__set_total_num(total_num);
    TStatus report_st;

    Status rpcStatus = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &report_st](FrontendServiceConnection& client) {
                client->snapshotLoaderReport(report_st, request);
            },
            10000);

    if (!rpcStatus.ok()) {
        // rpc failed, ignore
        return Status::OK();
    }

    // reset
    *counter = 0;
    if (report_st.status_code == TStatusCode::CANCELLED) {
        LOG(INFO) << "job is cancelled. job id: " << _job_id << ", task id: " << _task_id;
        return Status::Cancelled("Cancelled");
    }
    return Status::OK();
}

} // end namespace starrocks
