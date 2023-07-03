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

#include <runtime/lake_snapshot_loader.h>

#include "fs/fs_broker.h"
#include "fs/fs_util.h"
#include "gen_cpp/TFileBrokerService.h"
#include "gen_cpp/lake_service.pb.h"
#include "runtime/snapshot_loader.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet.h"
#include "util/network_util.h"
#include "util/raw_container.h"

namespace starrocks {
LakeSnapshotLoader::LakeSnapshotLoader(ExecEnv* env) : _env(env) {}

Status LakeSnapshotLoader::_get_existing_files_from_remote(BrokerServiceConnection& client,
                                                           const std::string& remote_path,
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

Status LakeSnapshotLoader::_rename_remote_file(BrokerServiceConnection& client, const std::string& orig_name,
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

Status LakeSnapshotLoader::_check_snapshot_paths(const ::starrocks::lake::UploadSnapshotsRequest* request) {
    for (auto& [tablet_id, snapshot] : request->snapshots()) {
        auto tablet = _env->lake_tablet_manager()->get_tablet(tablet_id);
        if (!tablet.ok()) {
            std::stringstream ss;
            ss << "Fail to get tablet " << tablet_id;
            return Status::InternalError(ss.str());
        }
        auto tablet_metadata = tablet->get_metadata(snapshot.version());
        if (!tablet_metadata.ok()) {
            std::stringstream ss;
            ss << "Fail to get tablet metadata, tablet " << tablet_id;
            return Status::InternalError(ss.str());
        }
    }
    return Status::OK();
}

Status LakeSnapshotLoader::upload(const ::starrocks::lake::UploadSnapshotsRequest* request) {
    std::string ip = request->broker().substr(0, request->broker().find(':'));
    int port = std::stoi(request->broker().substr(request->broker().find(':') + 1).c_str());
    TNetworkAddress address = make_network_address(ip, port);
    std::map<string, string> broker_prop(request->broker_properties().begin(), request->broker_properties().end());

    // 1. validate tablet snapshot paths
    RETURN_IF_ERROR(_check_snapshot_paths(request));

    // 2. get broker client
    std::unique_ptr<BrokerServiceConnection> client;
    Status status = Status::OK();
    client = std::make_unique<BrokerServiceConnection>(_env->broker_client_cache(), address, 10000, &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "failed to get broker client. "
           << "broker addr: " << request->broker() << ". msg: " << status.get_error_msg();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    LOG(INFO) << "begin to upload snapshot files. num: " << request->snapshots().size()
              << ", broker addr: " << request->broker();

    for (auto& [tablet_id, snapshot] : request->snapshots()) {
        // TODO: support report logic

        // 2.1 get existing files from remote path
        std::map<std::string, FileStat> remote_files;
        status = _get_existing_files_from_remote(*client, snapshot.dest_path(), broker_prop, &remote_files);
        if (!status.ok()) {
            LOG(ERROR) << "failed to get existing files from remote " << snapshot.dest_path();
            return status;
        }

        for (auto& tmp : remote_files) {
            VLOG(2) << "get remote file: " << tmp.first << ", checksum: " << tmp.second.md5;
        }

        std::map<std::string, std::string> file_locations;
        auto tablet = _env->lake_tablet_manager()->get_tablet(tablet_id);
        auto tablet_metadata = tablet->get_metadata(snapshot.version());
        for (const auto& rowset : (*tablet_metadata)->rowsets()) {
            for (const std::string& segment : rowset.segments()) {
                file_locations[segment] = tablet->segment_location(segment);
            }
        }
        file_locations[starrocks::lake::tablet_metadata_filename(tablet_id, snapshot.version())] =
                tablet->metadata_location(snapshot.version());

        for (auto& [file_name, file] : file_locations) {
            // calc md5sum of file
            ASSIGN_OR_RETURN(auto md5sum, fs::md5sum(file));
            VLOG(2) << "get file checksum: " << file << ": " << md5sum;

            // check if this local file need upload
            bool need_upload = false;
            auto find = remote_files.find(file_name);
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
                VLOG(2) << "file exist in remote path, no need to upload: " << file;
                continue;
            }

            // upload
            // open broker writer. file name end with ".part"
            // it will be renamed to ".md5sum" after upload finished
            auto full_remote_file = snapshot.dest_path() + "/" + file_name;
            auto tmp_broker_file_name = full_remote_file + ".part";
            std::unique_ptr<WritableFile> remote_writable_file;
            WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
            BrokerFileSystem fs_broker(address, broker_prop);
            ASSIGN_OR_RETURN(remote_writable_file, fs_broker.new_writable_file(opts, tmp_broker_file_name));
            ASSIGN_OR_RETURN(auto input_file, fs::new_sequential_file(file));
            auto res = fs::copy(input_file.get(), remote_writable_file.get(), 1024 * 1024);
            if (!res.ok()) {
                return res.status();
            }
            LOG(INFO) << "finished to write file via broker. file: " << file << ", length: " << *res;
            RETURN_IF_ERROR(remote_writable_file->close());
            // rename file to end with ".md5sum"
            RETURN_IF_ERROR(_rename_remote_file(*client, full_remote_file + ".part", full_remote_file + "." + md5sum,
                                                broker_prop));
        }
    }
    return status;
}

Status LakeSnapshotLoader::restore(const ::starrocks::lake::RestoreSnapshotsRequest* request) {
    std::string ip = request->broker().substr(0, request->broker().find(':'));
    int port = std::stoi(request->broker().substr(request->broker().find(':') + 1).c_str());
    TNetworkAddress address = make_network_address(ip, port);
    std::map<string, string> broker_prop(request->broker_properties().begin(), request->broker_properties().end());

    Status status = Status::OK();

    // 1. Get broker client
    std::unique_ptr<BrokerServiceConnection> client;
    std::vector<TNetworkAddress> broker_addrs;
    client = std::make_unique<BrokerServiceConnection>(_env->broker_client_cache(), address, 10000, &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "failed to get broker client. "
           << "broker addr: " << request->broker() << ". msg: " << status.get_error_msg();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    // 2. For each tablet, remove the metadata first and then upload snapshot.
    // we only support overwriting now.
    for (auto& restore_info : request->restore_infos()) {
        // 2.1 Remove the tablet metadata
        RETURN_IF_ERROR(_env->lake_tablet_manager()->delete_tablet(restore_info.tablet_id()));

        // 2.2. Get remote files
        std::map<std::string, FileStat> remote_files;
        RETURN_IF_ERROR(
                _get_existing_files_from_remote(*client, restore_info.snapshot_path(), broker_prop, &remote_files));
        if (remote_files.empty()) {
            std::stringstream ss;
            ss << "get nothing from remote path: " << restore_info.snapshot_path();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // 2.3. Upload the tablet metadata. Metadata need to be uploaded first,
        // otherwise the segment files may be deleted by gc.
        for (auto& iter : remote_files) {
            if (!starrocks::lake::is_tablet_metadata(iter.first)) {
                continue;
            }
            const std::string& remote_file = iter.first;
            const FileStat& file_stat = iter.second;
            std::string full_remote_file = restore_info.snapshot_path() + "/" + remote_file + "." + file_stat.md5;
            std::string read_buf;
            BrokerFileSystem fs_broker(address, broker_prop);
            ASSIGN_OR_RETURN(auto rf, fs_broker.new_random_access_file(full_remote_file));
            ASSIGN_OR_RETURN(auto size, rf->get_size());
            if (UNLIKELY(size > std::numeric_limits<int>::max())) {
                return Status::Corruption("file size exceeded the int range");
            }
            raw::stl_string_resize_uninitialized(&read_buf, size);
            RETURN_IF_ERROR(rf->read_at_fully(0, read_buf.data(), size));
            std::shared_ptr<starrocks::lake::TabletMetadata> meta = std::make_shared<starrocks::lake::TabletMetadata>();
            bool parsed = meta->ParseFromArray(read_buf.data(), static_cast<int>(size));
            if (!parsed) {
                return Status::Corruption(fmt::format("failed to parse tablet meta {}", full_remote_file));
            }
            meta->set_id(restore_info.tablet_id());
            RETURN_IF_ERROR(_env->lake_tablet_manager()->put_tablet_metadata(meta));
        }

        // 2.4. upload the segment files.
        for (auto& iter : remote_files) {
            if (starrocks::lake::is_tablet_metadata(iter.first)) {
                continue;
            }
            const std::string& remote_file = iter.first;
            const FileStat& file_stat = iter.second;
            std::string full_remote_file = restore_info.snapshot_path() + "/" + remote_file + "." + file_stat.md5;
            std::string restored_file =
                    _env->lake_tablet_manager()->segment_location(restore_info.tablet_id(), iter.first);
            std::unique_ptr<WritableFile> remote_writable_file;
            WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
            BrokerFileSystem fs_broker(address, broker_prop);
            ASSIGN_OR_RETURN(auto input_file, fs_broker.new_sequential_file(full_remote_file));
            ASSIGN_OR_RETURN(remote_writable_file, fs::new_writable_file(opts, restored_file));
            auto res = fs::copy(input_file.get(), remote_writable_file.get(), 1024 * 1024);
            if (!res.ok()) {
                return res.status();
            }
            RETURN_IF_ERROR(remote_writable_file->close());
        }
    }
    return status;
}

} // end namespace starrocks
