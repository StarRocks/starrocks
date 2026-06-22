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

#include "storage/lake/tablet_restore.h"

#include <algorithm>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/utility/defer_op.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gen_cpp/AgentService_types.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/protobuf_file.h"
#include "storage/storage_env.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

namespace {

struct RestoreTabletInfo {
    int64_t source_tablet_id = 0;
    int64_t target_tablet_id = 0;
    int64_t target_schema_id = TabletSchema::invalid_id();
};

Status ensure_parent_dir(const std::shared_ptr<FileSystem>& fs, const std::string& path) {
    auto pos = path.find_last_of('/');
    if (pos == std::string::npos) {
        return Status::OK();
    }
    return fs->create_dir_recursive(path.substr(0, pos));
}

Status copy_file_between_fs(const std::shared_ptr<FileSystem>& src_fs, const std::string& src_path,
                            const std::shared_ptr<FileSystem>& dst_fs, const std::string& dst_path) {
    WritableFileOptions options;
    options.sync_on_close = false;
    options.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    RETURN_IF_ERROR(ensure_parent_dir(dst_fs, dst_path));
    ASSIGN_OR_RETURN(auto src_file, src_fs->new_sequential_file(src_path));
    ASSIGN_OR_RETURN(auto dst_file, dst_fs->new_writable_file(options, dst_path));
    RETURN_IF_ERROR(fs::copy(src_file.get(), dst_file.get()));
    return dst_file->close();
}

Status copy_dir_recursive(const std::shared_ptr<FileSystem>& src_fs, const std::string& src,
                          const std::shared_ptr<FileSystem>& dst_fs, const std::string& dst) {
    RETURN_IF_ERROR(dst_fs->create_dir_recursive(dst));
    Status status = Status::OK();
    auto iterate_status = src_fs->iterate_dir2(src, [&](DirEntry entry) {
        if (!status.ok()) {
            return false;
        }
        std::string name(entry.name);
        while (!name.empty() && name.back() == '/') {
            name.pop_back();
        }
        if (name.empty() || name == "." || name == "..") {
            return true;
        }
        const std::string src_child = join_path(src, name);
        const std::string dst_child = join_path(dst, name);
        bool is_dir = entry.is_dir.value_or(false);
        if (!entry.is_dir.has_value()) {
            auto dir_status = src_fs->is_directory(src_child);
            if (!dir_status.ok()) {
                status = dir_status.status();
                return false;
            }
            is_dir = dir_status.value();
        }
        Status op_status = Status::OK();
        if (is_dir) {
            op_status = copy_dir_recursive(src_fs, src_child, dst_fs, dst_child);
        } else {
            op_status = copy_file_between_fs(src_fs, src_child, dst_fs, dst_child);
        }
        if (!op_status.ok()) {
            status = std::move(op_status);
            return false;
        }
        return true;
    });
    RETURN_IF_ERROR(iterate_status);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status delete_path_if_exists(const std::shared_ptr<FileSystem>& fs, const std::string& path) {
    auto status = fs->is_directory(path);
    if (status.ok()) {
        if (status.value()) {
            RETURN_IF_ERROR(fs->delete_dir_recursive(path));
        } else {
            RETURN_IF_ERROR(fs->delete_file(path));
        }
        return Status::OK();
    }
    if (status.status().is_not_found()) {
        return Status::OK();
    }
    return status.status();
}

Status update_target_schema(TabletMetadataPB* metadata, int64_t target_schema_id) {
    if (metadata == nullptr) {
        return Status::InvalidArgument("metadata is null");
    }
    if (!metadata->has_schema()) {
        return Status::InvalidArgument("tablet metadata missing schema definition");
    }
    if (target_schema_id <= 0) {
        return Status::InvalidArgument(fmt::format("invalid target schema id {}", target_schema_id));
    }

    int64_t original_schema_id = metadata->schema().id();
    metadata->mutable_schema()->set_id(target_schema_id);

    auto* rowset_to_schema = metadata->mutable_rowset_to_schema();
    for (auto& entry : *rowset_to_schema) {
        if (entry.second == original_schema_id) {
            entry.second = target_schema_id;
        }
    }

    auto* historical_schemas = metadata->mutable_historical_schemas();
    for (auto& entry : *historical_schemas) {
        entry.second.set_id(entry.first);
    }

    if (original_schema_id != target_schema_id) {
        auto iter = historical_schemas->find(original_schema_id);
        if (iter != historical_schemas->end()) {
            TabletSchemaPB schema_pb = std::move(iter->second);
            schema_pb.set_id(target_schema_id);
            (*historical_schemas)[target_schema_id] = schema_pb;
            historical_schemas->erase(iter);
        } else {
            (*historical_schemas)[target_schema_id] = metadata->schema();
        }
    } else if (historical_schemas->find(target_schema_id) == historical_schemas->end()) {
        (*historical_schemas)[target_schema_id] = metadata->schema();
    }

    return Status::OK();
}

Status recreate_schema_files(TabletManager* tablet_mgr, int64_t tablet_id, const TabletMetadataPB& metadata) {
    std::unordered_set<int64_t> processed;
    auto create_schema = [&](const TabletSchemaPB& schema_pb) -> Status {
        if (schema_pb.id() == TabletSchema::invalid_id()) {
            return Status::InvalidArgument(
                    fmt::format("invalid schema id {} for tablet {}", schema_pb.id(), tablet_id));
        }
        if (!processed.insert(schema_pb.id()).second) {
            return Status::OK();
        }
        return tablet_mgr->create_schema_file(tablet_id, schema_pb);
    };

    if (metadata.has_schema()) {
        RETURN_IF_ERROR(create_schema(metadata.schema()));
    }
    for (const auto& entry : metadata.historical_schemas()) {
        RETURN_IF_ERROR(create_schema(entry.second));
    }
    return Status::OK();
}

Status list_meta_files(FileSystem* fs, const std::string& metadata_root_location, std::list<std::string>* meta_files,
                       std::list<std::string>* bundle_meta_files) {
    RETURN_IF_ERROR(ignore_not_found(fs->iterate_dir(metadata_root_location, [&](std::string_view name) {
        if (!is_tablet_metadata(name)) {
            return true;
        }
        auto [tablet_id, version] = parse_tablet_metadata_filename(basename(name));
        if (tablet_id == 0 && version != kInitialVersion) {
            bundle_meta_files->emplace_back(name);
        } else {
            meta_files->emplace_back(name);
        }
        return true;
    })));
    LOG(INFO) << "tablet restore scanned metadata root " << metadata_root_location << ", found " << meta_files->size()
              << " meta files and " << bundle_meta_files->size() << " bundle meta files";
    return Status::OK();
}

StatusOr<std::vector<RestoreTabletInfo>> build_restore_tablet_infos(const TRestoreTabletRequest& request) {
    if (!request.__isset.tablet_infos || request.tablet_infos.empty()) {
        return Status::InvalidArgument("tablet restore request missing tablet infos");
    }
    std::vector<RestoreTabletInfo> tablets;
    tablets.reserve(request.tablet_infos.size());
    for (const auto& tablet_info : request.tablet_infos) {
        if (!tablet_info.__isset.source_tablet_id || !tablet_info.__isset.target_tablet_id ||
            tablet_info.source_tablet_id <= 0 || tablet_info.target_tablet_id <= 0) {
            return Status::InvalidArgument("tablet info missing tablet identifiers");
        }
        RestoreTabletInfo info;
        info.source_tablet_id = tablet_info.source_tablet_id;
        info.target_tablet_id = tablet_info.target_tablet_id;
        if (tablet_info.__isset.target_schema_id && tablet_info.target_schema_id > 0) {
            info.target_schema_id = tablet_info.target_schema_id;
        }
        tablets.emplace_back(std::move(info));
    }
    return tablets;
}

Status reset_metadata_root(TabletManager* tablet_mgr, int64_t tablet_id,
                           std::unordered_set<std::string>* cleared_roots) {
    std::string meta_root = tablet_mgr->tablet_metadata_root_location(tablet_id);
    std::string cleared_key = meta_root;
    if (auto provider = tablet_mgr->location_provider(); provider != nullptr) {
        auto real_or = provider->real_location(meta_root);
        if (real_or.ok()) {
            cleared_key = real_or.value();
        }
    }
    if (!cleared_roots->insert(cleared_key).second) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto dst_meta_fs, FileSystemFactory::CreateSharedFromString(meta_root));
    Status st = dst_meta_fs->delete_dir_recursive(meta_root);
    if (!st.ok() && !st.is_not_found()) {
        return st;
    }
    return dst_meta_fs->create_dir_recursive(meta_root);
}

Status process_regular_metadata(TabletManager* tablet_mgr, const std::string& src_meta_root,
                                const std::vector<std::string>& meta_files,
                                const std::vector<RestoreTabletInfo>& tablets,
                                std::unordered_set<int64_t>* schema_initialized, bool* processed_any) {
    std::unordered_map<int64_t, const RestoreTabletInfo*> tablet_map;
    tablet_map.reserve(tablets.size());
    for (const auto& tablet : tablets) {
        tablet_map.emplace(tablet.source_tablet_id, &tablet);
    }

    for (const auto& meta_file : meta_files) {
        auto path = join_path(src_meta_root, meta_file);
        ASSIGN_OR_RETURN(auto metadata_ptr, tablet_mgr->get_tablet_metadata(path, false));

        int64_t source_tablet_id = metadata_ptr->id();
        auto info_iter = tablet_map.find(source_tablet_id);
        if (info_iter == tablet_map.end()) {
            LOG(INFO) << fmt::format(
                    "tablet restore skipping meta file {} because meta tablet {} not in restore request", path,
                    source_tablet_id);
            continue;
        }

        const auto* tablet_info = info_iter->second;
        auto [source_tablet_from_name, version_from_name] = parse_tablet_metadata_filename(basename(meta_file));
        LOG(INFO) << fmt::format(
                "tablet restore processing meta file {} (parsed tablet {}, version {}, meta tablet {}) -> target "
                "tablet "
                "{}, target schema {}",
                path, source_tablet_from_name, version_from_name, source_tablet_id, tablet_info->target_tablet_id,
                tablet_info->target_schema_id);

        auto mutable_meta = std::make_shared<TabletMetadata>(*metadata_ptr);
        mutable_meta->set_id(tablet_info->target_tablet_id);
        if (tablet_info->target_schema_id > 0) {
            RETURN_IF_ERROR(update_target_schema(mutable_meta.get(), tablet_info->target_schema_id));
        }
        auto const_meta = std::static_pointer_cast<const TabletMetadata>(mutable_meta);
        RETURN_IF_ERROR(tablet_mgr->put_tablet_metadata(const_meta));
        if (schema_initialized->insert(tablet_info->target_tablet_id).second) {
            RETURN_IF_ERROR(recreate_schema_files(tablet_mgr, tablet_info->target_tablet_id, *mutable_meta));
        }
        *processed_any = true;
    }

    return Status::OK();
}

Status hydrate_schema_from_bundle(const BundleTabletMetadataPB& bundle_metadata, int64_t tablet_id,
                                  TabletMetadataPB* metadata) {
    auto tablet_schema_iter = bundle_metadata.tablet_to_schema().find(tablet_id);
    if (tablet_schema_iter == bundle_metadata.tablet_to_schema().end()) {
        return Status::Corruption(fmt::format("tablet {} missing schema mapping in bundle metadata", tablet_id));
    }
    auto schema_iter = bundle_metadata.schemas().find(tablet_schema_iter->second);
    if (schema_iter == bundle_metadata.schemas().end()) {
        return Status::Corruption(fmt::format("schema {} missing from bundle metadata for tablet {}",
                                              tablet_schema_iter->second, tablet_id));
    }
    metadata->mutable_schema()->CopyFrom(schema_iter->second);
    auto& historical = *metadata->mutable_historical_schemas();
    historical[tablet_schema_iter->second] = schema_iter->second;

    for (auto& entry : *metadata->mutable_rowset_to_schema()) {
        auto rowset_schema_iter = bundle_metadata.schemas().find(entry.second);
        if (rowset_schema_iter == bundle_metadata.schemas().end()) {
            return Status::Corruption(
                    fmt::format("rowset schema {} missing in bundle metadata for tablet {}", entry.second, tablet_id));
        }
        historical[entry.second] = rowset_schema_iter->second;
    }
    return Status::OK();
}

Status collect_partition_bundle_metadata(const std::vector<const RestoreTabletInfo*>& tablets,
                                         const std::string& src_meta_root,
                                         const std::shared_ptr<FileSystem>& src_meta_fs,
                                         const std::list<std::string>& bundle_meta_files,
                                         std::map<int64_t, std::map<int64_t, TabletMetadataPB>>* version_to_metas,
                                         bool* processed_any) {
    if (tablets.empty() || bundle_meta_files.empty()) {
        return Status::OK();
    }
    if (src_meta_fs == nullptr) {
        return Status::InvalidArgument("missing filesystem for bundle metadata");
    }

    std::unordered_map<int64_t, const RestoreTabletInfo*> tablet_map;
    tablet_map.reserve(tablets.size());
    for (const auto* tablet : tablets) {
        tablet_map.emplace(tablet->source_tablet_id, tablet);
    }

    RandomAccessFileOptions options;
    options.skip_fill_local_cache = true;
    bool local_processed = false;

    for (const auto& bundle_meta_file : bundle_meta_files) {
        auto [unused_tablet_id, version] = parse_tablet_metadata_filename(bundle_meta_file);
        (void)unused_tablet_id;
        const std::string path = join_path(src_meta_root, bundle_meta_file);
        ASSIGN_OR_RETURN(auto file, src_meta_fs->new_random_access_file(options, path));
        ASSIGN_OR_RETURN(auto serialized_string, file->read_all());
        ASSIGN_OR_RETURN(auto bundle_metadata, TabletManager::parse_bundle_tablet_metadata(path, serialized_string));
        const auto file_size = serialized_string.size();
        std::unordered_set<int64_t> seen_tablets;
        seen_tablets.reserve(tablet_map.size());

        for (const auto& entry : bundle_metadata->tablet_meta_pages()) {
            auto info_iter = tablet_map.find(entry.first);
            if (info_iter == tablet_map.end()) {
                continue;
            }
            size_t offset = entry.second.offset();
            size_t size = entry.second.size();
            if (offset + size > file_size) {
                return Status::Corruption(
                        fmt::format("invalid page pointer for tablet {} in bundle {}", entry.first, path));
            }
            TabletMetadataPB dest_meta;
            std::string_view metadata_sv(serialized_string.data() + offset, size);
            if (!dest_meta.ParseFromArray(metadata_sv.data(), size)) {
                return Status::Corruption(
                        fmt::format("failed to parse tablet {} metadata from bundle {}", entry.first, path));
            }
            RETURN_IF_ERROR(hydrate_schema_from_bundle(*bundle_metadata, entry.first, &dest_meta));

            dest_meta.set_id(info_iter->second->target_tablet_id);
            if (info_iter->second->target_schema_id > 0) {
                RETURN_IF_ERROR(update_target_schema(&dest_meta, info_iter->second->target_schema_id));
            }

            auto& metas = (*version_to_metas)[version];
            metas[info_iter->second->target_tablet_id] = std::move(dest_meta);
            local_processed = true;
            seen_tablets.insert(entry.first);
        }

        if (seen_tablets.size() != tablet_map.size()) {
            for (const auto& [tablet_id, _] : tablet_map) {
                if (seen_tablets.find(tablet_id) == seen_tablets.end()) {
                    return Status::NotFound(fmt::format("tablet {} metadata missing from bundle {}", tablet_id, path));
                }
            }
        }
    }

    if (local_processed && processed_any != nullptr) {
        *processed_any = true;
    }
    return Status::OK();
}

Status write_bundle_metadata(TabletManager* tablet_mgr,
                             std::map<int64_t, std::map<int64_t, TabletMetadataPB>>* version_to_metas,
                             std::unordered_set<int64_t>* schema_initialized) {
    for (auto& [version, tablet_metas] : *version_to_metas) {
        (void)version;
        for (auto& [tablet_id, meta] : tablet_metas) {
            if (schema_initialized->insert(tablet_id).second) {
                RETURN_IF_ERROR(recreate_schema_files(tablet_mgr, tablet_id, meta));
            }
        }
        RETURN_IF_ERROR(tablet_mgr->put_bundle_tablet_metadata(tablet_metas));
    }
    return Status::OK();
}

Status copy_tablet_tree(TabletManager* tablet_mgr, int64_t src_tablet_id, int64_t dst_tablet_id) {
    if (src_tablet_id == dst_tablet_id) {
        return Status::InvalidArgument("source and target tablet are identical");
    }

    std::string src_root = tablet_mgr->tablet_root_location(src_tablet_id);
    std::string dst_root = tablet_mgr->tablet_root_location(dst_tablet_id);
    ASSIGN_OR_RETURN(auto src_fs, FileSystemFactory::CreateSharedFromString(src_root));
    ASSIGN_OR_RETURN(auto dst_fs, FileSystemFactory::CreateSharedFromString(dst_root));

    Status copy_status = Status::OK();
    auto iterate_status = src_fs->iterate_dir2(src_root, [&](DirEntry entry) {
        if (!copy_status.ok()) {
            return false;
        }
        std::string name(entry.name);
        while (!name.empty() && name.back() == '/') {
            name.pop_back();
        }
        if (name.empty() || name == "." || name == "..") {
            return true;
        }
        if (name == kMetadataDirectoryName) {
            return true; // metadata handled separately
        }
        const std::string src_child = join_path(src_root, name);
        const std::string dst_child = join_path(dst_root, name);
        if (is_schema_file(name)) {
            Status remove_status = delete_path_if_exists(dst_fs, dst_child);
            if (!remove_status.ok()) {
                copy_status = std::move(remove_status);
                return false;
            }
            return true;
        }
        bool is_dir = entry.is_dir.value_or(false);
        if (!entry.is_dir.has_value()) {
            auto dir_status = src_fs->is_directory(src_child);
            if (!dir_status.ok()) {
                copy_status = dir_status.status();
                return false;
            }
            is_dir = dir_status.value();
        }
        Status op_status = Status::OK();
        if (is_dir) {
            op_status = delete_path_if_exists(dst_fs, dst_child);
            if (!op_status.ok()) {
                copy_status = std::move(op_status);
                return false;
            }
            op_status = copy_dir_recursive(src_fs, src_child, dst_fs, dst_child);
        } else {
            op_status = copy_file_between_fs(src_fs, src_child, dst_fs, dst_child);
        }
        if (!op_status.ok()) {
            copy_status = std::move(op_status);
            return false;
        }
        return true;
    });
    RETURN_IF_ERROR(iterate_status);
    if (!copy_status.ok()) {
        return copy_status;
    }

    return Status::OK();
}

Status rewrite_tablets_metadata(TabletManager* tablet_mgr, const std::vector<RestoreTabletInfo>& tablets,
                                std::optional<int64_t> source_visible_version) {
    if (tablets.empty()) {
        return Status::InvalidArgument("no tablet info provided");
    }

    std::unordered_set<std::string> cleared_meta_roots;
    for (const auto& tablet : tablets) {
        RETURN_IF_ERROR(reset_metadata_root(tablet_mgr, tablet.target_tablet_id, &cleared_meta_roots));
    }

    std::unordered_set<int64_t> schema_initialized;
    std::map<int64_t, std::map<int64_t, TabletMetadataPB>> version_to_bundle_metas;
    bool processed_any = false;

    const RestoreTabletInfo& primary_tablet = tablets.front();
    std::string meta_root = tablet_mgr->tablet_metadata_root_location(primary_tablet.source_tablet_id);
    ASSIGN_OR_RETURN(auto meta_fs, FileSystemFactory::CreateSharedFromString(meta_root));

    std::list<std::string> regular_meta_files;
    std::list<std::string> bundle_meta_files;
    RETURN_IF_ERROR(list_meta_files(meta_fs.get(), meta_root, &regular_meta_files, &bundle_meta_files));
    if (source_visible_version.has_value()) {
        auto filter_by_version = [&](std::list<std::string>& files) {
            files.remove_if([&](const std::string& filename) {
                auto parsed = parse_tablet_metadata_filename(basename(filename));
                return parsed.second != source_visible_version.value();
            });
        };
        filter_by_version(regular_meta_files);
        filter_by_version(bundle_meta_files);
    }
    LOG(INFO) << fmt::format(
            "tablet restore collected files for meta root {}, meta entries {}, bundle files {}, "
            "source visible version {}",
            meta_root, regular_meta_files.size(), bundle_meta_files.size(), source_visible_version.value_or(-1));

    if (!regular_meta_files.empty()) {
        RETURN_IF_ERROR(process_regular_metadata(
                tablet_mgr, meta_root, std::vector<std::string>(regular_meta_files.begin(), regular_meta_files.end()),
                tablets, &schema_initialized, &processed_any));
    }

    if (!bundle_meta_files.empty()) {
        std::vector<const RestoreTabletInfo*> tablet_ptrs;
        tablet_ptrs.reserve(tablets.size());
        for (const auto& tablet : tablets) {
            tablet_ptrs.push_back(&tablet);
        }
        RETURN_IF_ERROR(collect_partition_bundle_metadata(tablet_ptrs, meta_root, meta_fs, bundle_meta_files,
                                                          &version_to_bundle_metas, &processed_any));

        if (!version_to_bundle_metas.empty()) {
            RETURN_IF_ERROR(write_bundle_metadata(tablet_mgr, &version_to_bundle_metas, &schema_initialized));
        }
    }

    if (!processed_any) {
        return Status::NotFound(fmt::format("no metadata found for tablet {}", tablets.front().source_tablet_id));
    }

    return Status::OK();
}

} // namespace

Status restore_tablet_data(ExecEnv* env, const TRestoreTabletRequest& request) {
    (void)env;
    auto* tablet_mgr = StorageEnv::GetInstance()->lake_tablet_manager();
    if (tablet_mgr == nullptr) {
        return Status::NotSupported("lake tablet manager is not initialized");
    }

    ASSIGN_OR_RETURN(auto tablets, build_restore_tablet_infos(request));
    std::optional<int64_t> source_visible_version;
    if (request.__isset.source_visible_version && request.source_visible_version > 0) {
        source_visible_version = request.source_visible_version;
    }
    for (const auto& tablet : tablets) {
        RETURN_IF_ERROR(tablet_mgr->get_tablet(tablet.source_tablet_id).status());
        RETURN_IF_ERROR(tablet_mgr->get_tablet(tablet.target_tablet_id).status());
    }

    for (const auto& tablet : tablets) {
        RETURN_IF_ERROR(copy_tablet_tree(tablet_mgr, tablet.source_tablet_id, tablet.target_tablet_id));
    }
    RETURN_IF_ERROR(rewrite_tablets_metadata(tablet_mgr, tablets, source_visible_version));

    tablet_mgr->prune_metacache();

    return Status::OK();
}

} // namespace starrocks::lake
