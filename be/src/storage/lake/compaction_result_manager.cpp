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

#include "storage/lake/compaction_result_manager.h"

#include <fmt/format.h>

#include <chrono>
#include <random>
#include <regex>
#include <set>

#include "common/config.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "storage/data_dir.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/storage_engine.h"
#include "util/raw_container.h"

namespace starrocks::lake {

// Helper function to read file content as string
static StatusOr<std::string> read_file_to_string(const std::string& path) {
    ASSIGN_OR_RETURN(auto rf, fs::new_sequential_file(path));
    std::string content;
    raw::stl_string_resize_uninitialized(&content, 10 * 1024 * 1024); // 10MB max
    ASSIGN_OR_RETURN(auto nread, rf->read(content.data(), content.size()));
    content.resize(nread);
    return content;
}

// Helper function to write string to file
static Status write_string_to_file(const std::string& path, const std::string& content) {
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(path));
    RETURN_IF_ERROR(wf->append(content));
    RETURN_IF_ERROR(wf->close());
    return Status::OK();
}

// P7 fix: Use thread-local static generator for better performance
thread_local std::mt19937 g_random_gen(std::random_device{}());

std::string CompactionResultManager::get_results_dir(const std::string& storage_root) {
    return fmt::format("{}/lake/compaction_results", storage_root);
}

std::string CompactionResultManager::generate_result_filename(int64_t tablet_id) {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    // Use static thread-local generator instead of creating new one each time
    std::uniform_int_distribution<> dis(0, 999999);
    int random_suffix = dis(g_random_gen);

    return fmt::format("tablet_{}_result_{}_{}.pb", tablet_id, timestamp, random_suffix);
}

std::vector<std::string> CompactionResultManager::get_storage_roots() {
    std::vector<std::string> roots;
    auto* engine = StorageEngine::instance();
    if (engine == nullptr) {
        return roots;
    }

    for (const auto& data_dir : engine->get_stores()) {
        roots.push_back(data_dir->path());
    }
    return roots;
}

StatusOr<int64_t> CompactionResultManager::extract_tablet_id_from_filename(const std::string& filename) {
    // Filename format: tablet_{tablet_id}_result_{timestamp}_{random}.pb
    std::regex pattern("tablet_(\\d+)_result_\\d+_\\d+\\.pb");
    std::smatch match;
    if (std::regex_match(filename, match, pattern) && match.size() == 2) {
        try {
            return std::stoll(match[1].str());
        } catch (const std::exception& e) {
            return Status::InvalidArgument(fmt::format("Invalid tablet_id in filename: {}", filename));
        }
    }
    return Status::InvalidArgument(fmt::format("Cannot extract tablet_id from filename: {}", filename));
}

StatusOr<std::vector<std::pair<std::string, int64_t>>> CompactionResultManager::scan_all_result_files() {
    std::vector<std::pair<std::string, int64_t>> result_files;  // (file_path, tablet_id)

    auto roots = get_storage_roots();
    for (const auto& root : roots) {
        std::string results_dir = get_results_dir(root);

        // Check if directory exists
        if (!fs::path_exist(results_dir)) {
            continue;
        }

        // List all files in the directory
        std::set<std::string> dirs, files;
        auto list_st = fs::list_dirs_files(results_dir, &dirs, &files);
        if (!list_st.ok()) {
            LOG(WARNING) << "Failed to list files in " << results_dir << ": " << list_st;
            continue;
        }

        for (const auto& file : files) {
            if (file.ends_with(".pb") && file.find("tablet_") == 0) {
                std::string file_path = fmt::format("{}/{}", results_dir, file);
                auto tablet_id_or = extract_tablet_id_from_filename(file);
                if (tablet_id_or.ok()) {
                    result_files.emplace_back(file_path, tablet_id_or.value());
                }
            }
        }
    }

    return result_files;
}

// ============== Startup Recovery (Mechanism 1 from Design Doc 6.1.1) ==============

StatusOr<RecoveryStats> CompactionResultManager::recover_on_startup(TabletManager* tablet_mgr) {
    RecoveryStats stats;

    if (tablet_mgr == nullptr) {
        return Status::InvalidArgument("tablet_mgr cannot be null");
    }

    LOG(INFO) << "Starting compaction result recovery on BE startup...";

    // Step 1: Scan all result files
    ASSIGN_OR_RETURN(auto all_files, scan_all_result_files());
    stats.total_files_scanned = all_files.size();

    LOG(INFO) << "Found " << all_files.size() << " compaction result files to validate";

    // Step 2: Group files by tablet_id
    std::unordered_map<int64_t, std::vector<std::string>> tablet_files;
    for (const auto& [file_path, tablet_id] : all_files) {
        tablet_files[tablet_id].push_back(file_path);
    }

    // Step 3: Validate each result file
    std::unordered_set<int64_t> tablets_with_valid_results;

    for (const auto& [tablet_id, files] : tablet_files) {
        for (const auto& file_path : files) {
            // Read and parse the result file
            auto content_or = read_file_to_string(file_path);
            if (!content_or.ok()) {
                LOG(WARNING) << "Failed to read result file " << file_path << ": " << content_or.status();
                stats.parse_errors++;
                // Delete unreadable files
                (void)fs::delete_file(file_path);
                stats.invalid_results_cleaned++;
                continue;
            }

            CompactionResultPB result;
            if (!result.ParseFromString(content_or.value())) {
                LOG(WARNING) << "Failed to parse result file " << file_path;
                stats.parse_errors++;
                // Delete unparseable files
                (void)fs::delete_file(file_path);
                stats.invalid_results_cleaned++;
                continue;
            }

            // Validate the result
            std::string invalid_reason;
            auto valid_or = validate_result(tablet_mgr, result, &invalid_reason);
            if (!valid_or.ok()) {
                LOG(WARNING) << "Failed to validate result file " << file_path << ": " << valid_or.status();
                stats.validation_errors++;
                continue;
            }

            if (valid_or.value()) {
                // Valid result - retain it
                stats.valid_results++;
                tablets_with_valid_results.insert(tablet_id);
                LOG(INFO) << "Valid compaction result retained: tablet=" << tablet_id 
                          << ", base_version=" << result.base_version()
                          << ", input_rowsets=" << result.input_rowset_ids_size();
            } else {
                // Invalid result - clean up
                LOG(INFO) << "Invalid compaction result cleaned up: tablet=" << tablet_id
                          << ", reason=" << invalid_reason;
                (void)fs::delete_file(file_path);
                stats.invalid_results_cleaned++;
            }
        }
    }

    // Update the cache of tablets with pending results
    {
        std::lock_guard<std::mutex> lock(_pending_results_mutex);
        _tablets_with_pending_results = tablets_with_valid_results;
    }
    stats.recovered_tablet_ids = tablets_with_valid_results;

    LOG(INFO) << "Compaction result recovery completed: "
              << "total_files=" << stats.total_files_scanned
              << ", valid=" << stats.valid_results
              << ", cleaned=" << stats.invalid_results_cleaned
              << ", parse_errors=" << stats.parse_errors
              << ", validation_errors=" << stats.validation_errors
              << ", tablets_with_results=" << stats.recovered_tablet_ids.size();

    return stats;
}

StatusOr<bool> CompactionResultManager::validate_result(TabletManager* tablet_mgr, const CompactionResultPB& result,
                                                         std::string* invalid_reason) {
    if (tablet_mgr == nullptr) {
        return Status::InvalidArgument("tablet_mgr cannot be null");
    }

    if (!result.has_tablet_id()) {
        if (invalid_reason) *invalid_reason = "missing tablet_id";
        return false;
    }

    int64_t tablet_id = result.tablet_id();

    // Get latest tablet metadata by listing all versions and finding the max
    auto metadata_iter_or = tablet_mgr->list_tablet_metadata(tablet_id);
    if (!metadata_iter_or.ok()) {
        if (invalid_reason) *invalid_reason = fmt::format("tablet not found: {}", metadata_iter_or.status().message());
        return false;
    }

    TabletMetadataPtr metadata = nullptr;
    int64_t max_version = 0;
    auto& metadata_iter = metadata_iter_or.value();
    while (metadata_iter.has_next()) {
        auto meta_or = metadata_iter.next();
        if (meta_or.ok() && meta_or.value()->version() > max_version) {
            max_version = meta_or.value()->version();
            metadata = meta_or.value();
        }
    }

    if (!metadata) {
        if (invalid_reason) *invalid_reason = "tablet metadata not found";
        return false;
    }

    int64_t visible_version = metadata->version();

    // Validation 1: Check base_version <= visible_version
    if (result.has_base_version() && result.base_version() > visible_version) {
        if (invalid_reason) {
            *invalid_reason = fmt::format("base_version ({}) > visible_version ({})", 
                                          result.base_version(), visible_version);
        }
        return false;
    }

    // Validation 2: Check input rowsets still exist
    std::unordered_set<uint32_t> existing_rowsets;
    for (const auto& rowset : metadata->rowsets()) {
        existing_rowsets.insert(rowset.id());
    }

    bool all_rowsets_exist = true;
    std::vector<uint32_t> missing_rowsets;
    for (uint32_t input_rid : result.input_rowset_ids()) {
        if (existing_rowsets.find(input_rid) == existing_rowsets.end()) {
            all_rowsets_exist = false;
            missing_rowsets.push_back(input_rid);
        }
    }

    if (!all_rowsets_exist) {
        if (invalid_reason) {
            std::string missing_str;
            for (uint32_t rid : missing_rowsets) {
                if (!missing_str.empty()) missing_str += ",";
                missing_str += std::to_string(rid);
            }
            *invalid_reason = fmt::format("input rowsets no longer exist: [{}]", missing_str);
        }
        return false;
    }

    return true;
}

StatusOr<std::vector<CompactionResultPB>> CompactionResultManager::load_and_validate_results(
        TabletManager* tablet_mgr, int64_t tablet_id, int64_t max_version, bool cleanup_invalid) {
    std::vector<CompactionResultPB> valid_results;

    // Get all result files for this tablet
    ASSIGN_OR_RETURN(auto files, list_result_files(tablet_id));

    for (const auto& file_path : files) {
        // Read and parse
        auto content_or = read_file_to_string(file_path);
        if (!content_or.ok()) {
            LOG(WARNING) << "Failed to read result file " << file_path;
            if (cleanup_invalid) {
                (void)fs::delete_file(file_path);
            }
            continue;
        }

        CompactionResultPB result;
        if (!result.ParseFromString(content_or.value())) {
            LOG(WARNING) << "Failed to parse result file " << file_path;
            if (cleanup_invalid) {
                (void)fs::delete_file(file_path);
            }
            continue;
        }

        // Filter by max_version
        if (max_version >= 0 && result.has_base_version() && result.base_version() > max_version) {
            LOG(INFO) << "Skipping result with base_version=" << result.base_version() 
                      << " > max_version=" << max_version;
            continue;
        }

        // Validate
        std::string invalid_reason;
        auto valid_or = validate_result(tablet_mgr, result, &invalid_reason);
        if (valid_or.ok() && valid_or.value()) {
            valid_results.push_back(std::move(result));
        } else {
            LOG(INFO) << "Invalid result file " << file_path << ": " << invalid_reason;
            if (cleanup_invalid) {
                (void)fs::delete_file(file_path);
            }
        }
    }

    return valid_results;
}

bool CompactionResultManager::has_pending_results(int64_t tablet_id) {
    std::lock_guard<std::mutex> lock(_pending_results_mutex);
    return _tablets_with_pending_results.count(tablet_id) > 0;
}

std::unordered_set<int64_t> CompactionResultManager::get_tablets_with_pending_results() {
    std::lock_guard<std::mutex> lock(_pending_results_mutex);
    return _tablets_with_pending_results;
}

// ============== Basic Operations ==============

StatusOr<std::string> CompactionResultManager::save_result(const CompactionResultPB& result) {
    if (!result.has_tablet_id()) {
        return Status::InvalidArgument("tablet_id is required");
    }

    // Use the first storage root (typically the primary data directory)
    auto roots = get_storage_roots();
    if (roots.empty()) {
        return Status::InternalError("No storage root found");
    }

    std::string results_dir = get_results_dir(roots[0]);

    // Ensure the results directory exists
    RETURN_IF_ERROR(fs::create_directories(results_dir));

    // Generate unique filename
    std::string filename = generate_result_filename(result.tablet_id());
    std::string file_path = fmt::format("{}/{}", results_dir, filename);

    // Serialize and write to file
    std::string serialized;
    if (!result.SerializeToString(&serialized)) {
        return Status::InternalError("Failed to serialize CompactionResultPB");
    }

    RETURN_IF_ERROR(write_string_to_file(file_path, serialized));

    // Update pending results cache
    {
        std::lock_guard<std::mutex> lock(_pending_results_mutex);
        _tablets_with_pending_results.insert(result.tablet_id());
    }

    LOG(INFO) << "Saved compaction result for tablet " << result.tablet_id() << " to " << file_path
              << ", base_version=" << result.base_version();

    return file_path;
}

StatusOr<std::vector<CompactionResultPB>> CompactionResultManager::load_results(int64_t tablet_id,
                                                                                 int64_t max_version) {
    std::vector<CompactionResultPB> results;

    auto roots = get_storage_roots();
    for (const auto& root : roots) {
        std::string results_dir = get_results_dir(root);

        // Check if directory exists
        if (!fs::path_exist(results_dir)) {
            continue;
        }

        // List all files in the directory
        std::set<std::string> dirs, files;
        RETURN_IF_ERROR(fs::list_dirs_files(results_dir, &dirs, &files));

        // Filter files for this tablet
        std::string tablet_prefix = fmt::format("tablet_{}_result_", tablet_id);
        for (const auto& file : files) {
            if (file.find(tablet_prefix) == 0 && file.ends_with(".pb")) {
                std::string file_path = fmt::format("{}/{}", results_dir, file);

                // Read and deserialize
                std::string content;
                ASSIGN_OR_RETURN(content, read_file_to_string(file_path));

                CompactionResultPB result;
                if (!result.ParseFromString(content)) {
                    LOG(WARNING) << "Failed to parse compaction result from " << file_path << ", skipping";
                    continue;
                }

                // Filter by version if specified
                if (max_version >= 0 && result.base_version() > max_version) {
                    LOG(INFO) << "Skipping result from " << file_path << " with base_version=" << result.base_version()
                              << " > max_version=" << max_version;
                    continue;
                }

                results.push_back(std::move(result));
            }
        }
    }

    LOG(INFO) << "Loaded " << results.size() << " compaction results for tablet " << tablet_id;
    return results;
}

Status CompactionResultManager::delete_result(const std::string& file_path) {
    auto st = fs::delete_file(file_path);
    if (st.ok()) {
        LOG(INFO) << "Deleted compaction result file: " << file_path;
    } else {
        LOG(WARNING) << "Failed to delete compaction result file " << file_path << ": " << st;
    }
    return st;
}

Status CompactionResultManager::delete_tablet_results(int64_t tablet_id) {
    auto files_or = list_result_files(tablet_id);
    if (!files_or.ok()) {
        return files_or.status();
    }

    int deleted_count = 0;
    for (const auto& file_path : files_or.value()) {
        auto st = fs::delete_file(file_path);
        if (st.ok()) {
            deleted_count++;
        } else {
            LOG(WARNING) << "Failed to delete result file " << file_path << ": " << st;
        }
    }

    // Update pending results cache
    {
        std::lock_guard<std::mutex> lock(_pending_results_mutex);
        _tablets_with_pending_results.erase(tablet_id);
    }

    LOG(INFO) << "Deleted " << deleted_count << " compaction result files for tablet " << tablet_id;
    return Status::OK();
}

StatusOr<std::vector<std::string>> CompactionResultManager::list_result_files(int64_t tablet_id) {
    std::vector<std::string> result_files;

    auto roots = get_storage_roots();
    for (const auto& root : roots) {
        std::string results_dir = get_results_dir(root);

        // Check if directory exists
        if (!fs::path_exist(results_dir)) {
            continue;
        }

        // List all files for this tablet
        std::set<std::string> dirs, files;
        RETURN_IF_ERROR(fs::list_dirs_files(results_dir, &dirs, &files));

        std::string tablet_prefix = fmt::format("tablet_{}_result_", tablet_id);
        for (const auto& file : files) {
            if (file.find(tablet_prefix) == 0 && file.ends_with(".pb")) {
                result_files.push_back(fmt::format("{}/{}", results_dir, file));
            }
        }
    }

    return result_files;
}

} // namespace starrocks::lake


