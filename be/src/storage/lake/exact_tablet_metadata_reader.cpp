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

#include "storage/lake/exact_tablet_metadata_reader.h"

#include <fmt/format.h>

#include <limits>
#include <string>
#include <utility>

#include "base/coding.h"
#include "common/storage_define.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/filenames.h"
#include "storage/lake/lake_proto_normalizer.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/protobuf_file.h"
#include "storage/utils.h"

namespace starrocks::lake {
namespace {

Status validate_metadata_identity(const TabletMetadataPB& metadata, std::optional<int64_t> tablet_id, int64_t version,
                                  const std::string& path) {
    if (tablet_id.has_value() && metadata.id() != *tablet_id) {
        return Status::Corruption(
                fmt::format("Tablet ID mismatch in {}, expected={}, actual={}", path, *tablet_id, metadata.id()));
    }
    if (metadata.version() != version) {
        return Status::Corruption(fmt::format("Tablet version mismatch in {}, expected={}, actual={}", path, version,
                                              metadata.version()));
    }
    return Status::OK();
}

Status validate_allocation_size(uint64_t size, uint64_t limit, const std::string& description,
                                const std::string& path) {
    if (size == 0 || size > std::numeric_limits<size_t>::max() ||
        size > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return Status::Corruption(fmt::format("Invalid {} size in {}: {} bytes", description, path, size));
    }
    if (size > limit) {
        return Status::CapacityLimitExceed(
                fmt::format("{} size in {} exceeds limit: {} bytes > {} bytes", description, path, size, limit));
    }
    return Status::OK();
}

} // namespace

StatusOr<TabletMetadataPtr> read_bundle_tablet_metadata_page(int64_t tablet_id, int64_t version,
                                                             const std::string& bundle_path,
                                                             const std::shared_ptr<FileSystem>& fs,
                                                             ExactTabletMetadataReadLimits limits) {
    RandomAccessFileOptions opts{.skip_fill_local_cache = true, .skip_disk_cache = true};
    ASSIGN_OR_RETURN(auto input_file, fs->new_random_access_file(opts, bundle_path));
    ASSIGN_OR_RETURN(auto signed_file_size, input_file->get_size());
    if (signed_file_size < 0) {
        return Status::Corruption(
                fmt::format("Invalid source metadata bundle size in {}: {}", bundle_path, signed_file_size));
    }
    const uint64_t file_size = signed_file_size;

    constexpr uint64_t kSizeFieldSize = sizeof(uint64_t);
    if (file_size < kSizeFieldSize) {
        return Status::Corruption(
                fmt::format("Source metadata bundle {} is too small: {} bytes", bundle_path, file_size));
    }

    std::string size_field(kSizeFieldSize, '\0');
    RETURN_IF_ERROR(input_file->read_at_fully(file_size - kSizeFieldSize, size_field.data(), size_field.size()));
    const uint64_t raw_bundle_metadata_size = decode_fixed64_le(reinterpret_cast<const uint8_t*>(size_field.data()));
    const bool checksummed = (raw_bundle_metadata_size & LAKE_BUNDLE_META_CHECKSUM_FLAG) != 0;
    const uint64_t bundle_metadata_size = raw_bundle_metadata_size & ~LAKE_BUNDLE_META_CHECKSUM_FLAG;
    const uint64_t footer_suffix_size = kSizeFieldSize + (checksummed ? sizeof(uint32_t) : 0);
    if (file_size < footer_suffix_size || bundle_metadata_size == 0 ||
        bundle_metadata_size > std::numeric_limits<size_t>::max() - footer_suffix_size ||
        bundle_metadata_size > file_size - footer_suffix_size) {
        return Status::Corruption(
                fmt::format("Invalid source metadata bundle footer in {}, file_size={}, bundle_metadata_size={}",
                            bundle_path, file_size, bundle_metadata_size));
    }
    if (bundle_metadata_size > limits.max_bundle_footer_bytes) {
        return Status::CapacityLimitExceed(
                fmt::format("Bundle metadata footer size in {} exceeds limit: {} bytes > {} bytes", bundle_path,
                            bundle_metadata_size, limits.max_bundle_footer_bytes));
    }

    const uint64_t bundle_metadata_offset = file_size - footer_suffix_size - bundle_metadata_size;
    std::string footer(bundle_metadata_size + footer_suffix_size, '\0');
    RETURN_IF_ERROR(input_file->read_at_fully(bundle_metadata_offset, footer.data(), footer.size()));
    ASSIGN_OR_RETURN(auto bundle, TabletManager::parse_bundle_tablet_metadata(bundle_path, footer));

    auto page_it = bundle->tablet_meta_pages().find(tablet_id);
    if (page_it == bundle->tablet_meta_pages().end()) {
        return Status::NotFound(
                fmt::format("Tablet {} is absent from source metadata bundle {}", tablet_id, bundle_path));
    }
    const uint64_t offset = page_it->second.offset();
    const uint64_t size = page_it->second.size();
    RETURN_IF_ERROR(validate_allocation_size(size, limits.max_metadata_bytes, "tablet metadata page", bundle_path));
    if (offset > bundle_metadata_offset || size > bundle_metadata_offset - offset) {
        return Status::Corruption(fmt::format("Invalid source tablet metadata page in {}, offset={}, size={}",
                                              bundle_path, offset, size));
    }

    std::string page(size, '\0');
    RETURN_IF_ERROR(input_file->read_at_fully(offset, page.data(), page.size()));
    auto checksum_it = bundle->tablet_meta_page_checksum().find(tablet_id);
    if (checksum_it != bundle->tablet_meta_page_checksum().end() &&
        olap_adler32(ADLER32_INIT, page.data(), page.size()) != checksum_it->second) {
        return Status::Corruption(
                fmt::format("Mismatched checksum for tablet {} metadata in {}", tablet_id, bundle_path));
    }

    auto metadata = std::make_shared<TabletMetadataPB>();
    if (!metadata->ParseFromArray(page.data(), page.size())) {
        return Status::Corruption(fmt::format("Failed to parse tablet {} metadata from {}", tablet_id, bundle_path));
    }
    RETURN_IF_ERROR(validate_metadata_identity(*metadata, tablet_id, version, bundle_path));
    normalize_tablet_metadata_after_load(metadata.get());

    auto schema_id_it = bundle->tablet_to_schema().find(tablet_id);
    if (schema_id_it == bundle->tablet_to_schema().end()) {
        return Status::Corruption(
                fmt::format("Schema mapping for tablet {} is absent from {}", tablet_id, bundle_path));
    }
    auto schema_it = bundle->schemas().find(schema_id_it->second);
    if (schema_it == bundle->schemas().end()) {
        return Status::Corruption(
                fmt::format("Schema {} for tablet {} is absent from {}", schema_id_it->second, tablet_id, bundle_path));
    }
    metadata->mutable_schema()->CopyFrom(schema_it->second);
    (*metadata->mutable_historical_schemas())[schema_id_it->second].CopyFrom(schema_it->second);
    force_cloud_native_pk_persistent_index(metadata.get());

    for (const auto& [_, historical_schema_id] : metadata->rowset_to_schema()) {
        auto historical_schema_it = bundle->schemas().find(historical_schema_id);
        if (historical_schema_it == bundle->schemas().end()) {
            return Status::Corruption(fmt::format("Historical schema {} for tablet {} is absent from {}",
                                                  historical_schema_id, tablet_id, bundle_path));
        }
        (*metadata->mutable_historical_schemas())[historical_schema_id].CopyFrom(historical_schema_it->second);
    }
    return metadata;
}

ExactTabletMetadataReader::ExactTabletMetadataReader(std::shared_ptr<LocationProvider> location_provider,
                                                     ExactTabletMetadataReadLimits limits,
                                                     std::shared_ptr<FileSystem> fs)
        : _location_provider(std::move(location_provider)), _limits(limits), _fs(std::move(fs)) {}

StatusOr<std::shared_ptr<FileSystem>> ExactTabletMetadataReader::_filesystem(const std::string& path) const {
    if (_fs != nullptr) {
        return _fs;
    }
    return FileSystemFactory::CreateSharedFromString(path);
}

StatusOr<TabletMetadataPtr> ExactTabletMetadataReader::_read_standalone_file(const std::string& path,
                                                                             std::optional<int64_t> expected_tablet_id,
                                                                             int64_t expected_version) const {
    ASSIGN_OR_RETURN(auto fs, _filesystem(path));
    RandomAccessFileOptions opts{.skip_fill_local_cache = true, .skip_disk_cache = true};
    ASSIGN_OR_RETURN(auto input_file, fs->new_random_access_file(opts, path));
    ASSIGN_OR_RETURN(auto signed_size, input_file->get_size());
    if (signed_size < 0) {
        return Status::Corruption(fmt::format("Invalid tablet metadata object size in {}: {}", path, signed_size));
    }
    const uint64_t size = signed_size;
    RETURN_IF_ERROR(validate_allocation_size(size, _limits.max_metadata_bytes, "tablet metadata object", path));

    std::string content(size, '\0');
    RETURN_IF_ERROR(input_file->read_at_fully(0, content.data(), content.size()));
    auto metadata = std::make_shared<TabletMetadataPB>();
    RETURN_IF_ERROR(ProtobufFileWithHeader::load_from_buffer_strict(
            metadata.get(), content, LAKE_META_HEADER_MAGIC_NUMBER, /*allow_plain_protobuf_fallback=*/true));
    RETURN_IF_ERROR(validate_metadata_identity(*metadata, expected_tablet_id, expected_version, path));
    normalize_tablet_metadata_after_load(metadata.get());
    return metadata;
}

StatusOr<TabletMetadataPtr> ExactTabletMetadataReader::_read_standalone(int64_t tablet_id, int64_t version) const {
    const auto path = _location_provider->tablet_metadata_location(tablet_id, version);
    auto metadata = _read_standalone_file(path, tablet_id, version);
    if (metadata.ok() || !metadata.status().is_not_found() || tablet_id == 0 || version != kInitialVersion) {
        return metadata;
    }

    const auto shared_path = _location_provider->tablet_initial_metadata_location(tablet_id);
    ASSIGN_OR_RETURN(auto shared_metadata, _read_standalone_file(shared_path, std::nullopt, kInitialVersion));
    auto logical_metadata = std::make_shared<TabletMetadataPB>(*shared_metadata);
    logical_metadata->set_id(tablet_id);
    return logical_metadata;
}

StatusOr<TabletMetadataPtr> ExactTabletMetadataReader::read(int64_t tablet_id, int64_t version,
                                                            TabletMetadataStorageFormat format) const {
    if (tablet_id <= 0) {
        return Status::InvalidArgument(fmt::format("tablet_id must be positive: {}", tablet_id));
    }
    if (version <= 0) {
        return Status::InvalidArgument(fmt::format("version must be positive: {}", version));
    }
    if (version == kInitialVersion && format == TabletMetadataStorageFormat::kBundle) {
        return Status::InvalidArgument("version 1 tablet metadata is always standalone");
    }
    if (format == TabletMetadataStorageFormat::kStandalone) {
        return _read_standalone(tablet_id, version);
    }

    const auto path = _location_provider->bundle_tablet_metadata_location(tablet_id, version);
    ASSIGN_OR_RETURN(auto fs, _filesystem(path));
    return read_bundle_tablet_metadata_page(tablet_id, version, path, fs, _limits);
}

} // namespace starrocks::lake
