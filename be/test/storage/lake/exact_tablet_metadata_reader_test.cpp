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

#include <gtest/gtest.h>

#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/coding.h"
#include "base/testutil/assert.h"
#include "common/storage_define.h"
#include "fs/fs_memory.h"
#include "gen_cpp/lake_types.pb.h"
#include "io/seekable_input_stream.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/protobuf_file.h"
#include "storage/utils.h"

namespace starrocks::lake {
namespace {

struct ReadCounters {
    int iterate_dir_calls = 0;
    int read_all_calls = 0;
    int open_calls = 0;
    int64_t read_at_fully_bytes = 0;
    bool all_opens_skip_fill_local_cache = true;
    bool all_opens_skip_disk_cache = true;
};

class CountingInputStream final : public io::SeekableInputStreamWrapper {
public:
    CountingInputStream(std::shared_ptr<io::SeekableInputStream> stream, ReadCounters* counters)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
              _stream(std::move(stream)),
              _counters(counters) {}

    StatusOr<std::string> read_all() override {
        ++_counters->read_all_calls;
        return _stream->read_all();
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        RETURN_IF_ERROR(_stream->read_at_fully(offset, out, count));
        _counters->read_at_fully_bytes += count;
        return Status::OK();
    }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    ReadCounters* _counters;
};

class CountingMemoryFileSystem final : public MemoryFileSystem {
public:
    explicit CountingMemoryFileSystem(ReadCounters* counters) : _counters(counters) {}

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        ++_counters->open_calls;
        _counters->all_opens_skip_fill_local_cache &= opts.skip_fill_local_cache;
        _counters->all_opens_skip_disk_cache &= opts.skip_disk_cache;
        ASSIGN_OR_RETURN(auto file, MemoryFileSystem::new_random_access_file(opts, url));
        auto stream = std::make_shared<CountingInputStream>(file->stream(), _counters);
        return std::make_unique<RandomAccessFile>(std::move(stream), url);
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override {
        ++_counters->iterate_dir_calls;
        return MemoryFileSystem::iterate_dir(dir, cb);
    }

    Status iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) override {
        ++_counters->iterate_dir_calls;
        return MemoryFileSystem::iterate_dir2(dir, cb);
    }

private:
    ReadCounters* _counters;
};

TabletSchemaPB make_schema(int64_t schema_id, KeysType keys_type = DUP_KEYS) {
    TabletSchemaPB schema;
    schema.set_id(schema_id);
    schema.set_keys_type(keys_type);
    schema.set_num_short_key_columns(1);
    auto* column = schema.add_column();
    column->set_unique_id(0);
    column->set_name("c0");
    column->set_type("INT");
    column->set_is_key(true);
    column->set_is_nullable(false);
    return schema;
}

TabletMetadataPB make_metadata(int64_t tablet_id, int64_t version, int64_t schema_id = 700,
                               KeysType keys_type = DUP_KEYS) {
    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(version);
    metadata.mutable_schema()->CopyFrom(make_schema(schema_id, keys_type));
    return metadata;
}

std::string serialize(const google::protobuf::Message& message) {
    std::string result;
    EXPECT_TRUE(message.SerializeToString(&result));
    return result;
}

std::string encode_bundle(const std::vector<TabletMetadataPB>& input, bool checksummed_footer, bool checksummed_pages,
                          bool include_index = true, bool include_tablet_schema = true,
                          bool include_current_schema = true, bool include_historical_schemas = true,
                          bool overlap_footer = false) {
    BundleTabletMetadataPB footer;
    std::string content;
    for (auto metadata : input) {
        const int64_t tablet_id = metadata.id();
        const int64_t current_schema_id = metadata.schema().id();
        if (include_tablet_schema) {
            (*footer.mutable_tablet_to_schema())[tablet_id] = current_schema_id;
        }
        if (include_current_schema) {
            (*footer.mutable_schemas())[current_schema_id].CopyFrom(metadata.schema());
        }
        if (include_historical_schemas) {
            for (const auto& [schema_id, schema] : metadata.historical_schemas()) {
                (*footer.mutable_schemas())[schema_id].CopyFrom(schema);
            }
        }
        metadata.clear_schema();
        metadata.clear_historical_schemas();
        const std::string page = serialize(metadata);
        if (include_index) {
            auto& pointer = (*footer.mutable_tablet_meta_pages())[tablet_id];
            pointer.set_offset(content.size());
            pointer.set_size(page.size());
        }
        if (checksummed_pages) {
            (*footer.mutable_tablet_meta_page_checksum())[tablet_id] =
                    olap_adler32(ADLER32_INIT, page.data(), page.size());
        }
        content.append(page);
    }

    if (overlap_footer && !input.empty()) {
        auto& pointer = (*footer.mutable_tablet_meta_pages())[input.front().id()];
        pointer.set_offset(content.size());
        pointer.set_size(1);
    }

    const std::string serialized_footer = serialize(footer);
    content.append(serialized_footer);
    uint64_t size_field = serialized_footer.size();
    if (checksummed_footer) {
        put_fixed32_le(&content, olap_adler32(ADLER32_INIT, serialized_footer.data(), serialized_footer.size()));
        size_field |= LAKE_BUNDLE_META_CHECKSUM_FLAG;
    }
    put_fixed64_le(&content, size_field);
    return content;
}

class ExactTabletMetadataReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fs = std::make_shared<CountingMemoryFileSystem>(&_counters);
        _provider = std::make_shared<FixedLocationProvider>("/exact-reader");
        ASSERT_OK(_fs->create_dir_recursive("/exact-reader/meta"));
    }

    ExactTabletMetadataReader reader(uint64_t max_metadata_bytes = 1024 * 1024,
                                     uint64_t max_bundle_footer_bytes = 1024 * 1024) const {
        return ExactTabletMetadataReader(_provider, {max_metadata_bytes, max_bundle_footer_bytes}, _fs);
    }

    void write_bytes(const std::string& path, const std::string& content) {
        ASSERT_OK(_fs->create_file(path));
        ASSERT_OK(_fs->append_file(path, Slice(content)));
    }

    void overwrite_bytes(const std::string& path, const std::string& content) {
        (void)_fs->delete_file(path);
        write_bytes(path, content);
    }

    void write_plain_metadata(const TabletMetadataPB& metadata, int64_t path_tablet_id, int64_t path_version) {
        write_bytes(_provider->tablet_metadata_location(path_tablet_id, path_version), serialize(metadata));
    }

    void write_checked_metadata(const TabletMetadataPB& metadata, int64_t path_tablet_id, int64_t path_version) {
        ProtobufFileWithHeader file(_provider->tablet_metadata_location(path_tablet_id, path_version), _fs,
                                    LAKE_META_HEADER_MAGIC_NUMBER, /*allow_plain_protobuf_fallback=*/true);
        ASSERT_OK(file.save(metadata, true));
    }

    void write_bundle(const std::string& content, int64_t path_tablet_id, int64_t path_version) {
        write_bytes(_provider->bundle_tablet_metadata_location(path_tablet_id, path_version), content);
    }

    void expect_exact_io_policy() const {
        EXPECT_EQ(0, _counters.iterate_dir_calls);
        EXPECT_EQ(0, _counters.read_all_calls);
        EXPECT_TRUE(_counters.all_opens_skip_fill_local_cache);
        EXPECT_TRUE(_counters.all_opens_skip_disk_cache);
    }

    ReadCounters _counters;
    std::shared_ptr<CountingMemoryFileSystem> _fs;
    std::shared_ptr<FixedLocationProvider> _provider;
};

TEST_F(ExactTabletMetadataReaderTest, reads_checksummed_and_legacy_standalone_objects_exactly) {
    write_checked_metadata(make_metadata(101, 2), 101, 2);
    write_plain_metadata(make_metadata(102, 3), 102, 3);

    auto checked = reader().read(101, 2, TabletMetadataStorageFormat::kStandalone);
    ASSERT_OK(checked.status());
    EXPECT_EQ(101, checked.value()->id());
    EXPECT_EQ(2, checked.value()->version());
    auto legacy = reader().read(102, 3, TabletMetadataStorageFormat::kStandalone);
    ASSERT_OK(legacy.status());
    EXPECT_EQ(102, legacy.value()->id());
    EXPECT_EQ(3, legacy.value()->version());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, version_one_prefers_per_tablet_then_remaps_shared_zero_id) {
    write_plain_metadata(make_metadata(111, 1), 111, 1);
    write_plain_metadata(make_metadata(777, 1), 0, 1);

    auto per_tablet = reader().read(111, 1, TabletMetadataStorageFormat::kStandalone);
    ASSERT_OK(per_tablet.status());
    EXPECT_EQ(111, per_tablet.value()->id());
    EXPECT_EQ(1, per_tablet.value()->version());

    auto shared = reader().read(112, 1, TabletMetadataStorageFormat::kStandalone);
    ASSERT_OK(shared.status());
    EXPECT_EQ(112, shared.value()->id());
    EXPECT_EQ(1, shared.value()->version());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, rejects_nonpositive_identity_and_bundle_version_one) {
    EXPECT_TRUE(reader().read(0, 2, TabletMetadataStorageFormat::kStandalone).status().is_invalid_argument());
    EXPECT_TRUE(reader().read(-1, 2, TabletMetadataStorageFormat::kStandalone).status().is_invalid_argument());
    EXPECT_TRUE(reader().read(121, 0, TabletMetadataStorageFormat::kStandalone).status().is_invalid_argument());
    EXPECT_TRUE(reader().read(121, -1, TabletMetadataStorageFormat::kStandalone).status().is_invalid_argument());
    EXPECT_TRUE(reader().read(121, 1, TabletMetadataStorageFormat::kBundle).status().is_invalid_argument());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, wrong_format_does_not_fall_back) {
    write_plain_metadata(make_metadata(131, 2), 131, 2);
    EXPECT_TRUE(reader().read(131, 2, TabletMetadataStorageFormat::kBundle).status().is_not_found());

    ASSERT_OK(_fs->delete_file(_provider->tablet_metadata_location(131, 2)));
    write_bundle(encode_bundle({make_metadata(131, 2)}, true, true), 131, 2);
    EXPECT_TRUE(reader().read(131, 2, TabletMetadataStorageFormat::kStandalone).status().is_not_found());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, enforces_standalone_object_and_bundle_page_and_footer_limits) {
    auto standalone = make_metadata(141, 2);
    const auto standalone_content = serialize(standalone);
    write_bytes(_provider->tablet_metadata_location(141, 2), standalone_content);
    ASSERT_OK(reader(standalone_content.size(), 1024).read(141, 2, TabletMetadataStorageFormat::kStandalone).status());
    EXPECT_TRUE(reader(standalone_content.size() - 1, 1024)
                        .read(141, 2, TabletMetadataStorageFormat::kStandalone)
                        .status()
                        .is_capacity_limit_exceeded());

    auto bundled_metadata = make_metadata(142, 2);
    auto stripped_page = bundled_metadata;
    stripped_page.clear_schema();
    stripped_page.clear_historical_schemas();
    const uint64_t page_size = serialize(stripped_page).size();
    const auto bundle = encode_bundle({bundled_metadata}, true, true);
    const uint64_t raw_footer_size =
            decode_fixed64_le(reinterpret_cast<const uint8_t*>(bundle.data() + bundle.size() - sizeof(uint64_t)));
    const uint64_t footer_size = raw_footer_size & ~LAKE_BUNDLE_META_CHECKSUM_FLAG;
    write_bundle(bundle, 142, 2);
    ASSERT_OK(reader(page_size, footer_size).read(142, 2, TabletMetadataStorageFormat::kBundle).status());
    EXPECT_TRUE(reader(page_size - 1, footer_size)
                        .read(142, 2, TabletMetadataStorageFormat::kBundle)
                        .status()
                        .is_capacity_limit_exceeded());
    EXPECT_TRUE(reader(page_size, footer_size - 1)
                        .read(142, 2, TabletMetadataStorageFormat::kBundle)
                        .status()
                        .is_capacity_limit_exceeded());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, rejects_id_and_version_mismatches_in_both_formats) {
    write_plain_metadata(make_metadata(999, 2), 151, 2);
    EXPECT_TRUE(reader().read(151, 2, TabletMetadataStorageFormat::kStandalone).status().is_corruption());
    overwrite_bytes(_provider->tablet_metadata_location(151, 2), serialize(make_metadata(151, 999)));
    EXPECT_TRUE(reader().read(151, 2, TabletMetadataStorageFormat::kStandalone).status().is_corruption());

    write_bundle(encode_bundle({make_metadata(152, 999)}, true, true), 152, 2);
    EXPECT_TRUE(reader().read(152, 2, TabletMetadataStorageFormat::kBundle).status().is_corruption());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, validates_shared_v1_before_logical_id_remapping) {
    write_plain_metadata(make_metadata(0, 9), 0, 1);
    EXPECT_TRUE(reader().read(161, 1, TabletMetadataStorageFormat::kStandalone).status().is_corruption());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, reads_checked_and_legacy_bundle_footers) {
    write_bundle(encode_bundle({make_metadata(171, 2)}, true, true), 171, 2);
    write_bundle(encode_bundle({make_metadata(172, 3)}, false, false), 172, 3);

    auto checked = reader().read(171, 2, TabletMetadataStorageFormat::kBundle);
    ASSERT_OK(checked.status());
    EXPECT_EQ(171, checked.value()->id());
    EXPECT_EQ(2, checked.value()->version());
    auto legacy = reader().read(172, 3, TabletMetadataStorageFormat::kBundle);
    ASSERT_OK(legacy.status());
    EXPECT_EQ(172, legacy.value()->id());
    EXPECT_EQ(3, legacy.value()->version());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, reads_only_the_requested_bundle_page) {
    auto large = make_metadata(182, 2);
    auto* rowset = large.add_rowsets();
    rowset->set_id(7);
    for (int i = 0; i < 200; ++i) {
        rowset->add_deprecated_segments(std::string(100, 'x') + std::to_string(i));
    }
    const auto bundle = encode_bundle({make_metadata(181, 2), large}, true, true);
    write_bundle(bundle, 181, 2);

    auto result = reader().read(181, 2, TabletMetadataStorageFormat::kBundle);
    ASSERT_OK(result.status());
    EXPECT_EQ(181, result.value()->id());
    EXPECT_EQ(2, result.value()->version());
    EXPECT_GT(_counters.read_at_fully_bytes, 0);
    EXPECT_LT(_counters.read_at_fully_bytes, bundle.size());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, rejects_bundle_page_checksum_and_footer_overlap) {
    auto corrupt_page = encode_bundle({make_metadata(191, 2)}, true, true);
    corrupt_page[0] ^= 0x7f;
    write_bundle(corrupt_page, 191, 2);
    EXPECT_TRUE(reader().read(191, 2, TabletMetadataStorageFormat::kBundle).status().is_corruption());

    write_bundle(encode_bundle({make_metadata(192, 2)}, true, false, true, true, true, true, true), 192, 2);
    EXPECT_TRUE(reader().read(192, 2, TabletMetadataStorageFormat::kBundle).status().is_corruption());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, reports_missing_tablet_index) {
    write_bundle(encode_bundle({make_metadata(202, 2)}, true, true), 201, 2);
    EXPECT_TRUE(reader().read(201, 2, TabletMetadataStorageFormat::kBundle).status().is_not_found());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, rejects_missing_current_or_historical_schemas) {
    auto metadata = make_metadata(211, 2, 700);
    (*metadata.mutable_historical_schemas())[600].CopyFrom(make_schema(600));
    (*metadata.mutable_rowset_to_schema())[7] = 600;
    const auto path = _provider->bundle_tablet_metadata_location(211, 2);

    write_bundle(encode_bundle({metadata}, true, true, true, false), 211, 2);
    EXPECT_TRUE(reader().read(211, 2, TabletMetadataStorageFormat::kBundle).status().is_corruption());
    overwrite_bytes(path, encode_bundle({metadata}, true, true, true, true, false));
    EXPECT_TRUE(reader().read(211, 2, TabletMetadataStorageFormat::kBundle).status().is_corruption());
    overwrite_bytes(path, encode_bundle({metadata}, true, true, true, true, true, false));
    EXPECT_TRUE(reader().read(211, 2, TabletMetadataStorageFormat::kBundle).status().is_corruption());
    expect_exact_io_policy();
}

TEST_F(ExactTabletMetadataReaderTest, normalizes_and_restores_current_and_historical_schemas) {
    auto metadata = make_metadata(221, 2, 700, PRIMARY_KEYS);
    auto* rowset = metadata.add_rowsets();
    rowset->set_id(7);
    rowset->add_deprecated_segments("segment-a");
    (*metadata.mutable_historical_schemas())[600].CopyFrom(make_schema(600));
    (*metadata.mutable_rowset_to_schema())[7] = 600;
    write_bundle(encode_bundle({metadata}, true, true), 221, 2);

    auto result = reader().read(221, 2, TabletMetadataStorageFormat::kBundle);
    ASSERT_OK(result.status());
    EXPECT_EQ(221, result.value()->id());
    EXPECT_EQ(2, result.value()->version());
    EXPECT_EQ(700, result.value()->schema().id());
    EXPECT_EQ(700, result.value()->historical_schemas().at(700).id());
    EXPECT_EQ(600, result.value()->historical_schemas().at(600).id());
    ASSERT_EQ(1, result.value()->rowsets(0).segment_metas_size());
    EXPECT_EQ("segment-a", result.value()->rowsets(0).segment_metas(0).filename());
    EXPECT_TRUE(result.value()->enable_persistent_index());
    EXPECT_EQ(CLOUD_NATIVE, result.value()->persistent_index_type());
    expect_exact_io_policy();
}

} // namespace
} // namespace starrocks::lake
