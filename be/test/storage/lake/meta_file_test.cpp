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

#include "storage/lake/meta_file.h"

#include <gtest/gtest.h>

#include <ctime>
#include <functional>
#include <limits>
#include <set>
#include <unordered_map>

#include "base/hash/crc32c.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "common/config_lake_fwd.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/del_vector.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_reshard.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/storage_metrics.h"

namespace starrocks::lake {

class TestLocationProvider : public LocationProvider {
public:
    explicit TestLocationProvider(LocationProvider* lp) : _lp(lp) {}

    std::string root_location(int64_t tablet_id) const override {
        if (_owned_shards.count(tablet_id) > 0) {
            return _lp->root_location(tablet_id);
        } else {
            return "/path/to/nonexist/directory/";
        }
    }

    std::set<int64_t> _owned_shards;
    LocationProvider* _lp;
};

class MetaFileTest : public ::testing::Test {
public:
    void SetUp() {
        CHECK_OK(fs::create_directories(join_path(kTestDir, kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kTxnLogDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kSegmentDirectoryName)));

        _location_provider = std::make_unique<FixedLocationProvider>(kTestDir);
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 1638400000);
    }

    void TearDown() { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    void write_file(const std::string& path, const std::string& content) {
        WritableFileOptions options{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_ABORT(auto writer, fs::new_writable_file(options, path));
        ASSERT_OK(writer->append(Slice(content)));
        ASSERT_OK(writer->close());
    }

    DelvecPageInfo add_test_delvec(TabletMetadataPB* metadata, int64_t tablet_id, int64_t version, uint32_t segment_id,
                                   const std::string& filename, const std::string& content) {
        FileMetaPB file_meta;
        file_meta.set_name(filename);
        file_meta.set_size(content.size());
        (*metadata->mutable_delvec_meta()->mutable_version_to_file())[version] = file_meta;

        DelvecPagePB page;
        page.set_version(version);
        page.set_offset(0);
        page.set_size(content.size());
        (*metadata->mutable_delvec_meta()->mutable_delvecs())[segment_id] = page;
        write_file(_tablet_manager->delvec_location(tablet_id, filename), content);
        return DelvecPageInfo{tablet_id, std::move(file_meta), std::move(page)};
    }

    DelvecOutputPage raw_output_page(int64_t tablet_id, std::string filename, uint64_t offset, uint64_t size,
                                     std::optional<int64_t> declared_size = std::nullopt) {
        DelvecPageInfo raw;
        raw.tablet_id = tablet_id;
        raw.delvec_file.set_name(std::move(filename));
        if (declared_size.has_value()) {
            raw.delvec_file.set_size(*declared_size);
        }
        raw.page.set_version(1);
        raw.page.set_offset(offset);
        raw.page.set_size(size);
        return DelvecOutputPage{.raw_page = std::move(raw)};
    }

    enum class RejectionStatus { kInvalidArgument, kNotSupported, kCorruption };

    void expect_compacted_delvec_rejected(const std::vector<DelvecOutputPage>& pages, RejectionStatus expected_status,
                                          std::string_view expected_message, int expected_preflight_opens = 0,
                                          int expected_source_sizes = 0,
                                          const std::function<void(SyncPoint*)>& configure = {}) {
        SCOPED_TRACE(expected_message);
        FileMetaPB output;
        output.set_name("unchanged.delvec");
        output.set_size(123);
        output.set_shared(true);
        output.set_encryption_meta("unchanged-encryption");
        std::vector<uint64_t> offsets = {7, 11};
        const std::string before_file = output.SerializeAsString();
        const std::vector<uint64_t> before_offsets = offsets;

        int preflight_opens = 0;
        int source_sizes = 0;
        int get_del_vec_calls = 0;
        int range_reads = 0;
        int writer_opens = 0;
        int append_chunks = 0;
        auto* sync = SyncPoint::GetInstance();
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
        sync->SetCallBack("write_compacted_delvec_pages:preflight_source_open", [&](void*) { ++preflight_opens; });
        sync->SetCallBack("write_compacted_delvec_pages:source_size", [&](void*) { ++source_sizes; });
        sync->SetCallBack("merge_delvecs:before_get_del_vec", [&](void*) { ++get_del_vec_calls; });
        sync->SetCallBack("write_compacted_delvec_pages:read_chunk_size", [&](void*) { ++range_reads; });
        sync->SetCallBack("write_compacted_delvec_pages:writer_open", [&](void*) { ++writer_opens; });
        sync->SetCallBack("append_delvec_bytes_bounded:chunk_size", [&](void*) { ++append_chunks; });
        if (configure) configure(sync);
        sync->EnableProcessing();
        DeferOp cleanup([&] {
            sync->ClearAllCallBacks();
            sync->DisableProcessing();
        });

        const Status status =
                write_compacted_delvec_pages(_tablet_manager.get(), pages, next_id(), next_id(), &output, &offsets);
        switch (expected_status) {
        case RejectionStatus::kInvalidArgument:
            EXPECT_TRUE(status.is_invalid_argument()) << status;
            break;
        case RejectionStatus::kNotSupported:
            EXPECT_TRUE(status.is_not_supported()) << status;
            break;
        case RejectionStatus::kCorruption:
            EXPECT_TRUE(status.is_corruption()) << status;
            break;
        }
        EXPECT_EQ(expected_message, status.message()) << status;
        EXPECT_EQ(before_file, output.SerializeAsString());
        EXPECT_EQ(before_offsets, offsets);
        EXPECT_EQ(expected_preflight_opens, preflight_opens);
        EXPECT_EQ(expected_source_sizes, source_sizes);
        EXPECT_EQ(0, get_del_vec_calls);
        EXPECT_EQ(0, range_reads);
        EXPECT_EQ(0, writer_opens);
        EXPECT_EQ(0, append_chunks);
    }

    std::shared_ptr<TabletMetadataPB> make_shared_delvec_source(int64_t tablet_id, int segment_count) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        metadata->set_next_rowset_id(segment_count + 1);
        metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);
        metadata->mutable_schema()->set_id(1001);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10 * segment_count);
        rowset->set_data_size(100 * segment_count);
        rowset->mutable_uid()->set_hi(0x1234);
        rowset->mutable_uid()->set_lo(0x5678);
        for (int i = 0; i < segment_count; ++i) {
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(fmt::format("atomic_shared_{}.dat", i));
            segment->set_size(100);
            segment->set_shared(true);
        }
        return metadata;
    }

    Status publish_delvec_merge(const std::vector<TabletMetadataPtr>& sources, int64_t merged_tablet, int64_t txn_id,
                                std::unordered_map<int64_t, TabletMetadataPtr>* published_metadatas) {
        for (const auto& source : sources) {
            RETURN_IF_ERROR(_tablet_manager->put_tablet_metadata(source));
        }
        ReshardingTabletInfoPB resharding;
        auto& merging = *resharding.mutable_merging_tablet_info();
        for (const auto& source : sources) {
            merging.add_old_tablet_ids(source->id());
        }
        merging.set_new_tablet_id(merged_tablet);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        return lake::publish_resharding_tablet(_tablet_manager.get(), resharding, /*base_version=*/1,
                                               /*new_version=*/2, txn_info, false, *published_metadatas, tablet_ranges);
    }

    StatusOr<std::set<std::string>> delvec_inventory() {
        std::set<std::string> files;
        RETURN_IF_ERROR(FileSystem::Default()->iterate_dir(
                _location_provider->segment_root_location(1), [&](std::string_view filename) {
                    constexpr std::string_view kSuffix = ".delvec";
                    if (filename.size() >= kSuffix.size() &&
                        filename.substr(filename.size() - kSuffix.size()) == kSuffix) {
                        files.emplace(filename);
                    }
                    return true;
                }));
        return files;
    }

    constexpr static const char* const kTestDir = "./lake_meta_test";
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<UpdateManager> _update_manager;
};

TEST_F(MetaFileTest, test_meta_rw) {
    // 1. generate metadata
    const int64_t tablet_id = 10001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);

    // 2. write to pk meta file
    MetaFileBuilder builder(*tablet, metadata);
    Status st = builder.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 3. read meta from meta file
    ASSIGN_OR_ABORT(auto metadata2, _tablet_manager->get_tablet_metadata(tablet_id, 10));
}

// Regression: when add_rowset accumulates multiple op_writes into ONE composite rowset
// (a multi-statement / batch / cross-publish PK txn), it must SUM num_rows/data_size/num_dels,
// not keep only the first op_write's counts. The bug left e.g. num_rows=1 on a rowset whose
// segments hold 1 + 10000 = 10001 rows, corrupting reads until compaction rewrote the rowset.
TEST_F(MetaFileTest, test_add_rowset_sums_composite_stats) {
    const int64_t tablet_id = 10050;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(2);
    metadata->set_next_rowset_id(1);
    MetaFileBuilder builder(*tablet, metadata);

    RowsetMetadataPB rs_tiny; // first op_write: 1 row, 1 segment
    rs_tiny.set_num_rows(1);
    rs_tiny.set_data_size(100);
    rs_tiny.set_num_dels(2);
    rs_tiny.set_overlapped(false);
    {
        auto* segment_meta = rs_tiny.add_segment_metas();
        segment_meta->set_filename("seg_tiny.dat");
        segment_meta->set_size(100);
    }
    builder.add_rowset(rs_tiny, {}, {}, {}, {}, {});

    RowsetMetadataPB rs_large; // second op_write: 10000 rows, 1 segment
    rs_large.set_num_rows(10000);
    rs_large.set_data_size(50000);
    rs_large.set_num_dels(3);
    rs_large.set_overlapped(false);
    {
        auto* segment_meta = rs_large.add_segment_metas();
        segment_meta->set_filename("seg_large.dat");
        segment_meta->set_size(50000);
    }
    builder.add_rowset(rs_large, {}, {}, {}, {}, {});

    ASSERT_OK(builder.set_final_rowset());

    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& rs = metadata->rowsets(0);
    EXPECT_EQ(2, rs.segment_metas_size());
    EXPECT_TRUE(rs.segment_metas(0).has_size());
    EXPECT_TRUE(rs.segment_metas(1).has_size());
    EXPECT_EQ(1 + 10000, rs.num_rows());    // before the fix: 1 (first op_write only)
    EXPECT_EQ(100 + 50000, rs.data_size()); // before the fix: 100
    EXPECT_EQ(2 + 3, rs.num_dels());        // before the fix: 2 (first op_write only)
    EXPECT_TRUE(rs.overlapped());           // composite spanning >1 op_write
}

TEST_F(MetaFileTest, test_compacted_delvec_empty_plan_rejected) {
    expect_compacted_delvec_rejected({}, RejectionStatus::kInvalidArgument, "compacted delvec page plan is empty");
}

TEST_F(MetaFileTest, test_compacted_delvec_plan_shapes_and_filename) {
    expect_compacted_delvec_rejected({}, RejectionStatus::kInvalidArgument, "compacted delvec page plan is empty");
    expect_compacted_delvec_rejected({DelvecOutputPage{}}, RejectionStatus::kInvalidArgument,
                                     "compacted delvec output page must contain exactly one payload");
    auto both = raw_output_page(next_id(), "both.delvec", 0, 1, 1);
    both.serialized_page = "x";
    expect_compacted_delvec_rejected({both}, RejectionStatus::kInvalidArgument,
                                     "compacted delvec output page must contain exactly one payload");
    expect_compacted_delvec_rejected({raw_output_page(next_id(), "", 0, 1, 1)}, RejectionStatus::kInvalidArgument,
                                     "compacted delvec raw page filename is empty");
}

TEST_F(MetaFileTest, test_compacted_delvec_signed_and_file_domains) {
    expect_compacted_delvec_rejected({raw_output_page(next_id(), "zero.delvec", 0, 0, 0)},
                                     RejectionStatus::kInvalidArgument,
                                     "compacted delvec raw page size must be positive");
    expect_compacted_delvec_rejected({raw_output_page(next_id(), "negative.delvec", 0, 1, -1)},
                                     RejectionStatus::kInvalidArgument,
                                     "compacted delvec declared source size is negative");
    expect_compacted_delvec_rejected(
            {raw_output_page(next_id(), "offset.delvec", uint64_t{std::numeric_limits<int64_t>::max()} + 1, 1,
                             std::numeric_limits<int64_t>::max())},
            RejectionStatus::kInvalidArgument, "compacted delvec raw page offset is outside signed int64 domain");
    expect_compacted_delvec_rejected(
            {raw_output_page(next_id(), "size.delvec", 0, uint64_t{std::numeric_limits<int64_t>::max()} + 1,
                             std::numeric_limits<int64_t>::max())},
            RejectionStatus::kInvalidArgument, "compacted delvec raw page size is outside signed int64 domain");
    expect_compacted_delvec_rejected({raw_output_page(next_id(), "end.delvec", std::numeric_limits<int64_t>::max(), 1,
                                                      std::numeric_limits<int64_t>::max())},
                                     RejectionStatus::kInvalidArgument,
                                     "compacted delvec raw page end is outside signed int64 domain");
    expect_compacted_delvec_rejected({raw_output_page(next_id(), "undersize.delvec", 2, 3, 4)},
                                     RejectionStatus::kInvalidArgument,
                                     "compacted delvec declared source size does not contain page");

    // The first entry ends exactly at INT64_MAX and is valid. The following malformed entry
    // supplies a zero-I/O stopping point, proving the near-limit declaration passed preflight.
    expect_compacted_delvec_rejected(
            {raw_output_page(next_id(), "near-limit.delvec", std::numeric_limits<int64_t>::max() - 7, 7,
                             std::numeric_limits<int64_t>::max()),
             DelvecOutputPage{}},
            RejectionStatus::kInvalidArgument, "compacted delvec output page must contain exactly one payload");
}

TEST_F(MetaFileTest, test_compacted_delvec_output_overflow) {
    auto configure_overflow = [](uint64_t initial, bool skip_int64) {
        return [=](SyncPoint* sync) {
            sync->SetCallBack("write_compacted_delvec_pages:initial_output_offset",
                              [=](void* arg) { *static_cast<uint64_t*>(arg) = initial; });
            sync->SetCallBack("write_compacted_delvec_pages:test_skip_int64_output_limit",
                              [=](void* arg) { *static_cast<bool*>(arg) = skip_int64; });
        };
    };
    expect_compacted_delvec_rejected({DelvecOutputPage{.serialized_page = "xx"}}, RejectionStatus::kInvalidArgument,
                                     "compacted delvec output size is outside signed int64 domain", 0, 0,
                                     configure_overflow(std::numeric_limits<int64_t>::max(), false));
    expect_compacted_delvec_rejected({DelvecOutputPage{.serialized_page = "xx"}}, RejectionStatus::kInvalidArgument,
                                     "compacted delvec output size overflows uint64", 0, 0,
                                     configure_overflow(std::numeric_limits<uint64_t>::max() - 1, true));
}

TEST_F(MetaFileTest, test_compacted_delvec_duplicate_declarations) {
    const int64_t source_tablet = next_id();
    const int64_t target_tablet = next_id();
    const std::string filename = "duplicate-page.delvec";
    write_file(_tablet_manager->delvec_location(source_tablet, filename), "abcd");

    auto raw = raw_output_page(source_tablet, filename, 0, 4, 4);
    FileMetaPB output;
    std::vector<uint64_t> offsets;
    ASSERT_OK(write_compacted_delvec_pages(_tablet_manager.get(), {raw, raw}, target_tablet, next_id(), &output,
                                           &offsets));
    EXPECT_EQ(std::vector<uint64_t>({0, 4}), offsets);
    ASSIGN_OR_ABORT(auto reader,
                    fs::new_random_access_file(_tablet_manager->delvec_location(target_tablet, output.name())));
    ASSIGN_OR_ABORT(auto copied, reader->read_all());
    EXPECT_EQ("abcdabcd", copied);

    auto conflicting = raw_output_page(source_tablet, filename, 0, 3, 4);
    expect_compacted_delvec_rejected({raw, conflicting}, RejectionStatus::kCorruption,
                                     "compacted delvec duplicate page declarations disagree on size");
}

TEST_F(MetaFileTest, test_compacted_delvec_plaintext_guard_raw_and_mixed) {
    auto encrypted = raw_output_page(next_id(), "encrypted.delvec", 0, 1, 1);
    encrypted.raw_page->delvec_file.set_encryption_meta("stale");
    for (const auto& pages : std::vector<std::vector<DelvecOutputPage>>{
                 {encrypted}, {encrypted, DelvecOutputPage{.serialized_page = "x"}}}) {
        expect_compacted_delvec_rejected(
                pages, RejectionStatus::kNotSupported,
                "encrypted delvec input is unsupported; delvec must be plaintext: encrypted.delvec");
    }
}

TEST_F(MetaFileTest, test_compacted_delvec_absent_size_cache_and_reader_lifecycle) {
    const int64_t source_a = next_id();
    const int64_t source_b = next_id();
    const int64_t source_same_name_other_tablet = next_id();
    write_file(_tablet_manager->delvec_location(source_a, "a.delvec"), "a");
    write_file(_tablet_manager->delvec_location(source_b, "b.delvec"), "b");
    write_file(_tablet_manager->delvec_location(source_same_name_other_tablet, "a.delvec"), "a");
    const auto a = raw_output_page(source_a, "a.delvec", 0, 1);
    const auto b = raw_output_page(source_b, "b.delvec", 0, 1);
    const auto same_name_other_tablet = raw_output_page(source_same_name_other_tablet, "a.delvec", 0, 1);
    {
        int active_readers = 0;
        int peak_readers = 0;
        int copy_opens = 0;
        int source_size_lookups = 0;
        auto* sync = SyncPoint::GetInstance();
        sync->SetCallBack("write_compacted_delvec_pages:copy_source_reader_delta", [&](void* arg) {
            active_readers += *static_cast<int*>(arg);
            peak_readers = std::max(peak_readers, active_readers);
            if (*static_cast<int*>(arg) == 1) ++copy_opens;
        });
        sync->SetCallBack("write_compacted_delvec_pages:source_size", [&](void*) { ++source_size_lookups; });
        sync->SetCallBack("write_compacted_delvec_pages:source_options", [&](void* arg) {
            EXPECT_TRUE(static_cast<RandomAccessFileOptions*>(arg)->skip_fill_local_cache);
        });
        sync->EnableProcessing();
        DeferOp cleanup([&] {
            sync->ClearAllCallBacks();
            sync->DisableProcessing();
        });
        FileMetaPB output;
        std::vector<uint64_t> offsets;
        ASSERT_OK(write_compacted_delvec_pages(_tablet_manager.get(), {a, a, b, a}, next_id(), next_id(), &output,
                                               &offsets));
        EXPECT_EQ(2, source_size_lookups);
        EXPECT_EQ(3, copy_opens);
        EXPECT_EQ(1, peak_readers);
        EXPECT_EQ(0, active_readers);

        source_size_lookups = 0;
        ASSERT_OK(write_compacted_delvec_pages(_tablet_manager.get(), {a, same_name_other_tablet}, next_id(), next_id(),
                                               &output, &offsets));
        EXPECT_EQ(2, source_size_lookups);
    }

    auto configure_resolved_size = [](int64_t resolved_size) {
        return [=](SyncPoint* sync) {
            sync->SetCallBack("write_compacted_delvec_pages:source_size_override", [resolved_size](void* arg) {
                *static_cast<std::optional<StatusOr<int64_t>>*>(arg) = resolved_size;
            });
        };
    };
    expect_compacted_delvec_rejected({a}, RejectionStatus::kInvalidArgument,
                                     "compacted delvec resolved source size is negative", 1, 1,
                                     configure_resolved_size(-1));
    expect_compacted_delvec_rejected({a}, RejectionStatus::kInvalidArgument,
                                     "compacted delvec resolved source size does not contain page", 1, 1,
                                     configure_resolved_size(0));
}

TEST_F(MetaFileTest, test_compacted_delvec_reader_lifecycle_resets_across_serialized_page) {
    const int64_t source_tablet = next_id();
    const int64_t target_tablet = next_id();
    constexpr std::string_view source_name = "reader-lifecycle-source.delvec";

    DelVector raw_delvec;
    const uint32_t raw_deleted[] = {2, 7};
    raw_delvec.init(/*version=*/3, raw_deleted, std::size(raw_deleted));
    const std::string raw_bytes = raw_delvec.save();
    write_file(_tablet_manager->delvec_location(source_tablet, std::string(source_name)), raw_bytes);

    DelVector serialized_delvec;
    const uint32_t serialized_deleted[] = {1, 9, 14};
    serialized_delvec.init(/*version=*/4, serialized_deleted, std::size(serialized_deleted));
    const std::string serialized_bytes = serialized_delvec.save();
    const auto raw = raw_output_page(source_tablet, std::string(source_name), 0, raw_bytes.size());

    int active_readers = 0;
    int peak_readers = 0;
    int copy_opens = 0;
    int source_size_lookups = 0;
    int source_option_uses = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->ClearAllCallBacks();
    sync->DisableProcessing();
    sync->SetCallBack("write_compacted_delvec_pages:copy_source_reader_delta", [&](void* arg) {
        const int delta = *static_cast<int*>(arg);
        active_readers += delta;
        peak_readers = std::max(peak_readers, active_readers);
        if (delta == 1) ++copy_opens;
    });
    sync->SetCallBack("write_compacted_delvec_pages:source_size", [&](void*) { ++source_size_lookups; });
    sync->SetCallBack("write_compacted_delvec_pages:source_options", [&](void* arg) {
        ++source_option_uses;
        EXPECT_TRUE(static_cast<RandomAccessFileOptions*>(arg)->skip_fill_local_cache);
    });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });

    FileMetaPB output;
    std::vector<uint64_t> offsets;
    ASSERT_OK(write_compacted_delvec_pages(_tablet_manager.get(),
                                           {raw, DelvecOutputPage{.serialized_page = serialized_bytes}, raw},
                                           target_tablet, next_id(), &output, &offsets));
    EXPECT_EQ(std::vector<uint64_t>({0, raw_bytes.size(), raw_bytes.size() + serialized_bytes.size()}), offsets);
    ASSIGN_OR_ABORT(auto reader,
                    fs::new_random_access_file(_tablet_manager->delvec_location(target_tablet, output.name())));
    ASSIGN_OR_ABORT(auto actual, reader->read_all());
    EXPECT_EQ(raw_bytes + serialized_bytes + raw_bytes, actual);
    EXPECT_EQ(static_cast<int64_t>(actual.size()), output.size());
    EXPECT_EQ(1, source_size_lookups);
    EXPECT_EQ(3, source_option_uses);
    EXPECT_EQ(2, copy_opens);
    EXPECT_EQ(1, peak_readers);
    EXPECT_EQ(0, active_readers);
}

TEST_F(MetaFileTest, test_compacted_delvec_serialized_output_uses_bounded_appends) {
    const std::string expected(3 * (1UL << 20) + 17, 's');
    const int64_t target_tablet = next_id();
    std::vector<size_t> append_sizes;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("append_delvec_bytes_bounded:chunk_size",
                      [&](void* arg) { append_sizes.push_back(*static_cast<size_t*>(arg)); });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    FileMetaPB output;
    std::vector<uint64_t> offsets;
    ASSERT_OK(write_compacted_delvec_pages(_tablet_manager.get(), {DelvecOutputPage{.serialized_page = expected}},
                                           target_tablet, next_id(), &output, &offsets));
    ASSERT_GT(append_sizes.size(), 1);
    for (size_t size : append_sizes) EXPECT_LE(size, 1UL << 20);
    ASSIGN_OR_ABORT(auto input,
                    fs::new_random_access_file(_tablet_manager->delvec_location(target_tablet, output.name())));
    ASSIGN_OR_ABORT(auto actual, input->read_all());
    EXPECT_EQ(expected, actual);
}

TEST_F(MetaFileTest, test_finalize_delvec_uses_bounded_appends) {
    const int64_t tablet_id = next_id();
    const int64_t version = 2;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(1);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);
    MetaFileBuilder builder(*tablet, metadata);
    DelVector seed;
    const uint32_t deleted = 7;
    std::shared_ptr<DelVector> expected;
    seed.add_dels_as_new_version({deleted}, version, &expected);
    const std::string prefix = expected->save();
    const std::string padding(3 * (1UL << 20) + 17, 'p');
    const std::string expected_physical_bytes = prefix + padding;
    builder.append_delvec(expected, /*segment_id=*/1);

    std::vector<size_t> append_sizes;
    int64_t logical_size = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("MetaFileBuilder::_finalize_delvec", [&](void* arg) {
        auto* buffer = static_cast<Buffer<uint8_t>*>(arg);
        const size_t original_size = buffer->size();
        EXPECT_EQ(prefix.size(), original_size);
        buffer->resize(original_size + padding.size());
        std::fill(buffer->begin() + original_size, buffer->end(), static_cast<uint8_t>('p'));
    });
    sync->SetCallBack("MetaFileBuilder::_finalize_delvec:logical_append_size",
                      [&](void* arg) { logical_size = *static_cast<int64_t*>(arg); });
    sync->SetCallBack("append_delvec_bytes_bounded:chunk_size",
                      [&](void* arg) { append_sizes.push_back(*static_cast<size_t*>(arg)); });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    ASSERT_OK(builder.finalize(next_id()));
    ASSERT_GT(logical_size, 3 * (1UL << 20));
    ASSERT_GT(append_sizes.size(), 1);
    for (size_t size : append_sizes) EXPECT_LE(size, 1UL << 20);
    const auto& file = metadata->delvec_meta().version_to_file().at(version);
    EXPECT_EQ(logical_size, file.size());
    EXPECT_EQ(static_cast<int64_t>(expected_physical_bytes.size()), file.size());
    EXPECT_TRUE(file.encryption_meta().empty());
    ASSIGN_OR_ABORT(auto reader, fs::new_random_access_file(_tablet_manager->delvec_location(tablet_id, file.name())));
    ASSIGN_OR_ABORT(auto physical_size, reader->get_size());
    EXPECT_EQ(expected_physical_bytes.size(), physical_size);
    ASSIGN_OR_ABORT(auto physical_bytes, reader->read_all());
    EXPECT_EQ(expected_physical_bytes, physical_bytes);
    ASSERT_GE(physical_bytes.size(), prefix.size());
    EXPECT_EQ(prefix, physical_bytes.substr(0, prefix.size()));
    EXPECT_EQ(padding, physical_bytes.substr(prefix.size()));
    DelVector loaded;
    LakeIOOptions io_options;
    ASSERT_OK(get_del_vec(_tablet_manager.get(), *metadata, /*segment_id=*/1, false, io_options, &loaded));
    EXPECT_EQ(prefix, loaded.save());
}

TEST_F(MetaFileTest, test_get_delvec_ignores_encryption_metadata) {
    const int64_t tablet_id = next_id();
    const int64_t version = 11;
    const uint32_t segment_id = 7;
    const std::string file_name = "plain-delvec-with-stale-encryption-meta.delvec";

    DelVector expected;
    const uint32_t deleted_rowids[] = {3, 9};
    expected.init(version, deleted_rowids, std::size(deleted_rowids));
    const std::string content = expected.save();
    write_file(_tablet_manager->delvec_location(tablet_id, file_name), content);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(version);
    auto& file = (*metadata.mutable_delvec_meta()->mutable_version_to_file())[version];
    file.set_name(file_name);
    file.set_size(content.size());
    file.set_encryption_meta(std::string(1, static_cast<char>(0xff)));
    auto& page = (*metadata.mutable_delvec_meta()->mutable_delvecs())[segment_id];
    page.set_version(version);
    page.set_offset(0);
    page.set_size(content.size());

    DelVector actual;
    LakeIOOptions io_options;
    ASSERT_OK(get_del_vec(_tablet_manager.get(), metadata, segment_id, false, io_options, &actual));
    EXPECT_EQ(expected.save(), actual.save());
}

TEST_F(MetaFileTest, test_compacted_delvec_raw_page_writes_plaintext) {
    const int64_t source_tablet_id = next_id();
    const int64_t target_tablet_id = next_id();
    const int64_t txn_id = next_id();
    const int64_t version = 11;
    const uint32_t segment_id = 7;
    const std::string file_name = "plain-merge-source.delvec";

    DelVector expected;
    const uint32_t deleted_rowids[] = {2, 8};
    expected.init(version, deleted_rowids, std::size(deleted_rowids));
    const std::string content = expected.save();
    write_file(_tablet_manager->delvec_location(source_tablet_id, file_name), content);

    DelvecPageInfo source;
    source.tablet_id = source_tablet_id;
    source.delvec_file.set_name(file_name);
    source.delvec_file.set_size(content.size());
    source.page.set_version(version);
    source.page.set_offset(0);
    source.page.set_size(content.size());

    FileMetaPB output;
    std::vector<uint64_t> offsets;
    ASSERT_OK(write_compacted_delvec_pages(_tablet_manager.get(), {DelvecOutputPage{.raw_page = source}},
                                           target_tablet_id, txn_id, &output, &offsets));
    ASSERT_EQ(std::vector<uint64_t>({0}), offsets);
    EXPECT_TRUE(output.encryption_meta().empty());

    TabletMetadataPB metadata;
    metadata.set_id(target_tablet_id);
    metadata.set_version(version);
    (*metadata.mutable_delvec_meta()->mutable_version_to_file())[version] = output;
    auto& page = (*metadata.mutable_delvec_meta()->mutable_delvecs())[segment_id];
    page.set_version(version);
    page.set_offset(0);
    page.set_size(content.size());
    DelVector actual;
    LakeIOOptions io_options;
    ASSERT_OK(get_del_vec(_tablet_manager.get(), metadata, segment_id, false, io_options, &actual));
    EXPECT_EQ(expected.save(), actual.save());
}

TEST_F(MetaFileTest, test_compacted_delvec_serialized_page_writes_plaintext) {
    const int64_t tablet_id = next_id();
    const int64_t txn_id = next_id();
    DelVector expected;
    const uint32_t deleted = 1;
    expected.init(/*version=*/2, &deleted, 1);

    FileMetaPB output;
    std::vector<uint64_t> offsets;
    ASSERT_OK(write_compacted_delvec_pages(_tablet_manager.get(),
                                           {DelvecOutputPage{.serialized_page = expected.save()}}, tablet_id, txn_id,
                                           &output, &offsets));
    EXPECT_TRUE(output.encryption_meta().empty());
    EXPECT_EQ(std::vector<uint64_t>({0}), offsets);
}

TEST_F(MetaFileTest, test_compacted_delvec_failure_atomic_by_phase) {
    constexpr std::string_view kPrefix = "injected compacted delvec ";
    const int64_t source_tablet = next_id();
    const int64_t target_tablet = next_id();
    const std::string source_name = "failure-atomic-source.delvec";
    const std::string source_bytes((1UL << 20) + 17, 'r');
    write_file(_tablet_manager->delvec_location(source_tablet, source_name), source_bytes);
    auto raw = raw_output_page(source_tablet, source_name, 0, source_bytes.size(), source_bytes.size());
    const std::vector<DelvecOutputPage> pages = {raw, DelvecOutputPage{.serialized_page = "serialized"}};

    auto expect_source_unchanged = [&] {
        ASSIGN_OR_ABORT(auto reader,
                        fs::new_random_access_file(_tablet_manager->delvec_location(source_tablet, source_name)));
        ASSIGN_OR_ABORT(auto actual, reader->read_all());
        EXPECT_EQ(source_bytes, actual);
    };
    auto run_phase = [&](std::string_view seam, int fail_on_call, int expected_calls) {
        SCOPED_TRACE(seam);
        FileMetaPB output;
        output.set_name("unchanged.delvec");
        output.set_size(123);
        output.set_shared(true);
        std::vector<uint64_t> offsets = {7, 11};
        const std::string output_before = output.SerializeAsString();
        const auto offsets_before = offsets;
        const std::string message = std::string(kPrefix) + std::string(seam);
        int calls = 0;
        auto* sync = SyncPoint::GetInstance();
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
        sync->SetCallBack(std::string(seam), [&](void* arg) {
            if (++calls == fail_on_call) *static_cast<Status*>(arg) = Status::InternalError(message);
        });
        sync->EnableProcessing();
        const Status status =
                write_compacted_delvec_pages(_tablet_manager.get(), pages, target_tablet, next_id(), &output, &offsets);
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
        EXPECT_EQ(message, status.message()) << status;
        EXPECT_EQ(expected_calls, calls);
        EXPECT_EQ(output_before, output.SerializeAsString());
        EXPECT_EQ(offsets_before, offsets);
        expect_source_unchanged();

        ASSERT_OK(write_compacted_delvec_pages(_tablet_manager.get(), pages, target_tablet, next_id(), &output,
                                               &offsets));
        EXPECT_EQ(std::vector<uint64_t>({0, source_bytes.size()}), offsets);
        ASSIGN_OR_ABORT(auto reader,
                        fs::new_random_access_file(_tablet_manager->delvec_location(target_tablet, output.name())));
        ASSIGN_OR_ABORT(auto actual, reader->read_all());
        EXPECT_EQ(source_bytes + "serialized", actual);
    };

    // The actual-size seam is a preflight failure: no destination writer may have been created.
    {
        FileMetaPB output;
        output.set_name("unchanged.delvec");
        std::vector<uint64_t> offsets = {7, 11};
        const std::string output_before = output.SerializeAsString();
        const auto offsets_before = offsets;
        int calls = 0;
        auto absent_size_raw = raw;
        absent_size_raw.raw_page->delvec_file.clear_size();
        auto* sync = SyncPoint::GetInstance();
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
        sync->SetCallBack("write_compacted_delvec_pages:source_size_override", [&](void* arg) {
            ++calls;
            *static_cast<std::optional<StatusOr<int64_t>>*>(arg) = Status::InternalError("injected actual-size");
        });
        sync->EnableProcessing();
        const Status status = write_compacted_delvec_pages(_tablet_manager.get(), {absent_size_raw}, target_tablet,
                                                           next_id(), &output, &offsets);
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
        EXPECT_EQ("injected actual-size", status.message()) << status;
        EXPECT_EQ(1, calls);
        EXPECT_EQ(output_before, output.SerializeAsString());
        EXPECT_EQ(offsets_before, offsets);
        expect_source_unchanged();
        ASSERT_OK(write_compacted_delvec_pages(_tablet_manager.get(), {absent_size_raw}, target_tablet, next_id(),
                                               &output, &offsets));
    }

    run_phase("write_compacted_delvec_pages:before_writer_open", 1, 1);
    run_phase("write_compacted_delvec_pages:before_read_chunk", 1, 1);
    run_phase("write_compacted_delvec_pages:before_read_chunk", 2, 2);
    run_phase("append_delvec_bytes_bounded:before_chunk", 1, 1);
    run_phase("append_delvec_bytes_bounded:before_chunk", 2, 2);
    run_phase("write_compacted_delvec_pages:before_close", 1, 1);
    run_phase("write_compacted_delvec_pages:before_apply_offsets", 1, 1);

    // This source does not exist at INT64_MAX; the injected actual size and writer failure prove
    // that the valid near-limit declaration is preflighted without allocating or reading its range.
    {
        auto near_limit = raw_output_page(source_tablet, source_name, std::numeric_limits<int64_t>::max() - 7, 7);
        FileMetaPB output;
        output.set_name("unchanged.delvec");
        std::vector<uint64_t> offsets = {7, 11};
        const std::string output_before = output.SerializeAsString();
        const auto offsets_before = offsets;
        int override_calls = 0;
        int writer_calls = 0;
        auto* sync = SyncPoint::GetInstance();
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
        sync->SetCallBack("write_compacted_delvec_pages:source_size_override", [&](void* arg) {
            ++override_calls;
            *static_cast<std::optional<StatusOr<int64_t>>*>(arg) = std::numeric_limits<int64_t>::max();
        });
        sync->SetCallBack("write_compacted_delvec_pages:before_writer_open", [&](void* arg) {
            ++writer_calls;
            *static_cast<Status*>(arg) = Status::InternalError("injected near-limit writer");
        });
        sync->EnableProcessing();
        const Status status = write_compacted_delvec_pages(_tablet_manager.get(), {near_limit}, target_tablet,
                                                           next_id(), &output, &offsets);
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
        EXPECT_EQ("injected near-limit writer", status.message()) << status;
        EXPECT_EQ(1, override_calls);
        EXPECT_EQ(1, writer_calls);
        EXPECT_EQ(output_before, output.SerializeAsString());
        EXPECT_EQ(offsets_before, offsets);
        expect_source_unchanged();
    }
}

TEST_F(MetaFileTest, test_delvec_output_append_failure_is_atomic) {
    constexpr std::string_view kInjectedError = "injected delvec append failure";
    int injection_count = 0;
    SyncPoint::GetInstance()->SetCallBack("append_delvec_bytes_bounded:before_chunk", [&](void* arg) {
        ++injection_count;
        *static_cast<Status*>(arg) = Status::InternalError(kInjectedError);
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup_sync_points([&] {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    DelVector direct_delvec;
    const uint32_t direct_deleted = 2;
    direct_delvec.init(/*version=*/2, &direct_deleted, 1);
    FileMetaPB direct_output;
    const std::string direct_output_before = direct_output.SerializeAsString();
    std::vector<uint64_t> direct_offsets;
    auto direct_status = write_compacted_delvec_pages(_tablet_manager.get(),
                                                      {DelvecOutputPage{.serialized_page = direct_delvec.save()}},
                                                      next_id(), next_id(), &direct_output, &direct_offsets);
    EXPECT_FALSE(direct_status.ok());
    EXPECT_TRUE(direct_status.message().contains(kInjectedError)) << direct_status;
    EXPECT_EQ(direct_output_before, direct_output.SerializeAsString());

    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    auto meta_a = make_shared_delvec_source(child_a, /*segment_count=*/1);
    auto meta_b = make_shared_delvec_source(child_b, /*segment_count=*/1);
    DelVector delvec_a;
    const uint32_t deleted_a = 4;
    delvec_a.init(/*version=*/10, &deleted_a, 1);
    add_test_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, "append_atomic_a.delvec", delvec_a.save());
    DelVector delvec_b;
    const uint32_t deleted_b = 7;
    delvec_b.init(/*version=*/11, &deleted_b, 1);
    add_test_delvec(meta_b.get(), child_b, /*version=*/11, /*segment_id=*/1, "append_atomic_b.delvec", delvec_b.save());

    std::unordered_map<int64_t, TabletMetadataPtr> published_metadatas;
    auto publish_status = publish_delvec_merge({meta_a, meta_b}, merged_tablet, next_id(), &published_metadatas);
    EXPECT_FALSE(publish_status.ok());
    EXPECT_TRUE(publish_status.message().contains(kInjectedError)) << publish_status;
    EXPECT_FALSE(published_metadatas.contains(merged_tablet));
    EXPECT_TRUE(_tablet_manager->get_tablet_metadata(merged_tablet, /*version=*/2).status().is_not_found());
    EXPECT_EQ(2, injection_count);
}

TEST_F(MetaFileTest, test_delvec_output_close_failure_is_atomic) {
    constexpr std::string_view kInjectedError = "injected delvec close failure";
    int injection_count = 0;
    SyncPoint::GetInstance()->SetCallBack("write_compacted_delvec_pages:before_close", [&](void* arg) {
        ++injection_count;
        *static_cast<Status*>(arg) = Status::InternalError(kInjectedError);
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup_sync_points([&] {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    DelVector direct_raw_delvec;
    const uint32_t direct_raw_deleted = 1;
    direct_raw_delvec.init(/*version=*/10, &direct_raw_deleted, 1);
    auto direct_metadata = make_shared_delvec_source(next_id(), /*segment_count=*/1);
    auto direct_source = add_test_delvec(direct_metadata.get(), direct_metadata->id(), /*version=*/10,
                                         /*segment_id=*/1, "close_atomic_direct.delvec", direct_raw_delvec.save());
    DelVector direct_union_delvec;
    const uint32_t direct_union_deleted = 3;
    direct_union_delvec.init(/*version=*/2, &direct_union_deleted, 1);
    const std::string direct_union_content = direct_union_delvec.save();
    FileMetaPB direct_output;
    const std::string direct_output_before = direct_output.SerializeAsString();
    std::vector<uint64_t> direct_offsets;
    auto direct_status = write_compacted_delvec_pages(
            _tablet_manager.get(),
            {DelvecOutputPage{.raw_page = direct_source}, DelvecOutputPage{.serialized_page = direct_union_content}},
            next_id(), next_id(), &direct_output, &direct_offsets);
    EXPECT_FALSE(direct_status.ok());
    EXPECT_TRUE(direct_status.message().contains(kInjectedError)) << direct_status;
    EXPECT_EQ(direct_output_before, direct_output.SerializeAsString());

    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    auto meta_a = make_shared_delvec_source(child_a, /*segment_count=*/2);
    auto meta_b = make_shared_delvec_source(child_b, /*segment_count=*/2);
    DelVector plain_single;
    const uint32_t plain_deleted = 2;
    plain_single.init(/*version=*/10, &plain_deleted, 1);
    add_test_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, "close_atomic_plain.delvec",
                    plain_single.save());
    DelVector merged_a;
    const uint32_t merged_deleted_a = 5;
    merged_a.init(/*version=*/11, &merged_deleted_a, 1);
    add_test_delvec(meta_a.get(), child_a, /*version=*/11, /*segment_id=*/2, "close_atomic_merged_a.delvec",
                    merged_a.save());
    DelVector merged_b;
    const uint32_t merged_deleted_b = 8;
    merged_b.init(/*version=*/12, &merged_deleted_b, 1);
    add_test_delvec(meta_b.get(), child_b, /*version=*/12, /*segment_id=*/2, "close_atomic_merged_b.delvec",
                    merged_b.save());

    std::unordered_map<int64_t, TabletMetadataPtr> published_metadatas;
    auto publish_status = publish_delvec_merge({meta_a, meta_b}, merged_tablet, next_id(), &published_metadatas);
    EXPECT_FALSE(publish_status.ok());
    EXPECT_TRUE(publish_status.message().contains(kInjectedError)) << publish_status;
    EXPECT_FALSE(published_metadatas.contains(merged_tablet));
    EXPECT_TRUE(_tablet_manager->get_tablet_metadata(merged_tablet, /*version=*/2).status().is_not_found());
    EXPECT_EQ(2, injection_count);
}

TEST_F(MetaFileTest, test_delvec_rw) {
    // 1. generate metadata
    const int64_t tablet_id = 10002;
    const uint32_t segment_id = 1234;
    const int64_t version = 11;
    const int64_t version2 = 12;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // 2. write pk meta & delvec
    MetaFileBuilder builder(*tablet, metadata);
    DelVector dv;
    dv.set_empty();
    EXPECT_TRUE(dv.empty());

    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {1, 3, 5, 7, 90000};
    dv.add_dels_as_new_version(dels, version, &ndv);
    EXPECT_FALSE(ndv->empty());
    std::string before_delvec = ndv->save();
    builder.append_delvec(ndv, segment_id);
    Status st = builder.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 3. read delvec
    DelVector after_delvec;
    LakeIOOptions lake_io_opts;
    ASSIGN_OR_ABORT(auto metadata2, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_TRUE(get_del_vec(_tablet_manager.get(), *metadata2, segment_id, true, lake_io_opts, &after_delvec).ok());
    EXPECT_EQ(before_delvec, after_delvec.save());

    // 4. read meta
    auto iter = metadata2->delvec_meta().delvecs().find(segment_id);
    EXPECT_TRUE(iter != metadata2->delvec_meta().delvecs().end());
    auto delvec_pagepb = iter->second;
    EXPECT_EQ(delvec_pagepb.version(), version);

    // 5. update delvec
    metadata->set_version(version2);
    MetaFileBuilder builder2(*tablet, metadata);
    DelVector dv2;
    dv2.set_empty();
    EXPECT_TRUE(dv2.empty());
    std::shared_ptr<DelVector> ndv2;
    std::vector<uint32_t> dels2 = {1, 3, 5, 9, 90000};
    dv2.add_dels_as_new_version(dels2, version2, &ndv2);
    builder2.append_delvec(ndv2, segment_id);
    st = builder2.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 6. read again
    ASSIGN_OR_ABORT(auto metadata3, _tablet_manager->get_tablet_metadata(tablet_id, version2));

    iter = metadata3->delvec_meta().delvecs().find(segment_id);
    EXPECT_TRUE(iter != metadata3->delvec_meta().delvecs().end());
    auto delvecpb = iter->second;
    EXPECT_EQ(delvecpb.version(), version2);

    // 7. test reclaim delvec version to file name record
    ASSIGN_OR_ABORT(auto metadata4, _tablet_manager->get_tablet_metadata(tablet_id, version2));

    // clear all delvec meta element so that all element in
    // version_to_file map will also be removed
    // in this case, delvecs meta map has only one element [key=(segment=1234, value=(version=12, offset=0, size=35)]
    // delvec_to_file has also one element [key=(version=12), value=(delvec_file=xxx)]
    // after clearing,  delvecs meta map will have nothing, and element in delvec_to_file will also be useless
    auto new_meta = std::make_shared<TabletMetadataPB>(*metadata4);
    new_meta->mutable_delvec_meta()->mutable_delvecs()->clear();

    // insert a new delvec record into delvecs meta map with new version 13
    // we expect the old element in delvec_to_file map (version 12) will be removed
    auto new_version = version2 + 1;
    MetaFileBuilder builder3(*tablet, new_meta);
    new_meta->set_version(new_version);
    DelVector dv3;
    dv3.set_empty();
    EXPECT_TRUE(dv3.empty());
    std::shared_ptr<DelVector> ndv3;
    std::vector<uint32_t> dels3 = {1, 3, 5, 9, 90000};
    dv3.add_dels_as_new_version(dels3, new_version, &ndv3);
    builder3.append_delvec(ndv3, segment_id + 1);
    st = builder3.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // validate delvec file record with version 12 been removed
    ASSIGN_OR_ABORT(auto metadata5, _tablet_manager->get_tablet_metadata(tablet_id, new_version));
    auto version_to_file_map = metadata5->delvec_meta().version_to_file();
    EXPECT_EQ(version_to_file_map.size(), 1);

    auto iter2 = version_to_file_map.find(version2);
    EXPECT_TRUE(iter2 == version_to_file_map.end());

    iter2 = version_to_file_map.find(new_version);
    EXPECT_TRUE(iter2 != version_to_file_map.end());
}

// A delvec page whose bytes fail the recorded crc32c is most plausibly a corrupted
// block in the local data cache, so get_del_vec drops that cache and reads once
// more. Simulate exactly that: corrupt the delvec file, then have the cache-drop
// hook restore the original bytes -- standing in for the retry reading through to
// an intact remote object -- and the read must then succeed. Without the hook the
// drop reports NotSupported on this build, the retry is skipped, and the original
// Corruption surfaces.
TEST_F(MetaFileTest, test_get_del_vec_crc32c_retries_after_dropping_cache) {
    const int64_t tablet_id = 10012;
    const uint32_t segment_id = 5678;
    const int64_t version = 11;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    MetaFileBuilder builder(*tablet, metadata);
    DelVector dv;
    dv.set_empty();
    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {1, 3, 5, 7, 90000};
    dv.add_dels_as_new_version(dels, version, &ndv);
    const std::string expected_delvec = ndv->save();
    builder.append_delvec(ndv, segment_id);
    ASSERT_OK(builder.finalize(next_id()));

    ASSIGN_OR_ABORT(auto metadata2, _tablet_manager->get_tablet_metadata(tablet_id, version));
    auto page_iter = metadata2->delvec_meta().delvecs().find(segment_id);
    ASSERT_TRUE(page_iter != metadata2->delvec_meta().delvecs().end());
    const auto& delvec_page = page_iter->second;
    ASSERT_TRUE(delvec_page.has_crc32c());
    auto file_iter = metadata2->delvec_meta().version_to_file().find(delvec_page.version());
    ASSERT_TRUE(file_iter != metadata2->delvec_meta().version_to_file().end());
    const std::string delvec_path = _tablet_manager->delvec_location(tablet_id, file_iter->second.name());

    // Keep the good bytes, then corrupt one byte inside the page (length-preserving).
    std::string good_bytes;
    {
        ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(delvec_path));
        ASSIGN_OR_ABORT(good_bytes, rf->read_all());
        ASSERT_GT(good_bytes.size(), delvec_page.offset());
        auto corrupted = good_bytes;
        corrupted[delvec_page.offset()] = static_cast<char>(corrupted[delvec_page.offset()] ^ 0xff);
        WritableFileOptions wopts{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(wopts, delvec_path));
        ASSERT_OK(wf->append(Slice(corrupted)));
        ASSERT_OK(wf->close());
    }
    // Make sure the reads below hit the file, not a delvec primed into the metacache.
    _tablet_manager->metacache()->prune();

    bool old_strict = config::enable_strict_delvec_crc_check;
    config::enable_strict_delvec_crc_check = true;
    LakeIOOptions lake_io_opts;

    // Without the hook: the drop is NotSupported here, so the original Corruption
    // must surface.
    {
        DelVector read_delvec;
        auto st = get_del_vec(_tablet_manager.get(), *metadata2, delvec_page, false, lake_io_opts, &read_delvec);
        ASSERT_TRUE(st.is_corruption()) << st;
    }

    // With the hook: force the drop to report success and restore the file at the
    // same moment -- that is what dropping a corrupt cached block achieves in
    // production -- and the retry must succeed.
    int drop_calls = 0;
    const std::string sync_point = "lake::drop_corrupted_delvec_file_cache";
    SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void* arg) {
        ++drop_calls;
        WritableFileOptions wopts{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(wopts, delvec_path));
        CHECK_OK(wf->append(Slice(good_bytes)));
        CHECK_OK(wf->close());
        *(Status*)arg = Status::OK();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    {
        DelVector read_delvec;
        auto st = get_del_vec(_tablet_manager.get(), *metadata2, delvec_page, false, lake_io_opts, &read_delvec);
        ASSERT_OK(st);
        EXPECT_EQ(expected_delvec, read_delvec.save());
    }
    EXPECT_EQ(1, drop_calls);
    SyncPoint::GetInstance()->ClearCallBack(sync_point);
    SyncPoint::GetInstance()->DisableProcessing();
    config::enable_strict_delvec_crc_check = old_strict;
}

TEST_F(MetaFileTest, test_delvec_read_loop) {
    // 1. generate metadata
    const int64_t tablet_id = 10002;
    const int64_t version = 11;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // 2. test delvec
    auto test_delvec = [&](uint32_t segment_id) {
        MetaFileBuilder builder(*tablet, metadata);
        DelVector dv;
        dv.set_empty();
        EXPECT_TRUE(dv.empty());

        std::shared_ptr<DelVector> ndv;
        std::vector<uint32_t> dels;
        for (int i = 0; i < 10; i++) {
            dels.push_back(rand() % 1000);
        }
        dv.add_dels_as_new_version(dels, version, &ndv);
        EXPECT_FALSE(ndv->empty());
        std::string before_delvec = ndv->save();
        builder.append_delvec(ndv, segment_id);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());

        // 3. read delvec
        DelVector after_delvec;
        LakeIOOptions lake_io_opts;
        ASSIGN_OR_ABORT(auto meta, _tablet_manager->get_tablet_metadata(tablet_id, version));
        EXPECT_TRUE(get_del_vec(_tablet_manager.get(), *meta, segment_id, false, lake_io_opts, &after_delvec).ok());
        EXPECT_EQ(before_delvec, after_delvec.save());
    };
    for (uint32_t segment_id = 1000; segment_id < 1200; segment_id++) {
        test_delvec(segment_id);
    }
    // test twice
    for (uint32_t segment_id = 1000; segment_id < 1200; segment_id++) {
        test_delvec(segment_id);
    }
}

TEST_F(MetaFileTest, test_dcg) {
    // 1. generate metadata
    const int64_t tablet_id = 10001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    {
        MetaFileBuilder builder(*tablet, metadata);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }

    // 2. write first rowset
    {
        metadata->set_version(11);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("aaa.dat");
        TxnLogPB_OpWrite op_write;
        std::map<int, SegmentFileInfo> replace_segments;
        std::vector<FileMetaPB> orphan_files;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        builder.apply_opwrite(op_write, replace_segments, orphan_files);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    // 3. write dcg
    {
        metadata->set_version(12);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("bbb.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        std::vector<std::pair<std::string, std::string>> filenames;
        filenames.emplace_back("aaa.cols", "");
        filenames.emplace_back("bbb.cols", "");
        std::vector<std::vector<ColumnUID>> unique_column_id_list;
        unique_column_id_list.push_back({3, 4, 5});
        unique_column_id_list.push_back({6, 7, 8});
        std::vector<int64_t> file_sizes{111, 222};
        builder.append_dcg(110, filenames, unique_column_id_list, file_sizes);
        builder.apply_column_mode_partial_update(op_write);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
        // <3, 4, 5> -> aaa.cols
        // <6, 7, 8> -> bbb.cols
    }
    {
        metadata->set_version(13);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("ccc.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        std::vector<std::pair<std::string, std::string>> filenames;
        filenames.emplace_back("ccc.cols", "");
        std::vector<std::vector<ColumnUID>> unique_column_id_list;
        unique_column_id_list.push_back({4, 7});
        std::vector<int64_t> file_sizes{333};
        builder.append_dcg(110, filenames, unique_column_id_list, file_sizes);
        builder.apply_column_mode_partial_update(op_write);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
        // <3, 5> -> aaa.cols
        // <6, 8> -> bbb.cols
        // <4, 7> -> ccc.cols
    }
    {
        metadata->set_version(14);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("ddd.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        std::vector<std::pair<std::string, std::string>> filenames;
        filenames.emplace_back("ddd.cols", "");
        std::vector<std::vector<ColumnUID>> unique_column_id_list;
        unique_column_id_list.push_back({3, 5});
        std::vector<int64_t> file_sizes{444};
        builder.append_dcg(110, filenames, unique_column_id_list, file_sizes);
        builder.apply_column_mode_partial_update(op_write);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
        auto dcg_ver_iter = metadata->dcg_meta().dcgs().find(110);
        EXPECT_TRUE(dcg_ver_iter != metadata->dcg_meta().dcgs().end());
        EXPECT_TRUE(dcg_ver_iter->second.versions_size() == 3);
        EXPECT_TRUE(dcg_ver_iter->second.column_files_size() == 3);
        EXPECT_TRUE(dcg_ver_iter->second.unique_column_ids_size() == 3);
        // column_file_sizes stays 1:1 with column_files, and carries forward the size
        // of each retained `.cols` file regardless of how entries are reordered.
        EXPECT_TRUE(dcg_ver_iter->second.column_file_sizes_size() == 3);
        std::map<std::string, int64_t> file_to_size;
        for (int i = 0; i < dcg_ver_iter->second.column_files_size(); i++) {
            file_to_size[dcg_ver_iter->second.column_files(i)] = dcg_ver_iter->second.column_file_sizes(i);
        }
        EXPECT_EQ(444, file_to_size["ddd.cols"]);
        EXPECT_EQ(222, file_to_size["bbb.cols"]);
        EXPECT_EQ(333, file_to_size["ccc.cols"]);
        // <3, 5> -> ddd.cols
        // <6, 8> -> bbb.cols
        // <4, 7> -> ccc.cols
    }
    {
        auto loader = std::make_unique<LakeDeltaColumnGroupLoader>(metadata);
        TabletSegmentId tsid;
        tsid.tablet_id = tablet_id;
        tsid.segment_id = 110;
        DeltaColumnGroupList pdcgs;
        EXPECT_TRUE(loader->load(tsid, 1, &pdcgs).ok());
        EXPECT_TRUE(pdcgs.size() == 1);
        auto idx = pdcgs[0]->get_column_idx(3);
        EXPECT_TRUE("tmp/ddd.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/ddd.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
        idx = pdcgs[0]->get_column_idx(4);
        EXPECT_TRUE("tmp/ccc.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/ccc.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
        idx = pdcgs[0]->get_column_idx(5);
        EXPECT_TRUE("tmp/ddd.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/ddd.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
        idx = pdcgs[0]->get_column_idx(6);
        EXPECT_TRUE("tmp/bbb.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/bbb.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
        idx = pdcgs[0]->get_column_idx(7);
        EXPECT_TRUE("tmp/ccc.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/ccc.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
        idx = pdcgs[0]->get_column_idx(8);
        EXPECT_TRUE("tmp/bbb.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/bbb.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
    }
    // 4. compact (conflict)
    {
        metadata->set_version(15);
        MetaFileBuilder builder(*tablet, metadata);
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(110);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("eee.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(rowset_metadata);
        op_compaction.set_compact_version(13);
        EXPECT_TRUE(CompactionUpdateConflictChecker::conflict_check(op_compaction, 111, *metadata, &builder));
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    // 5. compact
    {
        metadata->set_version(16);
        MetaFileBuilder builder(*tablet, metadata);
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(110);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("fff.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(rowset_metadata);
        op_compaction.set_compact_version(14);
        EXPECT_FALSE(CompactionUpdateConflictChecker::conflict_check(op_compaction, 111, *metadata, &builder));
        builder.apply_opcompaction(op_compaction, 1, 0);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    {
        auto loader = std::make_unique<LakeDeltaColumnGroupLoader>(metadata);
        TabletSegmentId tsid;
        tsid.tablet_id = tablet_id;
        tsid.segment_id = 110;
        DeltaColumnGroupList pdcgs;
        EXPECT_TRUE(loader->load(tsid, 1, &pdcgs).ok());
        EXPECT_TRUE(pdcgs.empty());
    }
    // 6. check orphan files
    {
        std::set<std::string> to_check_filenames;
        to_check_filenames.insert("aaa.cols");
        to_check_filenames.insert("bbb.cols");
        to_check_filenames.insert("ccc.cols");
        to_check_filenames.insert("ddd.cols");
        to_check_filenames.insert("bbb.dat");
        to_check_filenames.insert("ccc.dat");
        to_check_filenames.insert("ddd.dat");
        to_check_filenames.insert("eee.dat");
        EXPECT_TRUE(metadata->orphan_files_size() == to_check_filenames.size());
        for (const auto& orphan_file : metadata->orphan_files()) {
            EXPECT_TRUE(to_check_filenames.count(orphan_file.name()) > 0);
        }
    }
}

TEST_F(MetaFileTest, test_unpersistent_del_files_when_compact) {
    // 1. generate metadata
    const int64_t tablet_id = 10001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    {
        MetaFileBuilder builder(*tablet, metadata);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }

    // 2. write first rowset (110)
    {
        metadata->set_version(11);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("aaa.dat");
        TxnLogPB_OpWrite op_write;
        std::map<int, SegmentFileInfo> replace_segments;
        std::vector<FileMetaPB> orphan_files;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        builder.apply_opwrite(op_write, replace_segments, orphan_files);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    // 3. write second rowset with del files (111)
    {
        metadata->set_version(12);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("bbb.dat");
        DelfileWithRowsetId delfile;
        delfile.set_name("bbb1.del");
        delfile.set_origin_rowset_id(metadata->next_rowset_id());
        rowset_metadata.add_del_files()->CopyFrom(delfile);
        delfile.set_name("bbb2.del");
        rowset_metadata.add_del_files()->CopyFrom(delfile);
        TxnLogPB_OpWrite op_write;
        std::map<int, SegmentFileInfo> replace_segments;
        std::vector<FileMetaPB> orphan_files;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        builder.apply_opwrite(op_write, replace_segments, orphan_files);
        PersistentIndexSstablePB sstable;
        sstable.set_max_rss_rowid((uint64_t)111 << 32);
        metadata->mutable_sstable_meta()->add_sstables()->CopyFrom(sstable);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    // 4. compact (112)
    {
        metadata->set_version(13);
        MetaFileBuilder builder(*tablet, metadata);
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(110);
        op_compaction.add_input_rowsets(111);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("ccc.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(rowset_metadata);
        op_compaction.set_compact_version(13);
        builder.apply_opcompaction(op_compaction, 111, 0);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
        // check unpersistent del files
        EXPECT_TRUE(metadata->rowsets_size() == 1);
        EXPECT_TRUE(metadata->rowsets(0).del_files_size() == 2);
        EXPECT_TRUE(metadata->rowsets(0).del_files(0).name() == "bbb1.del");
        EXPECT_TRUE(metadata->rowsets(0).del_files(0).origin_rowset_id() == 111);
        EXPECT_TRUE(metadata->rowsets(0).del_files(1).name() == "bbb2.del");
        EXPECT_TRUE(metadata->rowsets(0).del_files(1).origin_rowset_id() == 111);
        EXPECT_TRUE(metadata->compaction_inputs_size() == 2);
        EXPECT_TRUE(metadata->compaction_inputs(0).del_files_size() == 0);
        EXPECT_TRUE(metadata->compaction_inputs(1).del_files_size() == 0);
    }
    // 5. keep write (113)
    {
        metadata->set_version(14);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("ddd.dat");
        TxnLogPB_OpWrite op_write;
        std::map<int, SegmentFileInfo> replace_segments;
        std::vector<FileMetaPB> orphan_files;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        builder.apply_opwrite(op_write, replace_segments, orphan_files);
        PersistentIndexSstablePB sstable;
        sstable.set_max_rss_rowid((uint64_t)113 << 32);
        metadata->mutable_sstable_meta()->add_sstables()->CopyFrom(sstable);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    // 6. compact (114)
    {
        metadata->set_version(15);
        MetaFileBuilder builder(*tablet, metadata);
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(112);
        op_compaction.add_input_rowsets(113);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segment_metas()->set_filename("eee.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(rowset_metadata);
        op_compaction.set_compact_version(15);
        builder.apply_opcompaction(op_compaction, 113, 0);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
        // check unpersistent del files
        EXPECT_TRUE(metadata->rowsets_size() == 1);
        EXPECT_TRUE(metadata->rowsets(0).del_files_size() == 0);
        EXPECT_TRUE(metadata->compaction_inputs(0).del_files_size() == 0);
        EXPECT_TRUE(metadata->compaction_inputs(1).del_files_size() == 0);
    }
}

TEST_F(MetaFileTest, test_compaction_conflict_checker_with_sparse_segment_id) {
    const int64_t tablet_id = 32001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(200);

    auto* input_rowset = metadata->add_rowsets();
    input_rowset->set_id(110);
    {
        auto* sm0 = input_rowset->add_segment_metas();
        sm0->set_filename("a.dat");
        sm0->set_segment_idx(0);
        auto* sm1 = input_rowset->add_segment_metas();
        sm1->set_filename("b.dat");
        sm1->set_segment_idx(5);
    }

    DeltaColumnGroupVerPB dcg;
    dcg.add_versions(13);
    (*metadata->mutable_dcg_meta()->mutable_dcgs())[115] = dcg;

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(110);
    op_compaction.set_compact_version(12);
    op_compaction.mutable_output_rowset()->add_segment_metas()->set_filename("out.dat");

    EXPECT_TRUE(CompactionUpdateConflictChecker::conflict_check(op_compaction, 111, *metadata, &builder));
}

TEST_F(MetaFileTest, test_trim_partial_compaction_last_input_rowset) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(9);
    metadata->set_version(10);

    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(1);
    op_compaction.add_input_rowsets(11);
    op_compaction.add_input_rowsets(22);
    op_compaction.mutable_output_rowset()->add_segment_metas()->set_filename("aaa.dat");
    op_compaction.mutable_output_rowset()->add_segment_metas()->set_filename("bbb.dat");
    op_compaction.mutable_output_rowset()->add_segment_metas()->set_filename("ccc.dat");
    op_compaction.mutable_output_rowset()->add_segment_metas()->set_filename("ddd.dat");
    RowsetMetadataPB last_input_rowset_metadata;

    last_input_rowset_metadata.set_id(33);
    last_input_rowset_metadata.mutable_segment_metas()->Clear();
    last_input_rowset_metadata.add_segment_metas()->set_filename("aaa.dat");
    last_input_rowset_metadata.add_segment_metas()->set_filename("eee.dat");
    last_input_rowset_metadata.add_segment_metas()->set_filename("fff.dat");
    last_input_rowset_metadata.add_segment_metas()->set_filename("ddd.dat");
    EXPECT_EQ(last_input_rowset_metadata.segment_metas_size(), 4);
    // rowset id mismatch
    trim_partial_compaction_last_input_rowset(metadata, op_compaction, last_input_rowset_metadata);
    EXPECT_EQ(last_input_rowset_metadata.segment_metas_size(), 4);

    last_input_rowset_metadata.set_id(22);
    // normal case, duplicate segments will be trimed
    trim_partial_compaction_last_input_rowset(metadata, op_compaction, last_input_rowset_metadata);
    EXPECT_EQ(last_input_rowset_metadata.segment_metas_size(), 2);
    EXPECT_EQ(last_input_rowset_metadata.segment_metas(0).filename(), "eee.dat");
    EXPECT_EQ(last_input_rowset_metadata.segment_metas(1).filename(), "fff.dat");

    // no duplicate segments
    last_input_rowset_metadata.mutable_segment_metas()->Clear();
    last_input_rowset_metadata.add_segment_metas()->set_filename("xxx.dat");
    last_input_rowset_metadata.add_segment_metas()->set_filename("yyy.dat");
    EXPECT_EQ(last_input_rowset_metadata.segment_metas_size(), 2);
    trim_partial_compaction_last_input_rowset(metadata, op_compaction, last_input_rowset_metadata);
    EXPECT_EQ(last_input_rowset_metadata.segment_metas_size(), 2);
}

// Verify that trim_partial_compaction_last_input_rowset also trims segment_metas
// so that vacuum won't delete .vi files still referenced by the output rowset.
TEST_F(MetaFileTest, test_trim_partial_compaction_last_input_rowset_with_vi) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(9);
    metadata->set_version(10);

    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(22);
    // Output rowset contains: aaa.dat (reused), new_seg.dat (new compacted), ddd.dat (reused)
    op_compaction.mutable_output_rowset()->add_segment_metas()->set_filename("aaa.dat");
    op_compaction.mutable_output_rowset()->add_segment_metas()->set_filename("new_seg.dat");
    op_compaction.mutable_output_rowset()->add_segment_metas()->set_filename("ddd.dat");

    // Last input rowset has segments: [aaa.dat, bbb.dat, ccc.dat, ddd.dat]
    // where aaa.dat and ddd.dat are reused in output (uncompacted), bbb.dat and ccc.dat are consumed
    RowsetMetadataPB last_input_rowset;
    last_input_rowset.set_id(22);

    // All segments have vector index tracking via segment_metas
    auto* meta_aaa = last_input_rowset.add_segment_metas();
    meta_aaa->set_filename("aaa.dat");
    meta_aaa->add_vector_index_ids(100);
    auto* meta_bbb = last_input_rowset.add_segment_metas();
    meta_bbb->set_filename("bbb.dat");
    meta_bbb->add_vector_index_ids(100);
    meta_bbb->add_vector_index_ids(200);
    auto* meta_ccc = last_input_rowset.add_segment_metas();
    meta_ccc->set_filename("ccc.dat");
    meta_ccc->add_vector_index_ids(100);
    auto* meta_ddd = last_input_rowset.add_segment_metas();
    meta_ddd->set_filename("ddd.dat");
    meta_ddd->add_vector_index_ids(100);

    EXPECT_EQ(last_input_rowset.segment_metas_size(), 4);

    trim_partial_compaction_last_input_rowset(metadata, op_compaction, last_input_rowset);

    // After trim: only consumed segments (bbb.dat, ccc.dat) should remain
    EXPECT_EQ(last_input_rowset.segment_metas(0).filename(), "bbb.dat");
    EXPECT_EQ(last_input_rowset.segment_metas(1).filename(), "ccc.dat");

    // segment_metas should also be trimmed: aaa.dat and ddd.dat entries removed
    EXPECT_EQ(last_input_rowset.segment_metas_size(), 2);

    // Verify the index IDs are preserved correctly
    EXPECT_EQ(last_input_rowset.segment_metas(0).vector_index_ids_size(), 2); // bbb.dat
    EXPECT_EQ(last_input_rowset.segment_metas(1).vector_index_ids_size(), 1); // ccc.dat
}

TEST_F(MetaFileTest, test_error_state) {
    // generate metadata
    const int64_t tablet_id = 10001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);

    // add rowset with segment
    RowsetMetadataPB rowset_metadata;
    rowset_metadata.set_id(110);
    rowset_metadata.add_segment_metas()->set_filename("aaa.dat");
    rowset_metadata.add_segment_metas()->set_filename("bbb.dat");
    metadata->add_rowsets()->CopyFrom(rowset_metadata);
    std::map<uint32_t, size_t> segment_id_to_add_dels;
    for (int i = 0; i < 10; i++) {
        segment_id_to_add_dels[i] = 100;
    }
    // generate error state
    MetaFileBuilder builder(*tablet, metadata);
    Status st = builder.update_num_del_stat(segment_id_to_add_dels);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(StorageMetrics::instance()->primary_key_table_error_state_total.value() > 0);
}

TEST_F(MetaFileTest, test_segment_id_helper_fallback_and_override) {
    RowsetMetadataPB rowset;
    rowset.set_id(1000);
    {
        auto* sm0 = rowset.add_segment_metas();
        sm0->set_filename("a.dat");
        sm0->set_num_rows(10);
        auto* sm1 = rowset.add_segment_metas();
        sm1->set_filename("b.dat");
        sm1->set_num_rows(20);
    }

    // Backward compatibility: fallback to segment index when segment_id is absent.
    EXPECT_EQ(0, get_segment_idx(rowset, 0));
    EXPECT_EQ(1, get_segment_idx(rowset, 1));
    EXPECT_EQ(1000, get_rssid(rowset, 0));
    EXPECT_EQ(1001, get_rssid(rowset, 1));

    rowset.mutable_segment_metas(0)->set_segment_idx(3);
    rowset.mutable_segment_metas(1)->set_segment_idx(8);

    EXPECT_EQ(3, get_segment_idx(rowset, 0));
    EXPECT_EQ(8, get_segment_idx(rowset, 1));
    EXPECT_EQ(1003, get_rssid(rowset, 0));
    EXPECT_EQ(1008, get_rssid(rowset, 1));
}

TEST_F(MetaFileTest, test_apply_opwrite_del_op_offset_uses_max_segment_id) {
    const int64_t tablet_id = 31001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpWrite op_write;
    auto* rowset = op_write.mutable_rowset();
    {
        auto* sm0 = rowset->add_segment_metas();
        sm0->set_filename("a.dat");
        sm0->set_segment_idx(2);
        auto* sm1 = rowset->add_segment_metas();
        sm1->set_filename("b.dat");
        sm1->set_segment_idx(7);
    }
    op_write.add_dels_meta()->set_name("d1.del");
    op_write.add_dels_meta()->set_name("d2.del");

    builder.apply_opwrite(op_write, {}, {});

    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& written = metadata->rowsets(0);
    ASSERT_EQ(2, written.del_files_size());
    EXPECT_EQ(7, written.del_files(0).op_offset());
    EXPECT_EQ(7, written.del_files(1).op_offset());
    EXPECT_EQ(118, metadata->next_rowset_id());
}

// apply_opwrite path: op_write.del_num_rows (parallel to dels_meta) is recorded into
// DelfileWithRowsetId.num_rows; del files without a recorded count leave num_rows unset.
TEST_F(MetaFileTest, test_apply_opwrite_del_num_rows) {
    const int64_t tablet_id = 31010;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpWrite op_write;
    op_write.mutable_rowset()->add_segment_metas()->set_filename("s1.dat");
    op_write.add_dels_meta()->set_name("d1.del");
    op_write.add_dels_meta()->set_name("d2.del");
    op_write.add_dels_meta()->set_name("d3.del");
    // del_num_rows is parallel to dels_meta; the third is left unrecorded on purpose.
    op_write.add_del_num_rows(40);
    op_write.add_del_num_rows(60);

    builder.apply_opwrite(op_write, {}, {});

    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(3, rowset.del_files_size());
    int64_t total = 0;
    for (int i = 0; i < rowset.del_files_size(); ++i) {
        total += rowset.del_files(i).num_rows(); // absent reads as 0
    }
    EXPECT_EQ(100, total); // 40 + 60 + 0
    EXPECT_TRUE(rowset.del_files(0).has_num_rows());
    EXPECT_FALSE(rowset.del_files(2).has_num_rows());
}

// The del file checksum rules: absent means "not recorded" and is always accepted (a del file
// written before the field existed, or by the replication path), a recorded checksum is enforced,
// and the config turns verification off entirely.
TEST_F(MetaFileTest, test_verify_del_file_crc32c) {
    const std::string content = "0123456789abcdef";
    const std::string corrupted = "0123456789abcdeF";
    const uint32_t good = crc32c::Mask(crc32c::Value(content.data(), content.size()));

    FileMetaPB file_meta;
    file_meta.set_name("0000000000000001_d.del");
    EXPECT_FALSE(file_meta.has_crc32c());
    EXPECT_OK(verify_del_file_crc32c(file_meta, 42, content));
    EXPECT_OK(verify_del_file_crc32c(file_meta, 42, corrupted));

    file_meta.set_crc32c(good);
    EXPECT_OK(verify_del_file_crc32c(file_meta, 42, content));
    EXPECT_TRUE(verify_del_file_crc32c(file_meta, 42, corrupted).is_corruption());

    // Same rules on the persisted metadata type.
    DelfileWithRowsetId del_meta;
    del_meta.set_name(file_meta.name());
    EXPECT_OK(verify_del_file_crc32c(del_meta, 42, corrupted));
    del_meta.set_crc32c(good);
    EXPECT_OK(verify_del_file_crc32c(del_meta, 42, content));
    EXPECT_TRUE(verify_del_file_crc32c(del_meta, 42, corrupted).is_corruption());

    const bool old_check = config::lake_enable_del_file_crc_check;
    config::lake_enable_del_file_crc_check = false;
    EXPECT_OK(verify_del_file_crc32c(file_meta, 42, corrupted));
    EXPECT_OK(verify_del_file_crc32c(del_meta, 42, corrupted));
    config::lake_enable_del_file_crc_check = old_check;
}

// apply must carry dels_meta.crc32c into the persisted del file metadata, and leave it absent when
// the writer recorded none (older writer / replication).
TEST_F(MetaFileTest, test_apply_opwrite_del_crc32c) {
    const int64_t tablet_id = 31012;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpWrite op_write;
    op_write.mutable_rowset()->add_segment_metas()->set_filename("s1.dat");
    auto* d1 = op_write.add_dels_meta();
    d1->set_name("d1.del");
    d1->set_crc32c(0x12345678);
    op_write.add_dels_meta()->set_name("d2.del"); // no checksum recorded

    builder.apply_opwrite(op_write, {}, {});

    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(2, rowset.del_files_size());
    ASSERT_TRUE(rowset.del_files(0).has_crc32c());
    EXPECT_EQ(0x12345678u, rowset.del_files(0).crc32c());
    EXPECT_FALSE(rowset.del_files(1).has_crc32c());
}

// batch path: op_write.del_num_rows is carried through batch_apply_opwrite/set_final_rowset.
TEST_F(MetaFileTest, test_batch_apply_opwrite_del_num_rows) {
    const int64_t tablet_id = 31011;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(200);

    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpWrite op_write1;
    op_write1.mutable_rowset()->add_segment_metas()->set_filename("s1.dat");
    op_write1.add_dels_meta()->set_name("d1.del");
    op_write1.add_del_num_rows(70);
    builder.batch_apply_opwrite(op_write1, {}, {});

    TxnLogPB_OpWrite op_write2;
    op_write2.mutable_rowset()->add_segment_metas()->set_filename("s2.dat");
    op_write2.add_dels_meta()->set_name("d2.del");
    op_write2.add_del_num_rows(30);
    builder.batch_apply_opwrite(op_write2, {}, {});

    builder.set_final_rowset();

    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(2, rowset.del_files_size());
    int64_t total = 0;
    for (int i = 0; i < rowset.del_files_size(); ++i) {
        ASSERT_TRUE(rowset.del_files(i).has_num_rows());
        total += rowset.del_files(i).num_rows();
    }
    EXPECT_EQ(100, total); // 70 + 30
}

// The partial-update replace path refreshes segment_vector_index_uid wholesale from the replace
// FileInfo (apply_replace_segment): a recorded owner overwrites whatever the replaced segment
// carried, and an unset owner (-1) clears a stale one. Non-replaced segments are carried verbatim.
TEST_F(MetaFileTest, test_apply_opwrite_replace_refreshes_vector_index_owner) {
    const int64_t tablet_id = 31003;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpWrite op_write;
    auto* rowset = op_write.mutable_rowset();
    {
        // Replaced by a rewrite that recorded an owner: refreshed to the FileInfo's value.
        auto* replaced = rowset->add_segment_metas();
        replaced->set_filename("partial0.dat");
        replaced->add_vector_index_ids(100);
        // Replaced by a rewrite without vector indexes: the stale pre-set owner must be cleared.
        auto* stale_owner = rowset->add_segment_metas();
        stale_owner->set_filename("partial1.dat");
        stale_owner->add_vector_index_ids(100);
        stale_owner->set_segment_vector_index_uid(7777);
        // Not replaced: carried verbatim.
        auto* untouched = rowset->add_segment_metas();
        untouched->set_filename("untouched.dat");
        untouched->add_vector_index_ids(100);
        untouched->set_segment_vector_index_uid(9999);
    }

    std::map<int, SegmentFileInfo> replace_segments;
    SegmentFileInfo rewrite0;
    rewrite0.path = "rewrite0.dat";
    rewrite0.size = 1024;
    rewrite0.vector_index_ids.push_back(100);
    rewrite0.segment_vector_index_uid = tablet_id;
    replace_segments[0] = rewrite0;
    SegmentFileInfo rewrite1; // no ids, no owner
    rewrite1.path = "rewrite1.dat";
    rewrite1.size = 2048;
    replace_segments[1] = rewrite1;

    builder.apply_opwrite(op_write, replace_segments, {});

    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& written = metadata->rowsets(0);
    ASSERT_EQ(3, written.segment_metas_size());
    EXPECT_EQ("rewrite0.dat", written.segment_metas(0).filename());
    ASSERT_TRUE(written.segment_metas(0).has_segment_vector_index_uid());
    EXPECT_EQ(tablet_id, written.segment_metas(0).segment_vector_index_uid());
    EXPECT_EQ("rewrite1.dat", written.segment_metas(1).filename());
    EXPECT_EQ(0, written.segment_metas(1).vector_index_ids_size());
    EXPECT_FALSE(written.segment_metas(1).has_segment_vector_index_uid());
    ASSERT_TRUE(written.segment_metas(2).has_segment_vector_index_uid());
    EXPECT_EQ(9999, written.segment_metas(2).segment_vector_index_uid());
}

TEST_F(MetaFileTest, test_apply_opcompaction_delete_delvec_with_segment_id) {
    const int64_t tablet_id = 31002;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(102);

    // input rowset with sparse segment ids: rssids are 100 and 105.
    auto* input_rowset = metadata->add_rowsets();
    input_rowset->set_id(100);
    {
        auto* sm0 = input_rowset->add_segment_metas();
        sm0->set_filename("a.dat");
        sm0->set_segment_idx(0);
        auto* sm1 = input_rowset->add_segment_metas();
        sm1->set_filename("b.dat");
        sm1->set_segment_idx(5);
    }

    // neighbor rowset with rssid 101 should not be deleted.
    auto* neighbor_rowset = metadata->add_rowsets();
    neighbor_rowset->set_id(101);
    {
        auto* sm = neighbor_rowset->add_segment_metas();
        sm->set_filename("c.dat");
        sm->set_segment_idx(0);
    }

    DelvecPagePB delvec_page;
    delvec_page.set_version(10);
    delvec_page.set_offset(0);
    delvec_page.set_size(1);
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[100] = delvec_page;
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[101] = delvec_page;
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[105] = delvec_page;

    DeltaColumnGroupVerPB dcg;
    dcg.add_column_files("a.cols");
    (*metadata->mutable_dcg_meta()->mutable_dcgs())[100] = dcg;
    (*metadata->mutable_dcg_meta()->mutable_dcgs())[101] = dcg;
    (*metadata->mutable_dcg_meta()->mutable_dcgs())[105] = dcg;

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(100);
    {
        auto* sm = op_compaction.mutable_output_rowset()->add_segment_metas();
        sm->set_filename("out.dat");
        sm->set_segment_idx(0);
    }

    ASSERT_OK(builder.apply_opcompaction(op_compaction, 101, 0));

    const auto& delvecs = metadata->delvec_meta().delvecs();
    EXPECT_TRUE(delvecs.find(100) == delvecs.end());
    EXPECT_TRUE(delvecs.find(105) == delvecs.end());
    EXPECT_TRUE(delvecs.find(101) != delvecs.end());

    const auto& dcgs = metadata->dcg_meta().dcgs();
    EXPECT_TRUE(dcgs.find(100) == dcgs.end());
    EXPECT_TRUE(dcgs.find(105) == dcgs.end());
    EXPECT_TRUE(dcgs.find(101) != dcgs.end());
}

TEST_F(MetaFileTest, test_apply_opcompaction_next_rowset_id_uses_max_segment_id) {
    const int64_t tablet_id = 31003;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(200);

    auto* input_rowset = metadata->add_rowsets();
    input_rowset->set_id(100);
    {
        auto* sm = input_rowset->add_segment_metas();
        sm->set_filename("in.dat");
        sm->set_segment_idx(0);
    }

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(100);
    auto* output_rowset = op_compaction.mutable_output_rowset();
    {
        auto* sm0 = output_rowset->add_segment_metas();
        sm0->set_filename("out1.dat");
        sm0->set_segment_idx(1);
        auto* sm1 = output_rowset->add_segment_metas();
        sm1->set_filename("out2.dat");
        sm1->set_segment_idx(5);
    }

    ASSERT_OK(builder.apply_opcompaction(op_compaction, 100, 0));

    ASSERT_EQ(1, metadata->rowsets_size());
    EXPECT_EQ(200, metadata->rowsets(0).id());
    EXPECT_EQ(206, metadata->next_rowset_id());
}

TEST_F(MetaFileTest, test_batch_apply_opwrite_set_final_rowset_basic) {
    const int64_t tablet_id = 30001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);

    MetaFileBuilder builder(*tablet, metadata);

    // Batch 1: add two segments a.dat / b.dat
    TxnLogPB_OpWrite op_write1;
    RowsetMetadataPB rowset_meta1;
    rowset_meta1.add_segment_metas()->set_filename("a.dat");
    rowset_meta1.add_segment_metas()->set_filename("b.dat");
    op_write1.mutable_rowset()->CopyFrom(rowset_meta1);
    builder.batch_apply_opwrite(op_write1, /*replace_segments*/ {}, /*orphan_files*/ {});

    // Batch 2: append one segment c.dat (no cross-batch replacement to avoid OOB)
    TxnLogPB_OpWrite op_write2;
    RowsetMetadataPB rowset_meta2;
    rowset_meta2.add_segment_metas()->set_filename("c.dat");
    op_write2.mutable_rowset()->CopyFrom(rowset_meta2);
    builder.batch_apply_opwrite(op_write2, /*replace_segments*/ {}, /*orphan_files*/ {});

    // Update delete stats before finalizing; predicted segment ids start from next_rowset_id
    std::map<uint32_t, size_t> segid_to_add_dels;
    segid_to_add_dels[110] = 5; // a.dat
    segid_to_add_dels[111] = 3; // b.dat
    segid_to_add_dels[112] = 2; // c.dat
    ASSERT_TRUE(builder.update_num_del_stat(segid_to_add_dels).ok());

    // Seal pending rowset
    ASSERT_TRUE(builder.set_final_rowset().ok());
    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& final_rowset = metadata->rowsets(0);
    EXPECT_EQ(110, final_rowset.id());
    ASSERT_EQ(3, final_rowset.segment_metas_size());
    EXPECT_EQ("a.dat", final_rowset.segment_metas(0).filename());
    EXPECT_EQ("b.dat", final_rowset.segment_metas(1).filename());
    EXPECT_EQ("c.dat", final_rowset.segment_metas(2).filename());
    EXPECT_EQ(10, final_rowset.num_dels()); // 5 + 3 + 2
    EXPECT_EQ(113, metadata->next_rowset_id());

    // Persist metadata
    metadata->set_version(11);
    ASSERT_TRUE(builder.finalize(next_id()).ok());
    ASSIGN_OR_ABORT(auto persisted, _tablet_manager->get_tablet_metadata(tablet_id, 11));
    ASSERT_EQ(1, persisted->rowsets_size());
    EXPECT_EQ(10, persisted->rowsets(0).num_dels());
    EXPECT_EQ("b.dat", persisted->rowsets(0).segment_metas(1).filename());
}

TEST_F(MetaFileTest, test_batch_apply_opwrite_merge_dels) {
    const int64_t tablet_id = 30002;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(20);
    metadata->set_next_rowset_id(500);

    MetaFileBuilder builder(*tablet, metadata);

    // batch 1: two segments + two del files
    TxnLogPB_OpWrite op_write1;
    RowsetMetadataPB rowset_meta1;
    {
        auto* sm0 = rowset_meta1.add_segment_metas();
        sm0->set_filename("s1.dat");
        sm0->set_segment_idx(3);
        auto* sm1 = rowset_meta1.add_segment_metas();
        sm1->set_filename("s2.dat");
        sm1->set_segment_idx(9);
    }
    op_write1.mutable_rowset()->CopyFrom(rowset_meta1);
    op_write1.add_dels_meta()->set_name("d1.del");
    op_write1.add_dels_meta()->set_name("d2.del");
    builder.batch_apply_opwrite(op_write1, {}, {});

    // batch 2: one segment + one del file
    TxnLogPB_OpWrite op_write2;
    RowsetMetadataPB rowset_meta2;
    {
        auto* sm = rowset_meta2.add_segment_metas();
        sm->set_filename("s3.dat");
        sm->set_segment_idx(4);
    }
    op_write2.mutable_rowset()->CopyFrom(rowset_meta2);
    op_write2.add_dels_meta()->set_name("d3.del");
    builder.batch_apply_opwrite(op_write2, {}, {});

    ASSERT_TRUE(builder.set_final_rowset().ok());
    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& final_rowset = metadata->rowsets(0);
    EXPECT_EQ(500, final_rowset.id());
    ASSERT_EQ(3, final_rowset.segment_metas_size());
    EXPECT_EQ("s1.dat", final_rowset.segment_metas(0).filename());
    EXPECT_EQ("s2.dat", final_rowset.segment_metas(1).filename());
    EXPECT_EQ("s3.dat", final_rowset.segment_metas(2).filename());
    EXPECT_EQ(3, final_rowset.segment_metas(0).segment_idx());
    EXPECT_EQ(9, final_rowset.segment_metas(1).segment_idx());
    EXPECT_EQ(14, final_rowset.segment_metas(2).segment_idx());
    ASSERT_EQ(3, final_rowset.del_files_size());
    // A del's op_offset names the last segment of the op_write that PRODUCED it, in the merged
    // rowset's segment-id space -- not the merged rowset's last segment. batch 1's segments land at
    // 3 and 9, so its two dels resolve to 9; batch 2's single segment lands at 14, so its del
    // resolves to 14. Recording all three at 14 (what an unresolved -1 used to produce in
    // set_final_rowset) would sort batch 1's deletes after batch 2's segment, which is not where
    // apply put them.
    std::set<std::string> del_names;
    std::map<std::string, uint32_t> expected_op_offset{{"d1.del", 9}, {"d2.del", 9}, {"d3.del", 14}};
    for (int i = 0; i < final_rowset.del_files_size(); ++i) {
        const auto& name = final_rowset.del_files(i).name();
        del_names.insert(name);
        EXPECT_EQ(final_rowset.id(), final_rowset.del_files(i).origin_rowset_id());
        ASSERT_TRUE(expected_op_offset.count(name) > 0) << name;
        EXPECT_EQ(expected_op_offset[name], final_rowset.del_files(i).op_offset()) << name;
    }
    EXPECT_TRUE(del_names.count("d1.del") > 0);
    EXPECT_TRUE(del_names.count("d2.del") > 0);
    EXPECT_TRUE(del_names.count("d3.del") > 0);
    EXPECT_EQ(515, metadata->next_rowset_id());

    metadata->set_version(21);
    ASSERT_TRUE(builder.finalize(next_id()).ok());
    ASSIGN_OR_ABORT(auto persisted, _tablet_manager->get_tablet_metadata(tablet_id, 21));
    ASSERT_EQ(1, persisted->rowsets_size());
    ASSERT_EQ(3, persisted->rowsets(0).del_files_size());
}

TEST_F(MetaFileTest, test_batch_apply_opwrite_mixed_segment_meta_presence) {
    const int64_t tablet_id = 30003;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(30);
    metadata->set_next_rowset_id(600);

    MetaFileBuilder builder(*tablet, metadata);

    // First op_write's segment_metas omit segment_idx, so the builder must assign it positionally.
    TxnLogPB_OpWrite op_write1;
    op_write1.mutable_rowset()->add_segment_metas()->set_filename("m1.dat");
    op_write1.mutable_rowset()->add_segment_metas()->set_filename("m2.dat");
    op_write1.add_dels_meta()->set_name("d1.del");
    builder.batch_apply_opwrite(op_write1, {}, {});

    // Second op_write sets segment_idx explicitly; it must be remapped into the merged rowset's id space.
    TxnLogPB_OpWrite op_write2;
    {
        auto* sm = op_write2.mutable_rowset()->add_segment_metas();
        sm->set_filename("m3.dat");
        sm->set_segment_idx(0);
    }
    op_write2.add_dels_meta()->set_name("d2.del");
    builder.batch_apply_opwrite(op_write2, {}, {});

    ASSERT_TRUE(builder.set_final_rowset().ok());
    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& final_rowset = metadata->rowsets(0);
    ASSERT_EQ(3, final_rowset.segment_metas_size());
    EXPECT_EQ(0, final_rowset.segment_metas(0).segment_idx());
    EXPECT_EQ(1, final_rowset.segment_metas(1).segment_idx());
    EXPECT_EQ(2, final_rowset.segment_metas(2).segment_idx());
    ASSERT_EQ(2, final_rowset.del_files_size());
    // Same contract as test_batch_apply_opwrite_merge_dels: each del follows its own op_write's last
    // segment. batch 1's segments are positional 0 and 1, so d1 resolves to 1; batch 2's single
    // segment is remapped to 2, so d2 resolves to 2.
    EXPECT_EQ(1, final_rowset.del_files(0).op_offset());
    EXPECT_EQ(2, final_rowset.del_files(1).op_offset());
    EXPECT_EQ(603, metadata->next_rowset_id());
}

TEST_F(MetaFileTest, test_sstable_delvec_integration) {
    // Test SSTable delvec integration: test new get_del_vec(DelvecPagePB) function and
    // version reference collection from SSTable delvecs during finalization
    const int64_t tablet_id = 40001;
    const uint32_t segment_id = 1001;
    const int64_t version1 = 11;
    const int64_t version2 = 12;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version1);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    // 1. Create and write delvec first
    MetaFileBuilder builder1(*tablet, metadata);
    DelVector dv1;
    dv1.set_empty();
    std::shared_ptr<DelVector> ndv1;
    std::vector<uint32_t> dels1 = {1, 3, 5, 7, 100};
    dv1.add_dels_as_new_version(dels1, version1, &ndv1);
    std::string original_delvec = ndv1->save();
    builder1.append_delvec(ndv1, segment_id);
    Status st = builder1.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 2. Get the delvec page info for creating SSTable delvec
    ASSIGN_OR_ABORT(auto metadata1, _tablet_manager->get_tablet_metadata(tablet_id, version1));
    auto iter = metadata1->delvec_meta().delvecs().find(segment_id);
    EXPECT_TRUE(iter != metadata1->delvec_meta().delvecs().end());
    DelvecPagePB delvec_page = iter->second;

    // 3. Test new get_del_vec function with DelvecPagePB
    DelVector read_delvec1;
    LakeIOOptions lake_io_opts;
    EXPECT_TRUE(get_del_vec(_tablet_manager.get(), *metadata1, delvec_page, true, lake_io_opts, &read_delvec1).ok());
    EXPECT_EQ(original_delvec, read_delvec1.save());

    // 4. Create SSTable with delvec and write to version2
    metadata->set_version(version2);
    MetaFileBuilder builder2(*tablet, metadata);

    PersistentIndexSstableMetaPB sstable_meta;
    PersistentIndexSstablePB* sstable = sstable_meta.add_sstables();
    sstable->set_filename("test_sstable.sst");
    sstable->set_filesize(1024);
    sstable->set_max_rss_rowid(100);
    sstable->mutable_delvec()->CopyFrom(delvec_page); // Use DelvecPagePB instead of has_delvec

    builder2.finalize_sstable_meta(sstable_meta);
    st = builder2.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 5. Verify SSTable contains delvec information
    ASSIGN_OR_ABORT(auto metadata2, _tablet_manager->get_tablet_metadata(tablet_id, version2));
    EXPECT_EQ(1, metadata2->sstable_meta().sstables_size());
    const auto& saved_sstable = metadata2->sstable_meta().sstables(0);
    EXPECT_TRUE(saved_sstable.has_delvec());
    EXPECT_EQ(delvec_page.version(), saved_sstable.delvec().version());
    EXPECT_EQ(delvec_page.offset(), saved_sstable.delvec().offset());
    EXPECT_EQ(delvec_page.size(), saved_sstable.delvec().size());

    // 6. Test reading delvec via SSTable's delvec page
    DelVector read_delvec2;
    EXPECT_TRUE(
            get_del_vec(_tablet_manager.get(), *metadata2, saved_sstable.delvec(), true, lake_io_opts, &read_delvec2)
                    .ok());
    EXPECT_EQ(original_delvec, read_delvec2.save());

    // 7. Test version reference collection: create new metadata without regular delvec but with SSTable delvec
    auto metadata3 = std::make_shared<TabletMetadataPB>(*metadata2);
    metadata3->set_version(version2 + 1);
    metadata3->mutable_delvec_meta()->mutable_delvecs()->clear(); // Clear regular delvecs

    MetaFileBuilder builder3(*tablet, metadata3);
    st = builder3.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 8. Verify that delvec file version is preserved due to SSTable reference
    ASSIGN_OR_ABORT(auto metadata4, _tablet_manager->get_tablet_metadata(tablet_id, version2 + 1));
    auto version_to_file_map = metadata4->delvec_meta().version_to_file();

    // The delvec file with version1 should still exist because SSTable references it
    auto version_iter = version_to_file_map.find(version1);
    EXPECT_TRUE(version_iter != version_to_file_map.end());

    // 9. Verify we can still read delvec from SSTable after cleanup
    DelVector read_delvec3;
    EXPECT_TRUE(
            get_del_vec(_tablet_manager.get(), *metadata4, saved_sstable.delvec(), true, lake_io_opts, &read_delvec3)
                    .ok());
    EXPECT_EQ(original_delvec, read_delvec3.save());

    // 10. Remove SSTable delvec reference and verify delvec file cleanup
    auto metadata5 = std::make_shared<TabletMetadataPB>(*metadata4);
    metadata5->set_version(version2 + 2);
    metadata5->mutable_sstable_meta()->clear_sstables(); // Remove SSTable that references delvec

    MetaFileBuilder builder4(*tablet, metadata5);
    st = builder4.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 11. Verify that delvec file version is now removed since no SSTable references it
    ASSIGN_OR_ABORT(auto metadata6, _tablet_manager->get_tablet_metadata(tablet_id, version2 + 2));
    auto final_version_to_file_map = metadata6->delvec_meta().version_to_file();

    // The delvec file with version1 should be removed because no SSTable references it anymore
    auto final_version_iter = final_version_to_file_map.find(version1);
    EXPECT_TRUE(final_version_iter == final_version_to_file_map.end());
}
// Test that remove_compacted_sst skips SST files that appear in both input and output,
// which happens when parallel compaction's "full contain" optimization reuses input SSTs.
TEST_F(MetaFileTest, test_remove_compacted_sst_skip_reused_sst) {
    const int64_t tablet_id = 10010;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    // Setup: 3 input SSTs, where "reused.sst" appears in both input and output
    // (simulating the "full contain" optimization in parallel compaction)
    TxnLogPB_OpCompaction op_compaction;

    auto* input1 = op_compaction.add_input_sstables();
    input1->set_filename("old1.sst");
    input1->set_filesize(100);

    auto* input2 = op_compaction.add_input_sstables();
    input2->set_filename("reused.sst");
    input2->set_filesize(200);

    auto* input3 = op_compaction.add_input_sstables();
    input3->set_filename("old2.sst");
    input3->set_filesize(150);

    // Output contains "reused.sst" (full contain) and a new merged file
    auto* output1 = op_compaction.add_output_sstables();
    output1->set_filename("reused.sst");
    output1->set_filesize(200);

    auto* output2 = op_compaction.add_output_sstables();
    output2->set_filename("merged_new.sst");
    output2->set_filesize(250);

    MetaFileBuilder builder(*tablet, metadata);
    builder.remove_compacted_sst(op_compaction);

    // Verify: only "old1.sst" and "old2.sst" should be in orphan_files.
    // "reused.sst" must NOT be in orphan_files since it's also an output.
    ASSERT_EQ(metadata->orphan_files_size(), 2);
    std::set<std::string> orphan_names;
    for (const auto& f : metadata->orphan_files()) {
        orphan_names.insert(f.name());
    }
    EXPECT_TRUE(orphan_names.count("old1.sst") > 0);
    EXPECT_TRUE(orphan_names.count("old2.sst") > 0);
    EXPECT_TRUE(orphan_names.count("reused.sst") == 0);
}

// Test that remove_compacted_sst also handles output_sstable (singular, from major_compact)
TEST_F(MetaFileTest, test_remove_compacted_sst_skip_reused_sst_singular_output) {
    const int64_t tablet_id = 10011;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    TxnLogPB_OpCompaction op_compaction;

    auto* input1 = op_compaction.add_input_sstables();
    input1->set_filename("reused_single.sst");
    input1->set_filesize(100);

    auto* input2 = op_compaction.add_input_sstables();
    input2->set_filename("old.sst");
    input2->set_filesize(200);

    // Singular output_sstable reuses one of the input files
    op_compaction.mutable_output_sstable()->set_filename("reused_single.sst");
    op_compaction.mutable_output_sstable()->set_filesize(100);

    MetaFileBuilder builder(*tablet, metadata);
    builder.remove_compacted_sst(op_compaction);

    ASSERT_EQ(metadata->orphan_files_size(), 1);
    EXPECT_EQ(metadata->orphan_files(0).name(), "old.sst");
}

// Test that when no SSTs are reused, all inputs go to orphan_files (original behavior)
TEST_F(MetaFileTest, test_remove_compacted_sst_no_reuse) {
    const int64_t tablet_id = 10012;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    TxnLogPB_OpCompaction op_compaction;

    auto* input1 = op_compaction.add_input_sstables();
    input1->set_filename("a.sst");
    input1->set_filesize(100);

    auto* input2 = op_compaction.add_input_sstables();
    input2->set_filename("b.sst");
    input2->set_filesize(200);

    auto* output = op_compaction.add_output_sstables();
    output->set_filename("c.sst");
    output->set_filesize(300);

    MetaFileBuilder builder(*tablet, metadata);
    builder.remove_compacted_sst(op_compaction);

    // All inputs should be orphaned since output is a new file
    ASSERT_EQ(metadata->orphan_files_size(), 2);
    std::set<std::string> orphan_names;
    for (const auto& f : metadata->orphan_files()) {
        orphan_names.insert(f.name());
    }
    EXPECT_TRUE(orphan_names.count("a.sst") > 0);
    EXPECT_TRUE(orphan_names.count("b.sst") > 0);
}

// Test that append_delvec followed by apply_opcompaction in the same builder
// does NOT create orphan delvec entries. This reproduces the bug where a write
// txn generates a delvec for a segment that is then compacted away in the same
// publish batch. Without the fix, _finalize_delvec would re-insert the deleted
// delvec entry, creating an orphan that prevents delvec file GC.
TEST_F(MetaFileTest, test_no_orphan_delvec_after_write_then_compaction) {
    const int64_t tablet_id = 40001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // Create initial metadata on disk
    {
        MetaFileBuilder builder(*tablet, metadata);
        ASSERT_OK(builder.finalize(next_id()));
    }

    // Add two rowsets: rowset 100 (segment "a.dat") and rowset 101 (segment "b.dat")
    {
        metadata->set_version(11);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segment_metas()->set_filename("a.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }
    {
        metadata->set_version(12);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segment_metas()->set_filename("b.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }

    // Now metadata has rowsets: id=100 (seg "a.dat"), id=101 (seg "b.dat")
    ASSERT_EQ(2, metadata->rowsets_size());
    ASSERT_EQ(100, metadata->rowsets(0).id());
    ASSERT_EQ(101, metadata->rowsets(1).id());

    // Simulate a publish batch where:
    // 1) A write txn creates a delvec for segment 100 (belonging to rowset 100)
    // 2) A compaction txn compacts rowset 100 away
    // Both operations use the same MetaFileBuilder (batch publish).
    {
        metadata->set_version(13);
        MetaFileBuilder builder(*tablet, metadata);

        // Step 1: Write txn generates a delvec for segment 100
        DelVector dv;
        dv.set_empty();
        std::shared_ptr<DelVector> ndv;
        std::vector<uint32_t> dels = {1, 3, 5};
        dv.add_dels_as_new_version(dels, 13, &ndv);
        builder.append_delvec(ndv, 100); // segment_id = 100, belongs to rowset 100

        // Step 2: Compaction removes rowset 100
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(100);
        RowsetMetadataPB output_rs;
        output_rs.add_segment_metas()->set_filename("compacted.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(output_rs);
        ASSERT_OK(builder.apply_opcompaction(op_compaction, 100, 0));

        // Step 3: Finalize - this is where the bug would manifest
        ASSERT_OK(builder.finalize(next_id()));

        // Verify: segment 100's delvec should NOT exist in metadata (it was compacted away)
        const auto& delvecs_map = metadata->delvec_meta().delvecs();
        EXPECT_TRUE(delvecs_map.find(100) == delvecs_map.end())
                << "Orphan delvec entry found for compacted segment 100";

        // Verify: rowset 100 should be gone, only rowset 101 and the new compaction output remain
        EXPECT_EQ(2, metadata->rowsets_size());

        // Verify: version_to_file should not hold unreferenced entries
        for (const auto& vtf_entry : metadata->delvec_meta().version_to_file()) {
            bool referenced = false;
            for (const auto& dv_entry : delvecs_map) {
                if (dv_entry.second.version() == vtf_entry.first) {
                    referenced = true;
                    break;
                }
            }
            EXPECT_TRUE(referenced) << "version_to_file entry for version " << vtf_entry.first
                                    << " is not referenced by any delvec";
        }
    }
}

// Test the orphan delvec scenario with multiple segments in the compacted rowset.
// The write txn creates delvecs for two segments of the input rowset, both should
// be cleaned up when the rowset is compacted.
TEST_F(MetaFileTest, test_no_orphan_delvec_multi_segment_compaction) {
    const int64_t tablet_id = 40002;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(200);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // Create initial metadata
    {
        MetaFileBuilder builder(*tablet, metadata);
        ASSERT_OK(builder.finalize(next_id()));
    }

    // Add a rowset with 2 segments (rowset id=200, segments at rssid 200 and 201)
    {
        metadata->set_version(11);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segment_metas()->set_filename("seg0.dat");
        rs.add_segment_metas()->set_filename("seg1.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }

    // Add a second rowset (rowset id=202)
    {
        metadata->set_version(12);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segment_metas()->set_filename("other.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }

    ASSERT_EQ(2, metadata->rowsets_size());
    ASSERT_EQ(200, metadata->rowsets(0).id());
    ASSERT_EQ(202, metadata->rowsets(1).id());

    // Simulate batch publish: write creates delvecs for both segments of rowset 200,
    // then compaction removes rowset 200
    {
        metadata->set_version(13);
        MetaFileBuilder builder(*tablet, metadata);

        // Write creates delvecs for both segments
        DelVector dv1;
        dv1.set_empty();
        std::shared_ptr<DelVector> ndv1;
        std::vector<uint32_t> dels1 = {10, 20};
        dv1.add_dels_as_new_version(dels1, 13, &ndv1);
        builder.append_delvec(ndv1, 200);

        DelVector dv2;
        dv2.set_empty();
        std::shared_ptr<DelVector> ndv2;
        std::vector<uint32_t> dels2 = {30, 40};
        dv2.add_dels_as_new_version(dels2, 13, &ndv2);
        builder.append_delvec(ndv2, 201);

        // Compaction removes rowset 200
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(200);
        RowsetMetadataPB output_rs;
        output_rs.add_segment_metas()->set_filename("compacted.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(output_rs);
        ASSERT_OK(builder.apply_opcompaction(op_compaction, 200, 0));

        ASSERT_OK(builder.finalize(next_id()));

        // Both delvec entries for compacted segments should be gone
        const auto& delvecs_map = metadata->delvec_meta().delvecs();
        EXPECT_TRUE(delvecs_map.find(200) == delvecs_map.end()) << "Orphan delvec for segment 200";
        EXPECT_TRUE(delvecs_map.find(201) == delvecs_map.end()) << "Orphan delvec for segment 201";
    }
}

// Test that pre-existing orphan delvec entries in metadata are cleaned up
// during compaction. This simulates the upgrade scenario where orphan entries
// accumulated from the historical bug are present in the metadata.
TEST_F(MetaFileTest, test_cleanup_preexisting_orphan_delvecs_on_compaction) {
    const int64_t tablet_id = 40003;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(300);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // Create initial metadata
    {
        MetaFileBuilder builder(*tablet, metadata);
        ASSERT_OK(builder.finalize(next_id()));
    }

    // Add two rowsets: id=300 ("a.dat") and id=301 ("b.dat")
    {
        metadata->set_version(11);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segment_metas()->set_filename("a.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }
    {
        metadata->set_version(12);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segment_metas()->set_filename("b.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }

    ASSERT_EQ(2, metadata->rowsets_size());
    ASSERT_EQ(300, metadata->rowsets(0).id());
    ASSERT_EQ(301, metadata->rowsets(1).id());

    // Manually inject orphan delvec entries into metadata to simulate
    // pre-existing orphans from the historical bug.
    // Segment IDs 100, 200, 999 do not belong to any current rowset.
    DelvecPagePB orphan_page;
    orphan_page.set_version(5);
    orphan_page.set_offset(0);
    orphan_page.set_size(10);
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[100] = orphan_page;
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[200] = orphan_page;
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[999] = orphan_page;

    // Also add a valid delvec for existing segment 301
    DelvecPagePB valid_page;
    valid_page.set_version(12);
    valid_page.set_offset(0);
    valid_page.set_size(10);
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[301] = valid_page;

    // Add version_to_file entries referenced by orphans
    FileMetaPB file_meta;
    file_meta.set_name("old_orphan.delvec");
    file_meta.set_size(100);
    (*metadata->mutable_delvec_meta()->mutable_version_to_file())[5] = file_meta;

    EXPECT_EQ(4, metadata->delvec_meta().delvecs().size()); // 3 orphans + 1 valid

    // Enable orphan cleanup config for this test
    config::lake_enable_orphan_delvec_cleanup_on_compaction = true;

    // Compact rowset 300 — this triggers orphan cleanup in apply_opcompaction
    {
        metadata->set_version(13);
        MetaFileBuilder builder(*tablet, metadata);
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(300);
        RowsetMetadataPB output_rs;
        output_rs.add_segment_metas()->set_filename("compacted.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(output_rs);
        ASSERT_OK(builder.apply_opcompaction(op_compaction, 300, 0));
        ASSERT_OK(builder.finalize(next_id()));

        // All orphan entries should be removed by the compaction cleanup
        const auto& delvecs_map = metadata->delvec_meta().delvecs();
        EXPECT_TRUE(delvecs_map.find(100) == delvecs_map.end()) << "Orphan segment 100 should be cleaned";
        EXPECT_TRUE(delvecs_map.find(200) == delvecs_map.end()) << "Orphan segment 200 should be cleaned";
        EXPECT_TRUE(delvecs_map.find(999) == delvecs_map.end()) << "Orphan segment 999 should be cleaned";
        // Segment 300's delvec was also removed (rowset 300 was compacted, had no delvec anyway)
        EXPECT_TRUE(delvecs_map.find(300) == delvecs_map.end());

        // Valid entry for segment 301 (non-compacted rowset) should remain
        EXPECT_TRUE(delvecs_map.find(301) != delvecs_map.end()) << "Valid segment 301 should be kept";
        EXPECT_EQ(1, delvecs_map.size());

        // The orphan version_to_file entry (version=5) should now be unreferenced and removed
        const auto& vtf = metadata->delvec_meta().version_to_file();
        EXPECT_TRUE(vtf.find(5) == vtf.end()) << "version_to_file entry for orphan version 5 should be cleaned up";
    }

    config::lake_enable_orphan_delvec_cleanup_on_compaction = false;
}

// Verify that remove_compacted_sst takes the shared flag from tablet metadata
// rather than the txn log, because during tablet split the txn log value may
// have lost the shared=true marking.
TEST_F(MetaFileTest, test_remove_compacted_sst_shared_from_metadata) {
    const int64_t tablet_id = 10020;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    // Pre-populate sstable_meta in tablet metadata with shared=true
    auto* sst_in_meta = metadata->mutable_sstable_meta()->add_sstables();
    sst_in_meta->set_filename("shared.sst");
    sst_in_meta->set_filesize(100);
    sst_in_meta->set_shared(true);

    // Build an OpCompaction where the input_sstable has shared=false (lost during cross-publish)
    TxnLogPB_OpCompaction op_compaction;
    auto* input = op_compaction.add_input_sstables();
    input->set_filename("shared.sst");
    input->set_filesize(100);
    input->set_shared(false); // incorrect value from txn log

    auto* output = op_compaction.add_output_sstables();
    output->set_filename("new_output.sst");
    output->set_filesize(200);

    MetaFileBuilder builder(*tablet, metadata);
    builder.remove_compacted_sst(op_compaction);

    // The orphan file should have shared=true from metadata, not false from txn log
    ASSERT_EQ(metadata->orphan_files_size(), 1);
    EXPECT_EQ(metadata->orphan_files(0).name(), "shared.sst");
    EXPECT_TRUE(metadata->orphan_files(0).shared());
}

// Verify that remove_compacted_sst falls back to the txn log shared flag
// when the SST is not found in tablet metadata.
TEST_F(MetaFileTest, test_remove_compacted_sst_shared_fallback_to_txn_log) {
    const int64_t tablet_id = 10021;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    // No matching SST in metadata sstable_meta (empty)

    TxnLogPB_OpCompaction op_compaction;
    auto* input = op_compaction.add_input_sstables();
    input->set_filename("only_in_txnlog.sst");
    input->set_filesize(150);
    input->set_shared(true); // value only in txn log

    auto* output = op_compaction.add_output_sstables();
    output->set_filename("new_output.sst");
    output->set_filesize(200);

    MetaFileBuilder builder(*tablet, metadata);
    builder.remove_compacted_sst(op_compaction);

    // Should use the txn log value since SST not found in metadata
    ASSERT_EQ(metadata->orphan_files_size(), 1);
    EXPECT_EQ(metadata->orphan_files(0).name(), "only_in_txnlog.sst");
    EXPECT_TRUE(metadata->orphan_files(0).shared());
}

// Verify apply_opwrite copies op_write.shared_dels into the per-del shared flag on
// rowset.del_files. Regression guard for the cross-publish path where new dels in an
// in-flight write are shared across sibling split tablets.
TEST_F(MetaFileTest, test_apply_opwrite_preserves_shared_dels) {
    const int64_t tablet_id = 10030;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    TxnLogPB_OpWrite op_write;
    op_write.mutable_rowset()->add_segment_metas()->set_filename("seg.dat");
    {
        auto* d0 = op_write.add_dels_meta();
        d0->set_name("shared_d.del");
        d0->set_shared(true);
        auto* d1 = op_write.add_dels_meta();
        d1->set_name("private_d.del");
        d1->set_shared(false);
    }

    MetaFileBuilder builder(*tablet, metadata);
    builder.apply_opwrite(op_write, {}, {});

    ASSERT_EQ(metadata->rowsets_size(), 1);
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(rowset.del_files_size(), 2);
    EXPECT_EQ(rowset.del_files(0).name(), "shared_d.del");
    EXPECT_TRUE(rowset.del_files(0).shared());
    EXPECT_EQ(rowset.del_files(1).name(), "private_d.del");
    EXPECT_FALSE(rowset.del_files(1).shared());
}

// Verify backward compatibility: when op_write.shared_dels is empty (old txn log or
// non-cross-publish path), apply_opwrite leaves del_files[i].shared unset (default false).
TEST_F(MetaFileTest, test_apply_opwrite_empty_shared_dels_defaults_false) {
    const int64_t tablet_id = 10031;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    TxnLogPB_OpWrite op_write;
    op_write.mutable_rowset()->add_segment_metas()->set_filename("seg.dat");
    op_write.add_dels_meta()->set_name("d.del");
    // shared_dels left empty — legacy/normal write

    MetaFileBuilder builder(*tablet, metadata);
    builder.apply_opwrite(op_write, {}, {});

    ASSERT_EQ(metadata->rowsets_size(), 1);
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(rowset.del_files_size(), 1);
    EXPECT_FALSE(rowset.del_files(0).shared());
}

// Verify batch_apply_opwrite + set_final_rowset preserve shared_dels across multiple
// op_writes merged into a single rowset.
TEST_F(MetaFileTest, test_batch_apply_opwrite_preserves_shared_dels) {
    const int64_t tablet_id = 10032;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    MetaFileBuilder builder(*tablet, metadata);

    // First opwrite: 1 shared del
    TxnLogPB_OpWrite op_write1;
    op_write1.mutable_rowset()->add_segment_metas()->set_filename("seg1.dat");
    {
        auto* d = op_write1.add_dels_meta();
        d->set_name("d1.del");
        d->set_shared(true);
    }
    builder.batch_apply_opwrite(op_write1, {}, {});

    // Second opwrite: no shared_dels set (legacy / private)
    TxnLogPB_OpWrite op_write2;
    op_write2.mutable_rowset()->add_segment_metas()->set_filename("seg2.dat");
    op_write2.add_dels_meta()->set_name("d2.del");
    builder.batch_apply_opwrite(op_write2, {}, {});

    // Third opwrite: 1 shared del
    TxnLogPB_OpWrite op_write3;
    op_write3.mutable_rowset()->add_segment_metas()->set_filename("seg3.dat");
    {
        auto* d = op_write3.add_dels_meta();
        d->set_name("d3.del");
        d->set_shared(true);
    }
    builder.batch_apply_opwrite(op_write3, {}, {});

    ASSERT_OK(builder.set_final_rowset());

    ASSERT_EQ(metadata->rowsets_size(), 1);
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(rowset.del_files_size(), 3);
    EXPECT_EQ(rowset.del_files(0).name(), "d1.del");
    EXPECT_TRUE(rowset.del_files(0).shared());
    EXPECT_EQ(rowset.del_files(1).name(), "d2.del");
    EXPECT_FALSE(rowset.del_files(1).shared());
    EXPECT_EQ(rowset.del_files(2).name(), "d3.del");
    EXPECT_TRUE(rowset.del_files(2).shared());
}

// Verify that apply_opwrite clears shared_segments[i] to false for segments replaced
// by partial-update rewrite files. The rewrite file is private to this tablet and
// must not be routed through the shared-file GC path.
TEST_F(MetaFileTest, test_apply_opwrite_clears_shared_segments_for_rewrite) {
    const int64_t tablet_id = 10040;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // Build op_write with 2 segments, both marked shared (simulating post-cross-publish).
    TxnLogPB_OpWrite op_write;
    auto* src_rowset = op_write.mutable_rowset();
    {
        auto* sm0 = src_rowset->add_segment_metas();
        sm0->set_filename("orig0.dat");
        sm0->set_size(1000);
        sm0->set_shared(true);
        auto* sm1 = src_rowset->add_segment_metas();
        sm1->set_filename("orig1.dat");
        sm1->set_size(1000);
        sm1->set_shared(true);
    }

    // Partial update: segment 0 is rewritten into a new private file.
    std::map<int, SegmentFileInfo> replace_segments;
    SegmentFileInfo rewrite_info;
    rewrite_info.path = "rewrite0.dat";
    rewrite_info.size = 1500;
    replace_segments[0] = rewrite_info;

    MetaFileBuilder builder(*tablet, metadata);
    builder.apply_opwrite(op_write, replace_segments, {});

    ASSERT_EQ(metadata->rowsets_size(), 1);
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(rowset.segment_metas_size(), 2);
    // Segment 0 was rewritten: filename updated AND shared flag cleared.
    EXPECT_EQ(rowset.segment_metas(0).filename(), "rewrite0.dat");
    EXPECT_FALSE(rowset.segment_metas(0).shared());
    // Segment 1 was not touched: shared flag preserved.
    EXPECT_EQ(rowset.segment_metas(1).filename(), "orig1.dat");
    EXPECT_TRUE(rowset.segment_metas(1).shared());
}

// Verify the same behavior for the batched path (batch_apply_opwrite + set_final_rowset).
TEST_F(MetaFileTest, test_batch_apply_opwrite_clears_shared_segments_for_rewrite) {
    const int64_t tablet_id = 10041;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    TxnLogPB_OpWrite op_write;
    auto* src_rowset = op_write.mutable_rowset();
    {
        auto* sm0 = src_rowset->add_segment_metas();
        sm0->set_filename("orig0.dat");
        sm0->set_size(1000);
        sm0->set_shared(true);
        auto* sm1 = src_rowset->add_segment_metas();
        sm1->set_filename("orig1.dat");
        sm1->set_size(1000);
        sm1->set_shared(true);
    }

    std::map<int, SegmentFileInfo> replace_segments;
    SegmentFileInfo rewrite_info;
    rewrite_info.path = "rewrite0.dat";
    rewrite_info.size = 1500;
    replace_segments[0] = rewrite_info;

    MetaFileBuilder builder(*tablet, metadata);
    builder.batch_apply_opwrite(op_write, replace_segments, {});
    ASSERT_OK(builder.set_final_rowset());

    ASSERT_EQ(metadata->rowsets_size(), 1);
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(rowset.segment_metas_size(), 2);
    EXPECT_EQ(rowset.segment_metas(0).filename(), "rewrite0.dat");
    EXPECT_FALSE(rowset.segment_metas(0).shared());
    EXPECT_EQ(rowset.segment_metas(1).filename(), "orig1.dat");
    EXPECT_TRUE(rowset.segment_metas(1).shared());
}

// --- Lake IDG (ADD/DROP INDEX fast path) --------------------------------

namespace {
void push_segment_entry(TxnLogPB_OpAddIndex* op, uint32_t seg_id, int64_t version, const std::string& idx_file,
                        int32_t col_uid, IndexType type, bool set_seg_id = true) {
    auto* se = op->add_segment_entries();
    if (set_seg_id) {
        se->set_segment_id(seg_id);
    }
    auto* e = se->mutable_entry();
    e->set_index_file(idx_file);
    e->set_version(version);
    auto* k = e->add_keys();
    k->set_col_unique_id(col_uid);
    k->set_index_type(type);
}

void push_dropped(TxnLogPB_OpDropIndex* op, int64_t index_id, int32_t col_uid, IndexType type, bool set_col_uid = true,
                  bool set_type = true) {
    auto* d = op->add_dropped();
    d->set_index_id(index_id);
    if (set_col_uid) {
        d->set_col_unique_id(col_uid);
    }
    if (set_type) {
        d->set_index_type(type);
    }
}
} // namespace

TEST_F(MetaFileTest, test_apply_add_index_happy_path) {
    // Two segments each get one IDG entry; schema gains the corresponding
    // TabletIndexPB entry (idempotent reconciliation with FE schema publish).
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20001);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20001);
    metadata->set_version(5);
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    op.set_alter_version(6);
    push_segment_entry(&op, /*seg_id=*/0, /*version=*/6, "idx_seg0.idx", 100, BITMAP);
    push_segment_entry(&op, /*seg_id=*/1, /*version=*/6, "idx_seg1.idx", 100, BITMAP);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(7001);
    new_ix->set_index_type(BITMAP);
    new_ix->add_col_unique_id(100);

    ASSERT_OK(builder.apply_add_index(op));

    ASSERT_TRUE(metadata->has_idg_meta());
    const auto& idgs = metadata->idg_meta().idgs();
    ASSERT_EQ(2u, idgs.size());
    EXPECT_EQ("idx_seg0.idx", idgs.at(0).entries(0).index_file());
    EXPECT_EQ("idx_seg1.idx", idgs.at(1).entries(0).index_file());

    ASSERT_EQ(1, metadata->schema().table_indices_size());
    EXPECT_EQ(7001, metadata->schema().table_indices(0).index_id());
}

TEST_F(MetaFileTest, test_apply_add_index_missing_segment_id_skipped) {
    // A segment_entry missing segment_id would index the map at default 0
    // and corrupt segment 0's IDG — the builder must skip it.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20002);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20002);
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    // malformed — no segment_id
    push_segment_entry(&op, /*seg_id=*/999, /*version=*/1, "bogus.idx", 1, BITMAP, /*set_seg_id=*/false);
    // well-formed — should land
    push_segment_entry(&op, /*seg_id=*/3, /*version=*/1, "good.idx", 1, BITMAP);

    ASSERT_OK(builder.apply_add_index(op));

    const auto& idgs = metadata->idg_meta().idgs();
    ASSERT_EQ(1u, idgs.size());
    EXPECT_TRUE(idgs.find(0) == idgs.end());
    ASSERT_NE(idgs.find(3), idgs.end());
    EXPECT_EQ("good.idx", idgs.at(3).entries(0).index_file());
}

TEST_F(MetaFileTest, test_apply_add_index_merges_newest_first) {
    // Second apply prepends the newer entry; the per-segment entries list
    // becomes [new, old]. Mirrors DCG reverse-by-version ordering.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20003);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20003);
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op1;
    push_segment_entry(&op1, 5, /*version=*/10, "old.idx", 1, BITMAP);
    ASSERT_OK(builder.apply_add_index(op1));

    TxnLogPB_OpAddIndex op2;
    push_segment_entry(&op2, 5, /*version=*/11, "new.idx", 1, BITMAP);
    ASSERT_OK(builder.apply_add_index(op2));

    const auto& v = metadata->idg_meta().idgs().at(5);
    ASSERT_EQ(2, v.entries_size());
    EXPECT_EQ("new.idx", v.entries(0).index_file());
    EXPECT_EQ("old.idx", v.entries(1).index_file());
}

TEST_F(MetaFileTest, test_apply_add_index_merges_newest_first_multi) {
    // Three or more sequential applies must preserve strict newest-first
    // ordering: [v3, v2, v1]. Reader short-circuits on the first matching
    // entry, so a regression here silently resolves a column against a
    // stale .idx payload.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20013);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20013);
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op1;
    push_segment_entry(&op1, /*seg_id=*/7, /*version=*/10, "v1.idx", /*index_id=*/100, BITMAP);
    ASSERT_OK(builder.apply_add_index(op1));

    TxnLogPB_OpAddIndex op2;
    push_segment_entry(&op2, /*seg_id=*/7, /*version=*/11, "v2.idx", /*index_id=*/101, BITMAP);
    ASSERT_OK(builder.apply_add_index(op2));

    TxnLogPB_OpAddIndex op3;
    push_segment_entry(&op3, /*seg_id=*/7, /*version=*/12, "v3.idx", /*index_id=*/102, BITMAP);
    ASSERT_OK(builder.apply_add_index(op3));

    TxnLogPB_OpAddIndex op4;
    push_segment_entry(&op4, /*seg_id=*/7, /*version=*/13, "v4.idx", /*index_id=*/103, BITMAP);
    ASSERT_OK(builder.apply_add_index(op4));

    const auto& v = metadata->idg_meta().idgs().at(7);
    ASSERT_EQ(4, v.entries_size());
    EXPECT_EQ("v4.idx", v.entries(0).index_file());
    EXPECT_EQ("v3.idx", v.entries(1).index_file());
    EXPECT_EQ("v2.idx", v.entries(2).index_file());
    EXPECT_EQ("v1.idx", v.entries(3).index_file());
    // Verify per-entry versions stay strictly decreasing so the reader's
    // version-based tie-breaker sees the same order as the array order.
    EXPECT_EQ(13, v.entries(0).version());
    EXPECT_EQ(12, v.entries(1).version());
    EXPECT_EQ(11, v.entries(2).version());
    EXPECT_EQ(10, v.entries(3).version());
}

TEST_F(MetaFileTest, test_apply_add_index_empty_op_is_pure_noop) {
    // The rollup no-op shape: FE sends an empty index set for a materialized
    // index whose schema lacks the indexed column(s); do_process writes a txn
    // log whose op_add_index carries only alter_version. Applying it must leave
    // the tablet metadata untouched (the version advance itself happens in the
    // generic publish machinery): no idg_meta materialized, schema id/content
    // unchanged, no historical archiving, no rowset pins.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20031);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20031);
    metadata->set_version(7);
    metadata->mutable_schema()->set_id(300);
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    op.set_alter_version(7);
    ASSERT_OK(builder.apply_add_index(op));

    EXPECT_FALSE(metadata->has_idg_meta());
    EXPECT_EQ(300, metadata->schema().id());
    EXPECT_EQ(0, metadata->schema().table_indices_size());
    EXPECT_TRUE(metadata->historical_schemas().empty());
    EXPECT_TRUE(metadata->rowset_to_schema().empty());
}

TEST_F(MetaFileTest, test_apply_add_index_stamps_new_schema_id) {
    // When FE allocates a new schema id/version (durability fix), apply_add_index
    // must stamp it onto metadata->schema() so every by-id schema cache misses and
    // picks up the indexed schema. On a fresh table (empty rowset_to_schema) no
    // historical archiving is needed — existing rowsets resolve via schema().
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20021);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20021);
    metadata->set_version(5);
    metadata->mutable_schema()->set_id(100);
    metadata->mutable_schema()->set_schema_version(5);
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    op.set_alter_version(6);
    push_segment_entry(&op, /*seg_id=*/0, /*version=*/6, "s0.idx", 100, BITMAP);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(7001);
    new_ix->set_index_type(BITMAP);
    new_ix->add_col_unique_id(100);
    op.set_new_schema_id(200);
    op.set_new_schema_version(6);

    ASSERT_OK(builder.apply_add_index(op));

    EXPECT_EQ(200, metadata->schema().id());
    EXPECT_EQ(6, metadata->schema().schema_version());
    // Empty rowset_to_schema: no pins, no historical archiving.
    EXPECT_TRUE(metadata->rowset_to_schema().empty());
    EXPECT_EQ(0u, metadata->historical_schemas().count(200));
}

TEST_F(MetaFileTest, test_apply_add_index_evolved_table_archives_and_repoints) {
    // Fast-evolved table (non-empty rowset_to_schema): existing rowsets are pinned
    // to a historical schema, so both the read path and compaction resolve through
    // historical_schemas — bumping metadata->schema() alone is bypassed. The fix
    // must archive the indexed schema under the new id AND repoint the pins that
    // referenced the pre-index current schema (100) to it, while leaving pins to an
    // OLDER schema (50) untouched. rowset.cpp CHECKs that a pinned schema id exists
    // in historical_schemas, so the archive and the repoint must happen together.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20022);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20022);
    metadata->set_version(5);
    metadata->mutable_schema()->set_id(100); // pre-index current schema
    metadata->mutable_schema()->set_schema_version(5);
    // rowsets 1,2 pinned to the current schema (100); rowset 3 to an older one (50).
    (*metadata->mutable_rowset_to_schema())[1] = 100;
    (*metadata->mutable_rowset_to_schema())[2] = 100;
    (*metadata->mutable_rowset_to_schema())[3] = 50;
    (*metadata->mutable_historical_schemas())[100].set_id(100);
    (*metadata->mutable_historical_schemas())[50].set_id(50);
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    op.set_alter_version(6);
    push_segment_entry(&op, /*seg_id=*/0, /*version=*/6, "s0.idx", 100, BITMAP);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(7001);
    new_ix->set_index_type(BITMAP);
    new_ix->add_col_unique_id(100);
    op.set_new_schema_id(200);
    op.set_new_schema_version(6);

    ASSERT_OK(builder.apply_add_index(op));

    EXPECT_EQ(200, metadata->schema().id());
    // Indexed schema archived under the new id (pins to it must resolve).
    ASSERT_EQ(1u, metadata->historical_schemas().count(200));
    EXPECT_EQ(200, metadata->historical_schemas().at(200).id());
    // Pins on the pre-index current schema (100) repointed to the indexed id.
    EXPECT_EQ(200, metadata->rowset_to_schema().at(1));
    EXPECT_EQ(200, metadata->rowset_to_schema().at(2));
    // Pin to an OLDER (fewer-column) schema is untouched.
    EXPECT_EQ(50, metadata->rowset_to_schema().at(3));

    // Idempotent replay: re-applying the same op is a no-op (guarded on
    // historical_schemas already containing the new id).
    ASSERT_OK(builder.apply_add_index(op));
    EXPECT_EQ(200, metadata->schema().id());
    EXPECT_EQ(200, metadata->rowset_to_schema().at(1));
    EXPECT_EQ(50, metadata->rowset_to_schema().at(3));
    EXPECT_EQ(1u, metadata->historical_schemas().count(200));
}

TEST_F(MetaFileTest, test_apply_drop_index_populates_tombstone) {
    // Dropping an index from schema.table_indices must also copy the
    // TabletIndexPB into schema.dropped_table_indices so BE readers know
    // the footer payload (e.g. legacy NGRAMBF bloom) is stale until
    // compaction rewrites the segment.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20004);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20004);
    auto* schema = metadata->mutable_schema();
    auto* idx = schema->add_table_indices();
    idx->set_index_id(3001);
    idx->set_index_type(NGRAMBF);
    idx->add_col_unique_id(7);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpDropIndex op;
    push_dropped(&op, /*index_id=*/3001, /*col_uid=*/7, NGRAMBF);
    builder.apply_drop_index(op);

    EXPECT_EQ(0, schema->table_indices_size());
    ASSERT_EQ(1, schema->dropped_table_indices_size());
    EXPECT_EQ(3001, schema->dropped_table_indices(0).index_id());
    EXPECT_EQ(NGRAMBF, schema->dropped_table_indices(0).index_type());
}

TEST_F(MetaFileTest, test_apply_drop_index_tombstone_dedup) {
    // Dropping the same index_id twice (replay of a legacy log) must not
    // duplicate the tombstone entry.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20005);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20005);
    auto* schema = metadata->mutable_schema();
    auto* idx = schema->add_table_indices();
    idx->set_index_id(3002);
    idx->set_index_type(BITMAP);
    idx->add_col_unique_id(8);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpDropIndex op;
    push_dropped(&op, 3002, 8, BITMAP);

    builder.apply_drop_index(op);
    // Second apply: schema.table_indices already empty; tombstone stays at one.
    builder.apply_drop_index(op);

    EXPECT_EQ(0, schema->table_indices_size());
    EXPECT_EQ(1, schema->dropped_table_indices_size());
}

TEST_F(MetaFileTest, test_apply_drop_index_skips_malformed_entries) {
    // Drop entries missing col_unique_id or index_type must not feed the
    // drop_keys set — default 0 / INDEX_UNKNOWN would fabricate a matching
    // key for unrelated entries. Index_id-based removal still proceeds.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20006);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20006);
    auto* schema = metadata->mutable_schema();
    auto* idx = schema->add_table_indices();
    idx->set_index_id(3003);
    idx->set_index_type(BITMAP);
    idx->add_col_unique_id(9);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpDropIndex op;
    // malformed — no col_unique_id
    push_dropped(&op, /*index_id=*/3003, /*col_uid=*/9, BITMAP, /*set_col_uid=*/false, /*set_type=*/true);
    // malformed — no index_type
    push_dropped(&op, /*index_id=*/3003, /*col_uid=*/9, BITMAP, /*set_col_uid=*/true, /*set_type=*/false);

    builder.apply_drop_index(op);

    // Index_id 3003 still got removed from active list (index_id-based removal
    // is independent of the drop_keys set), and tombstone list has exactly one
    // entry for it — no bogus (0, INDEX_UNKNOWN) key was produced.
    EXPECT_EQ(0, schema->table_indices_size());
    EXPECT_EQ(1, schema->dropped_table_indices_size());
}

// --- Per-column flag flip on apply_add_index (the R3 fix) -----------------

namespace {
ColumnPB* push_column(TabletSchemaPB* schema, int col_uid, const std::string& name) {
    auto* c = schema->add_column();
    c->set_unique_id(col_uid);
    c->set_name(name);
    c->set_type("INT");
    c->set_has_bitmap_index(false);
    c->set_is_bf_column(false);
    return c;
}
} // namespace

TEST_F(MetaFileTest, test_apply_add_index_flips_has_bitmap_index_flag) {
    // BITMAP add must flip column.has_bitmap_index=true so a future
    // compaction inlines the bitmap into the new segment footer.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20100);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20100);
    auto* schema = metadata->mutable_schema();
    push_column(schema, /*col_uid=*/42, "c1");
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(7777);
    new_ix->set_index_type(BITMAP);
    new_ix->add_col_unique_id(42);
    ASSERT_OK(builder.apply_add_index(op));

    ASSERT_EQ(1, schema->table_indices_size());
    EXPECT_TRUE(schema->column(0).has_bitmap_index());
    EXPECT_FALSE(schema->column(0).is_bf_column());
}

TEST_F(MetaFileTest, test_apply_add_index_flips_is_bf_column_for_ngrambf) {
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20101);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20101);
    auto* schema = metadata->mutable_schema();
    push_column(schema, /*col_uid=*/55, "v1");
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(8888);
    new_ix->set_index_type(NGRAMBF);
    new_ix->add_col_unique_id(55);
    ASSERT_OK(builder.apply_add_index(op));

    EXPECT_FALSE(schema->column(0).has_bitmap_index());
    EXPECT_TRUE(schema->column(0).is_bf_column());
}

TEST_F(MetaFileTest, test_apply_add_index_flips_is_bf_column_for_bloom_filter) {
    // Plain BLOOM_FILTER (the bf_columns property fast path) also needs
    // the per-column flag flipped.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20102);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20102);
    auto* schema = metadata->mutable_schema();
    push_column(schema, /*col_uid=*/77, "v2");
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_type(BLOOM_FILTER);
    new_ix->add_col_unique_id(77);
    ASSERT_OK(builder.apply_add_index(op));

    EXPECT_TRUE(schema->column(0).is_bf_column());
}

// ---------------------------------------------------------------------------
// Installing the authoritative schema content (StarRocksTest#12090).
//
// Under fast schema evolution v2 a metadata-only ADD COLUMN updates only the FE
// catalog; tablet metadata catches up on the next write naming a newer schema.
// A fast-path ADD INDEX dispatched inside that window carries the full column
// definitions so publish does not bind FE's new schema id to content that is
// still missing the added column.
// ---------------------------------------------------------------------------

TEST_F(MetaFileTest, test_apply_add_index_installs_new_schema_content_with_new_id) {
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20110);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20110);
    metadata->set_version(4);
    auto* schema = metadata->mutable_schema();
    schema->set_id(500);
    schema->set_schema_version(3);
    push_column(schema, /*col_uid=*/42, "c1");

    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    // FE's catalog schema: c1 plus the ALTER-added c5. Its own id (999) is the
    // catalog id, which is neither the pre-apply id nor the id this alter
    // publishes -- apply must not leak it into the metadata.
    auto* fe_schema = op.mutable_new_schema();
    fe_schema->set_id(999);
    fe_schema->set_schema_version(4);
    push_column(fe_schema, /*col_uid=*/42, "c1");
    auto* c5 = push_column(fe_schema, /*col_uid=*/105, "c5");
    c5->set_is_nullable(true);
    c5->set_default_value("0");
    op.set_new_schema_id(777);
    op.set_new_schema_version(4);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_type(BLOOM_FILTER);
    new_ix->add_col_unique_id(105);

    ASSERT_OK(builder.apply_add_index(op));

    // Content: the added column is present...
    ASSERT_EQ(2, schema->column_size());
    EXPECT_EQ(105, schema->column(1).unique_id());
    // ...and the per-column index flag actually landed on it. Without installing
    // the content first, bump_flag() would have had no column 105 to find, and
    // compaction output would carry no bloom filter for c5.
    EXPECT_TRUE(schema->column(1).is_bf_column());
    ASSERT_EQ(1, schema->table_indices_size());
    // Id/version: this alter's, not FE's catalog id.
    EXPECT_EQ(777, schema->id());
    EXPECT_EQ(4, schema->schema_version());
}

TEST_F(MetaFileTest, test_apply_add_index_rejects_schema_version_regression) {
    // op.new_schema() is the snapshot FE took at dispatch; publish happens later.
    // If tablet metadata advanced in between, installing the snapshot would DROP
    // the columns that arrived meanwhile, so the apply must fail instead.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20111);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20111);
    metadata->set_version(9);
    auto* schema = metadata->mutable_schema();
    schema->set_id(500);
    schema->set_schema_version(10);
    push_column(schema, /*col_uid=*/42, "c1");
    push_column(schema, /*col_uid=*/43, "c2_added_later");

    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* fe_schema = op.mutable_new_schema();
    fe_schema->set_id(999);
    fe_schema->set_schema_version(4); // older than the metadata's 10
    push_column(fe_schema, /*col_uid=*/42, "c1");
    op.set_new_schema_id(777);
    op.set_new_schema_version(4);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_type(BLOOM_FILTER);
    new_ix->add_col_unique_id(42);

    EXPECT_FALSE(builder.apply_add_index(op).ok());
    // The later column survived and the id was not stamped.
    EXPECT_EQ(2, schema->column_size());
    EXPECT_EQ(500, schema->id());
    EXPECT_EQ(10, schema->schema_version());
}

TEST_F(MetaFileTest, test_apply_add_index_new_schema_replay_is_idempotent) {
    // Publish can replay (retry, or metadata replay after a restart). By then the
    // metadata already carries the TARGET schema version, which is newer than the
    // FE catalog snapshot in the log -- so the no-regression check must be made
    // against the target version, not the snapshot's, or every replay would fail.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20113);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20113);
    metadata->set_version(4);
    auto* schema = metadata->mutable_schema();
    schema->set_id(500);
    schema->set_schema_version(3);
    push_column(schema, /*col_uid=*/42, "c1");

    TxnLogPB_OpAddIndex op;
    auto* fe_schema = op.mutable_new_schema();
    fe_schema->set_id(999);
    fe_schema->set_schema_version(3); // catalog snapshot; target below is 4
    push_column(fe_schema, /*col_uid=*/42, "c1");
    auto* c5 = push_column(fe_schema, /*col_uid=*/105, "c5");
    c5->set_is_nullable(true);
    c5->set_default_value("0");
    op.set_new_schema_id(777);
    op.set_new_schema_version(4);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_type(BLOOM_FILTER);
    new_ix->add_col_unique_id(105);

    {
        MetaFileBuilder builder(*tablet, metadata);
        ASSERT_OK(builder.apply_add_index(op));
    }
    ASSERT_EQ(777, schema->id());
    ASSERT_EQ(4, schema->schema_version());

    // Replay the very same op against the already-applied metadata.
    {
        MetaFileBuilder builder(*tablet, metadata);
        ASSERT_OK(builder.apply_add_index(op));
    }
    // Same end state: no duplicated column, no duplicated index, id intact.
    EXPECT_EQ(2, schema->column_size());
    EXPECT_TRUE(schema->column(1).is_bf_column());
    EXPECT_EQ(1, schema->table_indices_size());
    EXPECT_EQ(777, schema->id());
    EXPECT_EQ(4, schema->schema_version());
}

// Installing the authoritative schema must not clobber the BE-only parts of the
// tablet schema that FE's converted schema cannot express.
//
// The dangerous one is dropped_table_indices: a metadata-only DROP INDEX records
// a tombstone there so readers do not reinterpret the footer payload the drop
// left behind. convert_t_schema_to_pb_schema() never emits that field, so copying
// FE's schema wholesale erased it -- and then
// ColumnReader::has_original_bloom_filter_index() would accept a footer holding an
// NGRAM bloom as if it were a plain one, pruning away matching rows. A later
// ADD INDEX on an unrelated column was enough to trigger it.
TEST_F(MetaFileTest, test_apply_add_index_preserves_be_only_schema_fields) {
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20116);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20116);
    metadata->set_version(6);
    auto* schema = metadata->mutable_schema();
    schema->set_id(600);
    schema->set_schema_version(4);
    // BE-only bookkeeping FE's converted schema does not reproduce faithfully.
    schema->set_next_column_unique_id(9000);
    schema->set_num_rows_per_row_block(1234);
    schema->set_bf_fpp(0.02);
    // A stale sort key, matching the metadata's own (pre-ADD-COLUMN) column order.
    schema->add_sort_key_idxes(0);
    schema->set_num_short_key_columns(2);
    push_column(schema, /*col_uid=*/42, "c1");
    auto* prev = push_column(schema, /*col_uid=*/105, "v2");
    // A flag bumped by an earlier fast-path ADD INDEX whose FE catalog mutation
    // has not landed yet, so FE's schema below does NOT carry it.
    prev->set_is_bf_column(true);
    // Tombstone from a metadata-only DROP INDEX of an NGRAMBF on v2.
    auto* tomb = schema->add_dropped_table_indices();
    tomb->set_index_id(-1);
    tomb->set_index_name("ngram_v2");
    tomb->set_index_type(NGRAMBF);
    tomb->add_col_unique_id(105);

    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* fe_schema = op.mutable_new_schema();
    fe_schema->set_id(999);
    fe_schema->set_schema_version(4);
    // FE recomputes these from what it knows, and cannot express the tombstone.
    fe_schema->set_next_column_unique_id(107);
    fe_schema->set_num_rows_per_row_block(65535);
    // FE's sort key reflects the post-ADD-COLUMN column order and must win.
    fe_schema->add_sort_key_idxes(2);
    fe_schema->set_num_short_key_columns(1);
    push_column(fe_schema, /*col_uid=*/42, "c1");
    push_column(fe_schema, /*col_uid=*/105, "v2");
    auto* fe_new = push_column(fe_schema, /*col_uid=*/106, "bfc");
    fe_new->set_is_nullable(true);
    fe_new->set_default_value("0");
    op.set_new_schema_id(888);
    op.set_new_schema_version(5);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(-1);
    new_ix->set_index_name("bf_bfc");
    new_ix->set_index_type(BLOOM_FILTER);
    new_ix->add_col_unique_id(106);

    ASSERT_OK(builder.apply_add_index(op));

    // The tombstone survived -- otherwise v2's stale footer bloom becomes live again.
    ASSERT_EQ(1, schema->dropped_table_indices_size());
    EXPECT_EQ("ngram_v2", schema->dropped_table_indices(0).index_name());
    EXPECT_EQ(NGRAMBF, schema->dropped_table_indices(0).index_type());

    // Other BE-only bookkeeping kept its value instead of FE's recomputation.
    // next_column_unique_id is a monotonic mark, so it takes the max: FE recomputed
    // a LOWER 107 from its current columns, which must not move the mark backwards.
    EXPECT_EQ(9000, schema->next_column_unique_id());
    EXPECT_EQ(1234, schema->num_rows_per_row_block());
    EXPECT_DOUBLE_EQ(0.02, schema->bf_fpp());

    // The columns came from FE (the added one is present)...
    ASSERT_EQ(3, schema->column_size());
    EXPECT_EQ(106, schema->column(2).unique_id());
    EXPECT_TRUE(schema->column(2).is_bf_column());
    // ...while a flag FE did not know about was merged, not cleared. Losing it
    // would make later writes stop building that column's index.
    EXPECT_EQ(105, schema->column(1).unique_id());
    EXPECT_TRUE(schema->column(1).is_bf_column()) << "pre-existing index flag was dropped";
    EXPECT_EQ(888, schema->id());

    // Fields that index INTO the column list must move together with it. FE's
    // schema declared a different sort key than the metadata's, and since ADD
    // COLUMN does not always append (... AFTER c / FIRST, or a new KEY column),
    // keeping the old ordinals alongside new columns would misalign the sort key.
    ASSERT_EQ(1, schema->sort_key_idxes_size());
    EXPECT_EQ(2, schema->sort_key_idxes(0));
    EXPECT_EQ(1, schema->num_short_key_columns());
}

// The other direction of the monotonic mark: right after an ADD COLUMN, FE has
// just allocated the new column's unique id, so ITS next_column_unique_id is the
// higher one and must win. Restoring the metadata value outright would hand the
// same id out twice on a later add.
TEST_F(MetaFileTest, test_apply_add_index_next_unique_id_takes_the_higher_mark) {
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20117);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20117);
    metadata->set_version(3);
    auto* schema = metadata->mutable_schema();
    schema->set_id(700);
    schema->set_schema_version(2);
    schema->set_next_column_unique_id(43);
    push_column(schema, /*col_uid=*/42, "c1");

    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* fe_schema = op.mutable_new_schema();
    fe_schema->set_id(999);
    fe_schema->set_schema_version(2);
    fe_schema->set_next_column_unique_id(44); // FE just allocated uid 43
    push_column(fe_schema, /*col_uid=*/42, "c1");
    auto* added = push_column(fe_schema, /*col_uid=*/43, "bfc");
    added->set_is_nullable(true);
    added->set_default_value("0");
    op.set_new_schema_id(701);
    op.set_new_schema_version(3);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_type(BLOOM_FILTER);
    new_ix->add_col_unique_id(43);

    ASSERT_OK(builder.apply_add_index(op));

    EXPECT_EQ(44, schema->next_column_unique_id()) << "allocation mark must not lag FE";
    ASSERT_EQ(2, schema->column_size());
    EXPECT_TRUE(schema->column(1).is_bf_column());
}

// Guard: apply_add_index() decides, field by field, whether TabletSchemaPB content
// comes from FE's converted schema or is preserved from tablet metadata. A newly
// added field silently inherits the FE side, which is wrong for anything BE-only
// (see dropped_table_indices, whose loss corrupts read results). If this trips,
// classify the new field and update the install block in apply_add_index, then
// bump the expected count here.
TEST_F(MetaFileTest, test_tablet_schema_pb_field_count_guard) {
    EXPECT_EQ(17, TabletSchemaPB::descriptor()->field_count())
            << "TabletSchemaPB gained or lost a field. Decide whether apply_add_index must preserve it "
               "from tablet metadata (BE-only bookkeeping) or take it from FE's schema (logical schema), "
               "then update the expected count.";
}

// A second fast-path ADD INDEX must not drop the first one's table_indices entry.
//
// A plain bloom filter is not an Index object in the FE catalog -- it lives in the
// table-level bloom_filter_columns property -- so the schema FE attaches to the
// request carries NO table_indices entry for it, and op.new_indexes() names only
// the columns THIS alter adds. An earlier version of this fix CopyFrom'd FE's
// whole schema, which left previously-indexed columns without their entry;
// installing only the columns leaves table_indices intact.
//
// Metadata fidelity rather than a read-path bug: the plain-BF read path keys off
// the per-column is_bf_column flag and `!has_index(uid, NGRAMBF)`, and has_index()
// is never queried for BLOOM_FILTER. Pinned anyway so the accumulated index set
// stays truthful for DROP INDEX, tooling, and any future consumer.
TEST_F(MetaFileTest, test_apply_add_index_keeps_earlier_bloom_filter_entry) {
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20115);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20115);
    metadata->set_version(6);
    auto* schema = metadata->mutable_schema();
    schema->set_id(600);
    schema->set_schema_version(4);
    push_column(schema, /*col_uid=*/42, "c1");
    auto* prev_bf_col = push_column(schema, /*col_uid=*/105, "bfc");
    prev_bf_col->set_is_nullable(true);
    prev_bf_col->set_is_bf_column(true);
    auto* dropped_col = push_column(schema, /*col_uid=*/106, "gone");
    dropped_col->set_is_bf_column(true);
    // What a previous fast-path ADD INDEX left behind: one entry for a column that
    // still exists, one for a column the target schema no longer has.
    auto* prev_ix = schema->add_table_indices();
    prev_ix->set_index_id(-1);
    prev_ix->set_index_name("bf_bfc");
    prev_ix->set_index_type(BLOOM_FILTER);
    prev_ix->add_col_unique_id(105);
    auto* dead_ix = schema->add_table_indices();
    dead_ix->set_index_id(-1);
    dead_ix->set_index_name("bf_gone");
    dead_ix->set_index_type(BLOOM_FILTER);
    dead_ix->add_col_unique_id(106);

    MetaFileBuilder builder(*tablet, metadata);

    // FE's schema: c1 + bfc + the newly added bfc2. Column 106 was dropped, and
    // no table_indices entry for any bloom filter (FE cannot express them).
    TxnLogPB_OpAddIndex op;
    auto* fe_schema = op.mutable_new_schema();
    fe_schema->set_id(999);
    fe_schema->set_schema_version(4);
    push_column(fe_schema, /*col_uid=*/42, "c1");
    auto* fe_bfc = push_column(fe_schema, /*col_uid=*/105, "bfc");
    fe_bfc->set_is_nullable(true);
    auto* fe_bfc2 = push_column(fe_schema, /*col_uid=*/107, "bfc2");
    fe_bfc2->set_is_nullable(true);
    fe_bfc2->set_default_value("0");
    op.set_new_schema_id(888);
    op.set_new_schema_version(5);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(-1);
    new_ix->set_index_name("bf_bfc2");
    new_ix->set_index_type(BLOOM_FILTER);
    new_ix->add_col_unique_id(107);

    ASSERT_OK(builder.apply_add_index(op));

    // Both the earlier BF column and the new one are represented...
    std::set<std::string> names;
    for (const auto& ix : schema->table_indices()) {
        names.insert(ix.index_name());
    }
    EXPECT_EQ(1u, names.count("bf_bfc")) << "earlier bloom filter entry was dropped";
    EXPECT_EQ(1u, names.count("bf_bfc2"));
    // The entry left behind by a DROP COLUMN stays too. Pruning dead index entries
    // is the DROP COLUMN path's job, not this alter's -- installing the schema
    // leaves table_indices alone, so it neither loses live entries nor takes on
    // cleanup it has no business doing.
    EXPECT_EQ(1u, names.count("bf_gone"));
    EXPECT_EQ(3, schema->table_indices_size());

    // Per-column flags: both indexed columns carry it, and neither is lost by the
    // install (FE's schema arrives with the flags unset for the new column).
    for (const auto& col : schema->column()) {
        if (col.unique_id() == 105 || col.unique_id() == 107) {
            EXPECT_TRUE(col.is_bf_column()) << "column " << col.unique_id();
        }
    }
    EXPECT_EQ(888, schema->id());
}

// Pins the LIMIT of coverage convergence, so no comment or doc can claim more
// than this.
//
// On a table that has already fast-evolved, a metadata-only ADD COLUMN makes the
// next write archive the current schema and pin every existing rowset to it
// (archive_current_schema_into_history in txn_log_applier.cpp). Those pins point
// at a schema that predates the added column. apply_add_index repoints only the
// pins that referenced the PRE-APPLY schema, by design -- rowsets pinned to older,
// fewer-column schemas keep their pin.
//
// Compacting such a rowset ALONE resolves the OLD schema, which has neither the
// added column nor its index flag. The output rowset is then pinned to that same
// resolved schema (txn_log_applier.cpp passes it as output_rowset_schema_id and
// meta_file.cpp records it), so compacting again resolves the same thing: a fixed
// point that does not advance on its own.
//
// It is NOT permanent, though. get_output_rowset_schema takes the highest
// schema_version among its inputs, so as soon as such a rowset is compacted
// TOGETHER with one pinned to the indexed schema -- which any write after this
// alter produces -- the output picks up the index. Block 3 asserts that recovery.
// What is unbounded is the timing: size-tiered compaction picks rowsets by level,
// so a partition that stops receiving writes can sit at the fixed point
// indefinitely. Queries stay correct throughout (the column reads as its default);
// only pruning is missing.
TEST_F(MetaFileTest, test_apply_add_index_old_pin_is_a_compaction_fixed_point) {
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20114);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20114);
    metadata->set_version(7);

    // The schema in effect before the ADD COLUMN: no c5. Archived, with rowset 1
    // pinned to it -- exactly what the next write after ADD COLUMN produces.
    const int64_t kPreAddColumnSchemaId = 400;
    auto& archived = (*metadata->mutable_historical_schemas())[kPreAddColumnSchemaId];
    archived.set_id(kPreAddColumnSchemaId);
    archived.set_schema_version(2);
    archived.set_keys_type(DUP_KEYS);
    push_column(&archived, /*col_uid=*/42, "c1");
    (*metadata->mutable_rowset_to_schema())[1] = kPreAddColumnSchemaId;
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);

    // Current schema, post-ADD-COLUMN: carries c5, still no index.
    auto* schema = metadata->mutable_schema();
    schema->set_id(500);
    schema->set_schema_version(3);
    schema->set_keys_type(DUP_KEYS);
    push_column(schema, /*col_uid=*/42, "c1");
    auto* cur_c5 = push_column(schema, /*col_uid=*/105, "c5");
    cur_c5->set_is_nullable(true);
    cur_c5->set_default_value("0");

    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* fe_schema = op.mutable_new_schema();
    fe_schema->set_id(999);
    fe_schema->set_schema_version(3);
    fe_schema->set_keys_type(DUP_KEYS);
    push_column(fe_schema, /*col_uid=*/42, "c1");
    auto* c5 = push_column(fe_schema, /*col_uid=*/105, "c5");
    c5->set_is_nullable(true);
    c5->set_default_value("0");
    op.set_new_schema_id(777);
    op.set_new_schema_version(4);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_type(BLOOM_FILTER);
    new_ix->add_col_unique_id(105);

    ASSERT_OK(builder.apply_add_index(op));

    // The pin to the pre-ADD-COLUMN schema is deliberately left alone.
    EXPECT_EQ(kPreAddColumnSchemaId, metadata->rowset_to_schema().at(1));
    // And that archived schema still has neither the column nor the index flag.
    const auto& still_archived = metadata->historical_schemas().at(kPreAddColumnSchemaId);
    EXPECT_EQ(1, still_archived.column_size());
    EXPECT_FALSE(still_archived.column(0).is_bf_column());

    // 1. Compacting rowset 1 alone resolves the old schema, so its output segment
    //    is written without the index -- and the output rowset would be pinned to
    //    this same schema, which is what makes it a fixed point.
    std::vector<uint32_t> input_rowsets{1};
    ASSIGN_OR_ABORT(auto compaction_schema, _tablet_manager->get_output_rowset_schema(input_rowsets, metadata.get()));
    EXPECT_EQ(kPreAddColumnSchemaId, compaction_schema->id());
    EXPECT_EQ(1u, compaction_schema->num_columns());

    // 2. Contrast: with no pins at all (a table that never fast-evolved, or one
    //    where no write followed the ADD COLUMN), compaction resolves the CURRENT
    //    schema and the index flag reaches the output segment.
    auto fresh = std::make_shared<TabletMetadata>(*metadata);
    fresh->mutable_rowset_to_schema()->clear();
    ASSIGN_OR_ABORT(auto fresh_schema, _tablet_manager->get_output_rowset_schema(input_rowsets, fresh.get()));
    EXPECT_EQ(777, fresh_schema->id());
    ASSERT_EQ(2u, fresh_schema->num_columns());
    EXPECT_TRUE(fresh_schema->column(1).is_bf_column());

    // 3. Recovery: add a rowset pinned to the indexed schema -- what any write
    //    after this alter produces -- and compact the two together. The highest
    //    input schema_version wins, so the output picks up the indexed schema and
    //    the old rowset's data gets rewritten with the index. The gap is bounded by
    //    when compaction happens to batch them, not permanent.
    auto mixed = std::make_shared<TabletMetadata>(*metadata);
    (*mixed->mutable_historical_schemas())[777].CopyFrom(mixed->schema());
    (*mixed->mutable_rowset_to_schema())[2] = 777;
    auto* new_rowset = mixed->add_rowsets();
    new_rowset->set_id(2);
    std::vector<uint32_t> mixed_inputs{1, 2};
    ASSIGN_OR_ABORT(auto mixed_schema, _tablet_manager->get_output_rowset_schema(mixed_inputs, mixed.get()));
    EXPECT_EQ(777, mixed_schema->id()) << "a co-compacted indexed rowset must pull the old one up";
    ASSERT_EQ(2u, mixed_schema->num_columns());
    EXPECT_TRUE(mixed_schema->column(1).is_bf_column());
}

TEST_F(MetaFileTest, test_apply_add_index_repoints_rowset_pins_from_pre_apply_id) {
    // On a table that already fast-evolved, rowsets are PINNED to a schema id and
    // both the read path and compaction resolve through historical_schemas. The
    // pins that referenced the schema this apply replaces must move to the new
    // id -- matched against the PRE-APPLY id, not schema->id(), which installing
    // op.new_schema() overwrites with FE's catalog id.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20112);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20112);
    metadata->set_version(7);
    auto* schema = metadata->mutable_schema();
    schema->set_id(500);
    schema->set_schema_version(3);
    push_column(schema, /*col_uid=*/42, "c1");
    (*metadata->mutable_rowset_to_schema())[1] = 500;
    (*metadata->mutable_historical_schemas())[500].CopyFrom(*schema);

    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* fe_schema = op.mutable_new_schema();
    fe_schema->set_id(999); // FE catalog id, deliberately != 500
    fe_schema->set_schema_version(4);
    push_column(fe_schema, /*col_uid=*/42, "c1");
    auto* c5 = push_column(fe_schema, /*col_uid=*/105, "c5");
    c5->set_is_nullable(true);
    c5->set_default_value("0");
    op.set_new_schema_id(777);
    op.set_new_schema_version(4);
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_type(BLOOM_FILTER);
    new_ix->add_col_unique_id(105);

    ASSERT_OK(builder.apply_add_index(op));

    ASSERT_EQ(1u, metadata->rowset_to_schema().count(1));
    EXPECT_EQ(777, metadata->rowset_to_schema().at(1));
    ASSERT_EQ(1u, metadata->historical_schemas().count(777));
    EXPECT_EQ(2, metadata->historical_schemas().at(777).column_size());
}

TEST_F(MetaFileTest, test_apply_add_index_unknown_col_unique_id_is_noop_on_columns) {
    // If the index references a unique_id that isn't on any column (e.g. the
    // column was dropped between alter request and publish), bump_flag must
    // silently skip rather than touch the wrong column.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20103);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20103);
    auto* schema = metadata->mutable_schema();
    push_column(schema, /*col_uid=*/42, "c1");
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(9001);
    new_ix->set_index_type(BITMAP);
    new_ix->add_col_unique_id(/*ghost=*/999);
    ASSERT_OK(builder.apply_add_index(op));

    EXPECT_FALSE(schema->column(0).has_bitmap_index());
    EXPECT_FALSE(schema->column(0).is_bf_column());
    // table_indices still gets the entry (it's keyed by index_id and is
    // logically the source of truth even if the column was dropped).
    EXPECT_EQ(1, schema->table_indices_size());
}

TEST_F(MetaFileTest, test_apply_add_index_self_heals_existing_index_id) {
    // Replay path: same index_id already in table_indices. The add must NOT
    // duplicate the entry, but should still flip per-column flags so older
    // metadata written before the R3 fix gets repaired on the next publish.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20104);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20104);
    auto* schema = metadata->mutable_schema();
    push_column(schema, /*col_uid=*/12, "c");
    auto* ix = schema->add_table_indices();
    ix->set_index_id(123);
    ix->set_index_type(BITMAP);
    ix->add_col_unique_id(12);
    // Simulate older BE that didn't bump the flag.
    EXPECT_FALSE(schema->column(0).has_bitmap_index());

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpAddIndex op;
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(123);
    new_ix->set_index_type(BITMAP);
    new_ix->add_col_unique_id(12);
    ASSERT_OK(builder.apply_add_index(op));

    EXPECT_EQ(1, schema->table_indices_size());        // not duplicated
    EXPECT_TRUE(schema->column(0).has_bitmap_index()); // self-healed
}

TEST_F(MetaFileTest, test_apply_add_index_no_segment_entries_only_index_pb) {
    // op carries new_indexes but no segment_entries (e.g. an alter that
    // touches schema only). idg_meta stays empty; schema gets the index.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20105);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20105);
    auto* schema = metadata->mutable_schema();
    push_column(schema, /*col_uid=*/1, "c1");
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(42);
    new_ix->set_index_type(BITMAP);
    new_ix->add_col_unique_id(1);
    ASSERT_OK(builder.apply_add_index(op));

    // mutable_idg_meta() lazy-allocates so has_idg_meta() is true; the
    // contract-relevant check is that the idgs map stayed empty.
    EXPECT_TRUE(metadata->idg_meta().idgs().empty());
    EXPECT_EQ(1, schema->table_indices_size());
    EXPECT_TRUE(schema->column(0).has_bitmap_index());
}

TEST_F(MetaFileTest, test_apply_add_index_index_without_type_skips_flag_bump) {
    // index_type unset → bump_flag never called.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20106);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20106);
    auto* schema = metadata->mutable_schema();
    push_column(schema, /*col_uid=*/3, "c");
    MetaFileBuilder builder(*tablet, metadata);

    TxnLogPB_OpAddIndex op;
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(99);
    // intentionally omit set_index_type
    new_ix->add_col_unique_id(3);
    ASSERT_OK(builder.apply_add_index(op));

    EXPECT_FALSE(schema->column(0).has_bitmap_index());
    EXPECT_FALSE(schema->column(0).is_bf_column());
}

// CompactionUpdateConflictChecker must detect a racing ADD INDEX (IDG entry
// version > op_compaction.compact_version) and force the compaction to land
// as a with_conflict no-op, mirroring the DCG conflict path. This covers
// column_mode_partial_update_handler.cpp lines 493-502.
TEST_F(MetaFileTest, test_compaction_conflict_checker_with_idg_race) {
    const int64_t tablet_id = 32011;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(20);
    metadata->set_next_rowset_id(300);

    // Input rowset id 210 covers two segments at rssid 210 and 211.
    auto* input_rowset = metadata->add_rowsets();
    input_rowset->set_id(210);
    input_rowset->add_segment_metas()->set_filename("a.dat");
    input_rowset->add_segment_metas()->set_filename("b.dat");

    // Racing ADD INDEX landed at version 19 on segment 211, *after* the
    // compaction's compact_version (=18). The checker must report a conflict.
    auto& idg_ver = (*metadata->mutable_idg_meta()->mutable_idgs())[211];
    auto* entry = idg_ver.add_entries();
    entry->set_index_file("race.idx");
    entry->set_version(19);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(210);
    op_compaction.set_compact_version(18);
    op_compaction.mutable_output_rowset()->add_segment_metas()->set_filename("out.dat");

    EXPECT_TRUE(CompactionUpdateConflictChecker::conflict_check(op_compaction, 999, *metadata, &builder));
}

TEST_F(MetaFileTest, test_apply_drop_index_unknown_id_noop) {
    // Drop op references an index_id not present in schema → table_indices
    // unchanged, dropped_table_indices stays empty, no crash.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20107);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20107);
    auto* schema = metadata->mutable_schema();
    auto* ix = schema->add_table_indices();
    ix->set_index_id(11);
    ix->set_index_type(BITMAP);
    ix->add_col_unique_id(0);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpDropIndex op;
    push_dropped(&op, /*index_id=*/9999, /*col_uid=*/0, BITMAP);
    builder.apply_drop_index(op);

    EXPECT_EQ(1, schema->table_indices_size());
    EXPECT_EQ(0, schema->dropped_table_indices_size());
}

TEST_F(MetaFileTest, test_apply_add_index_second_bitmap_with_sentinel_id_lands) {
    // BITMAP / NGRAMBF / BLOOM_FILTER all share index_id=-1 in FE-sent
    // protos. Dedup by id alone would skip every additional same-class
    // index after the first. Verify a second BITMAP on a different column
    // (also id=-1) actually gets added to schema.table_indices.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20200);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20200);
    auto* schema = metadata->mutable_schema();
    auto* existing = schema->add_table_indices();
    existing->set_index_id(-1);
    existing->set_index_name("idx_a");
    existing->set_index_type(BITMAP);
    existing->add_col_unique_id(101);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpAddIndex op;
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(-1);
    new_ix->set_index_name("idx_b");
    new_ix->set_index_type(BITMAP);
    new_ix->add_col_unique_id(102);
    ASSERT_OK(builder.apply_add_index(op));

    ASSERT_EQ(2, schema->table_indices_size());
    EXPECT_EQ("idx_a", schema->table_indices(0).index_name());
    EXPECT_EQ("idx_b", schema->table_indices(1).index_name());
}

TEST_F(MetaFileTest, test_apply_add_index_same_name_with_sentinel_id_dedups) {
    // The defensive idempotent reconciliation must still skip a re-apply of
    // the *same* index (FE may have pushed the schema, then publish replays
    // the OpAddIndex). For id=-1 entries the dedup key is index_name.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20201);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20201);
    auto* schema = metadata->mutable_schema();
    auto* existing = schema->add_table_indices();
    existing->set_index_id(-1);
    existing->set_index_name("idx_a");
    existing->set_index_type(BITMAP);
    existing->add_col_unique_id(101);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpAddIndex op;
    auto* new_ix = op.add_new_indexes();
    new_ix->set_index_id(-1);
    new_ix->set_index_name("idx_a"); // same name
    new_ix->set_index_type(BITMAP);
    new_ix->add_col_unique_id(101);
    ASSERT_OK(builder.apply_add_index(op));

    EXPECT_EQ(1, schema->table_indices_size());
}

TEST_F(MetaFileTest, test_apply_drop_index_sentinel_id_drops_only_target) {
    // Schema has two BITMAP indexes both at index_id=-1 (idx_a on col 201,
    // idx_b on col 202). Dropping idx_a must remove only idx_a; idx_b
    // must remain. Pre-fix, an id-only match removed every TabletIndexPB
    // whose id was -1, wiping idx_b too.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20202);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20202);
    auto* schema = metadata->mutable_schema();
    auto* a = schema->add_table_indices();
    a->set_index_id(-1);
    a->set_index_name("idx_a");
    a->set_index_type(BITMAP);
    a->add_col_unique_id(201);
    auto* b = schema->add_table_indices();
    b->set_index_id(-1);
    b->set_index_name("idx_b");
    b->set_index_type(BITMAP);
    b->add_col_unique_id(202);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpDropIndex op;
    push_dropped(&op, /*index_id=*/-1, /*col_uid=*/201, BITMAP);
    builder.apply_drop_index(op);

    ASSERT_EQ(1, schema->table_indices_size());
    EXPECT_EQ("idx_b", schema->table_indices(0).index_name());
    ASSERT_EQ(1, schema->dropped_table_indices_size());
    EXPECT_EQ("idx_a", schema->dropped_table_indices(0).index_name());
}

TEST_F(MetaFileTest, test_apply_drop_index_sentinel_id_respects_type) {
    // (col_uid=300, BITMAP) and (col_uid=300, NGRAMBF) live as two
    // TabletIndexPB with id=-1. Dropping BITMAP must not touch NGRAMBF
    // and vice versa.
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), 20203);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(20203);
    auto* schema = metadata->mutable_schema();
    auto* a = schema->add_table_indices();
    a->set_index_id(-1);
    a->set_index_name("bm_300");
    a->set_index_type(BITMAP);
    a->add_col_unique_id(300);
    auto* b = schema->add_table_indices();
    b->set_index_id(-1);
    b->set_index_name("bf_300");
    b->set_index_type(NGRAMBF);
    b->add_col_unique_id(300);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpDropIndex op;
    push_dropped(&op, /*index_id=*/-1, /*col_uid=*/300, NGRAMBF);
    builder.apply_drop_index(op);

    ASSERT_EQ(1, schema->table_indices_size());
    EXPECT_EQ(BITMAP, schema->table_indices(0).index_type());
    ASSERT_EQ(1, schema->dropped_table_indices_size());
    EXPECT_EQ(NGRAMBF, schema->dropped_table_indices(0).index_type());
}

} // namespace starrocks::lake
