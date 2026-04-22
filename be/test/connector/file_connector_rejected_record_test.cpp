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

// Tests for the rejected-record format-whitelist check in
// FileDataSource::_create_scanner() (file_connector.cpp lines 88-93).
// When enable_log_rejected_record() is true, formats other than CSV/JSON/
// PARQUET/ORC must return InternalError.

#include <gtest/gtest.h>

#include "common/runtime_profile.h"
#include "connector/file_connector.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

namespace {

// Build a minimal RuntimeState with:
//   * query_type = LOAD
//   * log_rejected_record_num = -1 (unlimited, enables rejected-record logging)
//   * one INT slot tuple so that desc_tbl lookup succeeds for tuple_id=0.
std::shared_ptr<RuntimeState> make_load_state_with_desc_tbl(ObjectPool* pool) {
    TQueryOptions opts;
    opts.query_type = TQueryType::LOAD;
    opts.log_rejected_record_num = -1;
    TQueryGlobals globals;
    TUniqueId id;

    auto state = std::make_shared<RuntimeState>(id, opts, globals, /*exec_env=*/nullptr);

    // Build a DescriptorTbl with a single INT column so tuple_id=0 is valid.
    TDescriptorTableBuilder desc_builder;
    TTupleDescriptorBuilder tuple_builder;
    TSlotDescriptorBuilder slot_builder;
    TypeDescriptor int_type(TYPE_INT);
    slot_builder.type(int_type).length(int_type.len).precision(0).scale(0).nullable(true);
    tuple_builder.add_slot(slot_builder.build());
    tuple_builder.build(&desc_builder);

    DescriptorTbl* tbl = nullptr;
    Status st = DescriptorTbl::create(state.get(), pool, desc_builder.desc_tbl(), &tbl, config::vector_chunk_size);
    CHECK(st.ok()) << st.message();
    state->set_desc_tbl(tbl);
    return state;
}

// Build a minimal TScanRange whose broker_scan_range has one range entry with
// the given format type and file_size > 0 (so it isn't filtered out by the
// FileDataSource constructor).
TScanRange make_scan_range(TFileFormatType::type format) {
    TBrokerRangeDesc range_desc;
    range_desc.__set_format_type(format);
    range_desc.__set_file_type(TFileType::FILE_LOCAL);
    range_desc.__set_splittable(false);
    range_desc.__set_path("/dev/null");
    range_desc.__set_start_offset(0);
    range_desc.__set_size(1);
    range_desc.__set_file_size(1); // non-zero so it isn't dropped by constructor
    range_desc.__set_num_of_columns_from_file(1);

    TBrokerScanRangeParams params;
    params.strict_mode = false;
    params.dest_tuple_id = 0;
    params.src_tuple_id = 0;

    TBrokerScanRange broker_scan_range;
    broker_scan_range.params = params;
    broker_scan_range.ranges.push_back(range_desc);

    TScanRange scan_range;
    scan_range.__set_broker_scan_range(broker_scan_range);
    return scan_range;
}

// Build the provider and data source for a given format type.
// provider is stored via the unique_ptr and must outlive data_source.
std::unique_ptr<FileDataSource> make_data_source(const FileDataSourceProvider* provider, TFileFormatType::type format,
                                                 RuntimeProfile* root_profile) {
    TScanRange scan_range = make_scan_range(format);
    auto ds = std::make_unique<FileDataSource>(provider, scan_range);
    ds->set_runtime_profile(root_profile);
    return ds;
}

} // namespace

class FileConnectorRejectedRecordTest : public ::testing::Test {
public:
    void SetUp() override {
        _pool = std::make_unique<ObjectPool>();
        _profile = std::make_unique<RuntimeProfile>("FileConnectorRejectedRecordTest");

        // Build a plan node with file_scan_node.tuple_id = 0.
        TFileScanNode file_scan_node;
        file_scan_node.tuple_id = 0;
        _plan_node.__set_file_scan_node(file_scan_node);

        _provider = std::make_unique<FileDataSourceProvider>(/*scan_node=*/nullptr, _plan_node);
    }

protected:
    std::unique_ptr<ObjectPool> _pool;
    std::unique_ptr<RuntimeProfile> _profile;
    TPlanNode _plan_node;
    std::unique_ptr<FileDataSourceProvider> _provider;
};

// ============================================================================
// Covering file_connector.cpp lines 88-93:
//   if (enable_log_rejected_record() && format not in {CSV,JSON,PARQUET,ORC})
//       return InternalError("only support csv/json/parquet/orc format ...")
// ============================================================================

TEST_F(FileConnectorRejectedRecordTest, UnsupportedFormatWithRejectedRecordLoggingReturnsError) {
    // FORMAT_AVRO is not in the accepted whitelist. With rejected-record logging
    // enabled this should surface as InternalError.
    auto state = make_load_state_with_desc_tbl(_pool.get());
    auto ds = make_data_source(_provider.get(), TFileFormatType::FORMAT_AVRO, _profile.get());

    Status st = ds->open(state.get());
    EXPECT_FALSE(st.ok()) << "Expected failure for unsupported format with rejected-record logging";
    EXPECT_NE(std::string::npos, st.message().find("only support csv/json/parquet/orc format"))
            << "Unexpected error message: " << st.message();
}

TEST_F(FileConnectorRejectedRecordTest, UnsupportedFormatWithoutRejectedRecordLoggingFallsThrough) {
    // Without rejected-record logging enabled, an AVRO scan range should
    // proceed past the whitelist check (it then tries to open a real file,
    // so it fails later -- but NOT with the whitelist message).
    TQueryOptions opts;
    opts.query_type = TQueryType::LOAD;
    opts.log_rejected_record_num = 0; // 0 means no rejected records allowed (logging disabled)
    TQueryGlobals globals;
    TUniqueId id;
    auto state = std::make_shared<RuntimeState>(id, opts, globals, /*exec_env=*/nullptr);

    // Build DescriptorTbl with tuple_id=0.
    TDescriptorTableBuilder desc_builder;
    TTupleDescriptorBuilder tuple_builder;
    TSlotDescriptorBuilder slot_builder;
    TypeDescriptor int_type(TYPE_INT);
    slot_builder.type(int_type).length(int_type.len).precision(0).scale(0).nullable(true);
    tuple_builder.add_slot(slot_builder.build());
    tuple_builder.build(&desc_builder);
    DescriptorTbl* tbl = nullptr;
    Status dts =
            DescriptorTbl::create(state.get(), _pool.get(), desc_builder.desc_tbl(), &tbl, config::vector_chunk_size);
    CHECK(dts.ok()) << dts.message();
    state->set_desc_tbl(tbl);

    auto ds = make_data_source(_provider.get(), TFileFormatType::FORMAT_AVRO, _profile.get());
    Status st = ds->open(state.get());

    // The error (if any) must NOT be the whitelist message.
    if (!st.ok()) {
        EXPECT_EQ(std::string::npos, st.message().find("only support csv/json/parquet/orc format"))
                << "Should not hit whitelist check when logging is disabled. Got: " << st.message();
    }
}

TEST_F(FileConnectorRejectedRecordTest, SupportedFormatsPassWhitelistCheck) {
    // CSV_PLAIN, JSON, PARQUET, ORC must all pass the whitelist check.
    // They will fail later (no real file at /dev/null for non-text formats),
    // but they must NOT return the "only support csv/json/parquet/orc" error.
    const std::vector<TFileFormatType::type> ok_formats = {
            TFileFormatType::FORMAT_CSV_PLAIN, TFileFormatType::FORMAT_JSON,
            // PARQUET and ORC require native libs that may not be available in the UT
            // environment; skip them to avoid unrelated failures.
    };

    for (auto fmt : ok_formats) {
        auto state = make_load_state_with_desc_tbl(_pool.get());
        // Reset the DescriptorTbl on a fresh pool each iteration since
        // make_load_state_with_desc_tbl uses the same pool.
        auto local_pool = std::make_unique<ObjectPool>();
        auto local_state = make_load_state_with_desc_tbl(local_pool.get());

        auto local_profile = std::make_unique<RuntimeProfile>("fmt_test");
        auto ds = make_data_source(_provider.get(), fmt, local_profile.get());
        Status st = ds->open(local_state.get());

        // Whatever happens, it must NOT be the whitelist InternalError.
        if (!st.ok()) {
            EXPECT_EQ(std::string::npos, st.message().find("only support csv/json/parquet/orc format"))
                    << "Format " << fmt << " should not hit whitelist check. Got: " << st.message();
        }
    }
}

} // namespace starrocks::connector
