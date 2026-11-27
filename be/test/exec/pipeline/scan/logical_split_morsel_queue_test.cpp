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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "exec/pipeline/scan/morsel.h"
#include "gen_cpp/InternalService_types.h"
#include "storage/base_tablet.h"
#include "storage/olap_common.h"
#include "storage/rowset/base_rowset.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

namespace {

TScanRange make_scan_range(int64_t tablet_id, int64_t version = 1) {
    TScanRange scan_range;
    TInternalScanRange internal;
    internal.tablet_id = tablet_id;
    internal.version = std::to_string(version);
    internal.partition_id = 1;
    scan_range.__set_internal_scan_range(internal);
    return scan_range;
}

// (c0 int key, v int value) with one short key column.
TabletSchemaCSPtr make_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    auto* c0 = schema_pb.add_column();
    c0->set_unique_id(0);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);
    c0->set_length(4);
    c0->set_index_length(4);
    auto* v = schema_pb.add_column();
    v->set_unique_id(1);
    v->set_name("v");
    v->set_type("INT");
    v->set_is_key(false);
    v->set_is_nullable(false);
    v->set_length(4);
    v->set_index_length(4);
    v->set_aggregation("NONE");
    return TabletSchema::create(schema_pb);
}

// Minimal non-cloud-native tablet with a fixed row count.
class FakeTablet : public BaseTablet {
public:
    FakeTablet(int64_t id, int64_t num_rows) : _id(id), _num_rows(num_rows) {}
    int64_t tablet_id() const override { return _id; }
    size_t num_rows() const override { return _num_rows; }
    StatusOr<bool> has_delete_predicates(const Version& /*version*/) override { return false; }
    bool belonged_to_cloud_native() const override { return false; }

private:
    int64_t _id;
    size_t _num_rows;
};

// A rowset whose segments fail to load, the way a lake Rowset behaves when a segment open fails
// (e.g. a StarMgr GetShard timeout): get_segments() swallows the error and returns an empty list,
// get_segments_checked() returns it. num_rows() comes from the rowset metadata, so it stays non-zero.
class LoadFailingRowset : public BaseRowset {
public:
    LoadFailingRowset(bool overlapped, int64_t num_rows, Status load_error)
            : _overlapped(overlapped), _num_rows(num_rows), _load_error(std::move(load_error)) {
        _rowset_id.init(1);
    }
    RowsetId rowset_id() const override { return _rowset_id; }
    int64_t num_rows() const override { return _num_rows; }
    bool is_overlapped() const override { return _overlapped; }
    std::vector<SegmentSharedPtr> get_segments() override { return {}; }
    StatusOr<std::vector<SegmentSharedPtr>> get_segments_checked() override { return _load_error; }
    bool has_data_files() const override { return true; }
    int64_t start_version() const override { return 1; }
    int64_t end_version() const override { return 1; }

private:
    bool _overlapped;
    int64_t _num_rows;
    Status _load_error;
    RowsetId _rowset_id;
};

} // namespace

class LogicalSplitMorselQueueTest : public ::testing::Test {};

// RB-2037: a segment-load failure of the largest rowset must fail the query with the load error. An
// overlapped rowset used to turn the empty get_segments() result into a null largest segment and
// crash the CN in Segment::load_index(); a non-overlapped one was split as if it had no segment.
TEST_F(LogicalSplitMorselQueueTest, segment_load_failure_fails_query) {
    auto schema = make_schema();
    const std::string kLoadError = "starlet err grpc.GetShard(shardId=888596) error: Deadline Exceeded";
    for (bool overlapped : {true, false}) {
        SCOPED_TRACE(overlapped ? "overlapped" : "non-overlapped");
        auto rowset = std::make_shared<LoadFailingRowset>(overlapped, 100, Status::InternalError(kLoadError));
        std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(10003, 100)};
        std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};

        Morsels morsels;
        morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(10003)));
        LogicalSplitMorselQueue queue(std::move(morsels), 1, 25);
        queue.set_tablets(tablets);
        queue.set_tablet_rowsets(tablet_rowsets);
        queue.set_tablet_schema(schema);

        auto morsel_or = queue.try_get();
        ASSERT_FALSE(morsel_or.ok());
        EXPECT_NE(morsel_or.status().to_string().find(kLoadError), std::string::npos) << morsel_or.status();
    }
}

} // namespace starrocks::pipeline
