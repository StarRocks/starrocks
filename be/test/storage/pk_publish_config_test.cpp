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

#include "storage/pk_publish_config.h"

#include <gtest/gtest.h>

#include <string_view>
#include <utility>
#include <vector>

#include "common/config_primary_key_fwd.h"
#include "gen_cpp/lake_service.pb.h"

namespace starrocks {

namespace {

PublishPropertyPB make_pb(int64_t revision, const std::map<std::string, std::string>& properties) {
    PublishPropertyPB pb;
    pb.set_revision(revision);
    for (const auto& [name, value] : properties) {
        (*pb.mutable_properties())[name] = value;
    }
    return pb;
}

constexpr int64_t kTabletId = 10001;

// Puts a config back the way the test found it. These accessors read this node's config whenever the
// table set nothing, so a value left behind here does not stay inside this test: every later test in
// the same binary reads it as the node's setting.
template <typename T>
class ConfigGuard {
public:
    ConfigGuard(T* field, T value) : _field(field), _original(*field) { *field = value; }
    ~ConfigGuard() { *_field = _original; }

    ConfigGuard(const ConfigGuard&) = delete;
    ConfigGuard& operator=(const ConfigGuard&) = delete;

    void set(T value) { *_field = value; }

private:
    T* _field;
    T _original;
};

} // namespace

// A table that set nothing answers with this node's config, and keeps answering with it after the
// config changes -- the point of storing only what the table set.
TEST(PkPublishConfigTest, unsetFollowsNodeConfig) {
    auto config = std::make_shared<const PkPublishConfig>();
    EXPECT_EQ(0, config->revision());

    ConfigGuard<int32_t> max_count(&config::pk_index_memtable_max_count, 3);
    ConfigGuard<int64_t> max_bytes(&config::l0_max_mem_usage, 111);
    EXPECT_EQ(3, config->memtable_max_count());
    EXPECT_EQ(111, config->memtable_max_bytes());

    max_count.set(5);
    max_bytes.set(222);
    EXPECT_EQ(5, config->memtable_max_count());
    EXPECT_EQ(222, config->memtable_max_bytes());
}

// A value the table set wins over the config, and only the properties it named are affected.
TEST(PkPublishConfigTest, setValueWinsOverNodeConfig) {
    ConfigGuard<int32_t> max_count(&config::pk_index_memtable_max_count, 3);
    ConfigGuard<int64_t> max_bytes(&config::l0_max_mem_usage, 111);

    auto config = PkPublishConfig::update(std::make_shared<const PkPublishConfig>(), kTabletId,
                                          make_pb(1, {{"pk_index_memtable_max_count", "7"}}));
    EXPECT_EQ(1, config->revision());
    EXPECT_EQ(7, config->memtable_max_count());
    EXPECT_EQ(111, config->memtable_max_bytes());

    // Still the table's value after the node config moves: the table pinned this one.
    max_count.set(4);
    EXPECT_EQ(7, config->memtable_max_count());
}

// Revisions are compared by order, not by difference: a request that overtook a newer one on the way
// here must not roll the table back, and a retry of the revision already held must not re-parse.
TEST(PkPublishConfigTest, revisionOrderDecidesReplacement) {
    auto first = PkPublishConfig::update(std::make_shared<const PkPublishConfig>(), kTabletId,
                                         make_pb(5, {{"pk_index_memtable_max_count", "7"}}));
    ASSERT_EQ(5, first->revision());

    auto same = PkPublishConfig::update(first, kTabletId, make_pb(5, {{"pk_index_memtable_max_count", "9"}}));
    EXPECT_EQ(first.get(), same.get()) << "an equal revision must reuse the instance already held";

    auto older = PkPublishConfig::update(first, kTabletId, make_pb(4, {{"pk_index_memtable_max_count", "9"}}));
    EXPECT_EQ(first.get(), older.get()) << "an older revision must not roll the table back";

    auto newer = PkPublishConfig::update(first, kTabletId, make_pb(6, {{"pk_index_memtable_max_count", "9"}}));
    EXPECT_NE(first.get(), newer.get());
    EXPECT_EQ(6, newer->revision());
    EXPECT_EQ(9, newer->memtable_max_count());
}

// A newer revision replaces the whole set, so a property it leaves out is no longer set -- which is
// how a removal travels, given a request always carries the table's complete set.
TEST(PkPublishConfigTest, newerRevisionReplacesWholeSet) {
    ConfigGuard<int32_t> max_count(&config::pk_index_memtable_max_count, 3);

    auto first = PkPublishConfig::update(std::make_shared<const PkPublishConfig>(), kTabletId,
                                         make_pb(1, {{"pk_index_memtable_max_count", "7"}}));
    ASSERT_EQ(7, first->memtable_max_count());

    auto second = PkPublishConfig::update(first, kTabletId, make_pb(2, {}));
    EXPECT_EQ(3, second->memtable_max_count()) << "a property absent from the new set follows the config again";
}

// Neither a value that does not parse nor one outside the range FE enforces may fail a publish: the
// property is dropped and the read site falls back. Only an FE without the same check can send these.
TEST(PkPublishConfigTest, unusableValueFallsBack) {
    ConfigGuard<int32_t> max_count(&config::pk_index_memtable_max_count, 3);
    ConfigGuard<int32_t> read_parallelism(&config::lake_rows_mapper_read_parallelism, 32);

    auto garbled = PkPublishConfig::update(
            std::make_shared<const PkPublishConfig>(), kTabletId,
            make_pb(1, {{"pk_index_memtable_max_count", "not a number"}, {"pk_rows_mapper_read_parallelism", "12x"}}));
    EXPECT_EQ(3, garbled->memtable_max_count());
    EXPECT_EQ(32, garbled->rows_mapper_read_parallelism());

    auto out_of_range = PkPublishConfig::update(
            std::make_shared<const PkPublishConfig>(), kTabletId,
            make_pb(2, {{"pk_index_memtable_max_count", "65"}, {"pk_rows_mapper_read_parallelism", "0"}}));
    EXPECT_EQ(3, out_of_range->memtable_max_count()) << "64 is the highest this property accepts";
    EXPECT_EQ(32, out_of_range->rows_mapper_read_parallelism()) << "1 is the lowest this property accepts";
}

// Zero turns the rebuild triggers off, so it has to survive the range check rather than being read as
// a missing value -- these are the only two properties for which it means anything.
TEST(PkPublishConfigTest, zeroDisablesRebuildTriggers) {
    ConfigGuard<int32_t> files_threshold(&config::cloud_native_pk_index_rebuild_files_threshold, 50);
    ConfigGuard<int64_t> rows_threshold(&config::cloud_native_pk_index_rebuild_rows_threshold, 10000000);

    auto config = PkPublishConfig::update(
            std::make_shared<const PkPublishConfig>(), kTabletId,
            make_pb(1, {{"pk_index_rebuild_files_threshold", "0"}, {"pk_index_rebuild_rows_threshold", "0"}}));
    EXPECT_EQ(0, config->rebuild_files_threshold());
    EXPECT_EQ(0, config->rebuild_rows_threshold());
}

// The built-in stands in when be.conf holds a value callers would divide by, exactly as it did before
// a table could set this property at all.
TEST(PkPublishConfigTest, parallelExecutionMinRowsNeverReturnsZero) {
    auto config = std::make_shared<const PkPublishConfig>();

    ConfigGuard<int64_t> min_rows(&config::pk_index_parallel_execution_min_rows, 4096);
    EXPECT_EQ(4096, config->parallel_execution_min_rows());

    min_rows.set(0);
    EXPECT_EQ(16384, config->parallel_execution_min_rows());
}

// What the query interface reports: the table's own values, named as the user writes them, and
// nothing standing in for a property the table left alone.
TEST(PkPublishConfigTest, propertiesReportOnlyWhatTheTableSet) {
    ConfigGuard<int32_t> max_count(&config::pk_index_memtable_max_count, 3);
    ConfigGuard<int64_t> max_bytes(&config::l0_max_mem_usage, 111);

    auto config = PkPublishConfig::update(
            std::make_shared<const PkPublishConfig>(), kTabletId,
            make_pb(4, {{"pk_index_memtable_max_count", "8"}, {"pk_rows_mapper_read_parallelism", "64"}}));
    ASSERT_EQ(4, config->revision());

    const std::vector<std::pair<std::string_view, int64_t>> expected{{"pk_index_memtable_max_count", 8},
                                                                     {"pk_rows_mapper_read_parallelism", 64}};
    EXPECT_EQ(expected, config->properties()) << "declaration order, and the node's config nowhere in it";
}

// A value this node refused is not reported either. It is not what the tablet runs with, and saying
// otherwise would hide the fallback the read sites actually take.
TEST(PkPublishConfigTest, propertiesLeaveOutRefusedValues) {
    auto config = PkPublishConfig::update(std::make_shared<const PkPublishConfig>(), kTabletId,
                                          make_pb(1, {{"pk_index_memtable_max_count", "65"},
                                                      {"pk_rows_mapper_read_parallelism", "not a number"},
                                                      {"pk_compaction_replace_batch_rows", "4096"}}));

    const std::vector<std::pair<std::string_view, int64_t>> expected{{"pk_compaction_replace_batch_rows", 4096}};
    EXPECT_EQ(expected, config->properties());
}

// A table that set nothing reports nothing, which is a different answer from having no index at all.
TEST(PkPublishConfigTest, propertiesAreEmptyWhenNothingWasSet) {
    auto config = std::make_shared<const PkPublishConfig>();
    EXPECT_EQ(0, config->revision());
    EXPECT_TRUE(config->properties().empty());
}

} // namespace starrocks
