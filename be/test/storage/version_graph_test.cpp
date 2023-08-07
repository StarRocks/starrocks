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

#include "storage/version_graph.h"

#include <gtest/gtest.h>

namespace starrocks {

// NOLINTNEXTLINE
TEST(VersionGraphTest, capture) {
    VersionGraph graph;
    RowsetId id;
    id.init(2, 3, 0, 0);

    std::vector<RowsetMetaSharedPtr> rs_meta;
    for (int i = 0; i < 10; i++) {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        rs_meta_pb->set_rowset_id(id.to_string());
        rs_meta_pb->set_start_version(i);
        rs_meta_pb->set_end_version(i);
        rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
    }

    for (int i = 0; i < 10; i = i + 2) {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        rs_meta_pb->set_rowset_id(id.to_string());
        rs_meta_pb->set_start_version(i);
        rs_meta_pb->set_end_version(i + 1);
        rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
    }

    {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        rs_meta_pb->set_rowset_id(id.to_string());
        rs_meta_pb->set_start_version(0);
        rs_meta_pb->set_end_version(5);
        rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
    }

    {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        rs_meta_pb->set_rowset_id(id.to_string());
        rs_meta_pb->set_start_version(11);
        rs_meta_pb->set_end_version(11);
        rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
    }

    int64_t max_version = -1;
    graph.construct_version_graph(rs_meta, &max_version);

    EXPECT_EQ(max_version, 11);

    for (int i = 0; i < 10; i++) {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(i, i), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 1);
        EXPECT_EQ(version_path[0].first, i);
        EXPECT_EQ(version_path[0].second, i);
    }

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 1), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 1);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 1);
    }

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 2), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 2);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 1);
        EXPECT_EQ(version_path[1].first, 2);
        EXPECT_EQ(version_path[1].second, 2);
    }

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 3), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 2);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 1);
        EXPECT_EQ(version_path[1].first, 2);
        EXPECT_EQ(version_path[1].second, 3);
    }

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 5), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 1);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 5);
    }

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 8), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 3);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 5);
        EXPECT_EQ(version_path[1].first, 6);
        EXPECT_EQ(version_path[1].second, 7);
        EXPECT_EQ(version_path[2].first, 8);
        EXPECT_EQ(version_path[2].second, 8);
    }

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 9), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 3);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 5);
        EXPECT_EQ(version_path[1].first, 6);
        EXPECT_EQ(version_path[1].second, 7);
        EXPECT_EQ(version_path[2].first, 8);
        EXPECT_EQ(version_path[2].second, 9);
    }

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 10), &version_path).ok(), false);
    }

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 11), &version_path).ok(), false);
    }

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 12), &version_path).ok(), false);
    }

    graph.add_version_to_graph(Version(10, 10));

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 10), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 4);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 5);
        EXPECT_EQ(version_path[1].first, 6);
        EXPECT_EQ(version_path[1].second, 7);
        EXPECT_EQ(version_path[2].first, 8);
        EXPECT_EQ(version_path[2].second, 9);
        EXPECT_EQ(version_path[3].first, 10);
        EXPECT_EQ(version_path[3].second, 10);
    }

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 11), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 5);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 5);
        EXPECT_EQ(version_path[1].first, 6);
        EXPECT_EQ(version_path[1].second, 7);
        EXPECT_EQ(version_path[2].first, 8);
        EXPECT_EQ(version_path[2].second, 9);
        EXPECT_EQ(version_path[3].first, 10);
        EXPECT_EQ(version_path[3].second, 10);
        EXPECT_EQ(version_path[4].first, 11);
        EXPECT_EQ(version_path[4].second, 11);
    }

    EXPECT_EQ(graph.delete_version_from_graph(Version(1, 1)).ok(), true);

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 11), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 5);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 5);
        EXPECT_EQ(version_path[1].first, 6);
        EXPECT_EQ(version_path[1].second, 7);
        EXPECT_EQ(version_path[2].first, 8);
        EXPECT_EQ(version_path[2].second, 9);
        EXPECT_EQ(version_path[3].first, 10);
        EXPECT_EQ(version_path[3].second, 10);
        EXPECT_EQ(version_path[4].first, 11);
        EXPECT_EQ(version_path[4].second, 11);
    }

    EXPECT_EQ(graph.delete_version_from_graph(Version(8, 9)).ok(), true);

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 11), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 6);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 5);
        EXPECT_EQ(version_path[1].first, 6);
        EXPECT_EQ(version_path[1].second, 7);
        EXPECT_EQ(version_path[2].first, 8);
        EXPECT_EQ(version_path[2].second, 8);
        EXPECT_EQ(version_path[3].first, 9);
        EXPECT_EQ(version_path[3].second, 9);
        EXPECT_EQ(version_path[4].first, 10);
        EXPECT_EQ(version_path[4].second, 10);
        EXPECT_EQ(version_path[5].first, 11);
        EXPECT_EQ(version_path[5].second, 11);
    }

    EXPECT_EQ(graph.delete_version_from_graph(Version(10, 10)).ok(), true);

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 11), &version_path).ok(), false);
    }

    rs_meta.clear();
    max_version = -1;
    graph.construct_version_graph(rs_meta, &max_version);

    EXPECT_EQ(max_version, -1);
}

// NOLINTNEXTLINE
TEST(VersionGraphTest, multi_edge) {
    VersionGraph graph;
    RowsetId id;
    id.init(2, 3, 0, 0);

    std::vector<RowsetMetaSharedPtr> rs_meta;
    {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        rs_meta_pb->set_rowset_id(id.to_string());
        rs_meta_pb->set_start_version(0);
        rs_meta_pb->set_end_version(5);
        rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
    }

    for (int i = 0; i < 10; i = i + 2) {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        rs_meta_pb->set_rowset_id(id.to_string());
        rs_meta_pb->set_start_version(i);
        rs_meta_pb->set_end_version(i + 1);
        rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
    }

    for (int i = 0; i < 10; i++) {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        rs_meta_pb->set_rowset_id(id.to_string());
        rs_meta_pb->set_start_version(i);
        rs_meta_pb->set_end_version(i);
        rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
    }

    int64_t max_version = -1;
    graph.construct_version_graph(rs_meta, &max_version);

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 8), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 3);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 5);
        EXPECT_EQ(version_path[1].first, 6);
        EXPECT_EQ(version_path[1].second, 7);
        EXPECT_EQ(version_path[2].first, 8);
        EXPECT_EQ(version_path[2].second, 8);
    }
}

// NOLINTNEXTLINE
TEST(VersionGraphTest, remove_version) {
    VersionGraph graph;
    RowsetId id;
    id.init(2, 3, 0, 0);

    std::vector<RowsetMetaSharedPtr> rs_meta;
    {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        rs_meta_pb->set_rowset_id(id.to_string());
        rs_meta_pb->set_start_version(0);
        rs_meta_pb->set_end_version(5);
        rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
    }

    for (int i = 0; i < 10; i = i + 2) {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        rs_meta_pb->set_rowset_id(id.to_string());
        rs_meta_pb->set_start_version(i);
        rs_meta_pb->set_end_version(i + 1);
        rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
    }

    for (int i = 0; i < 10; i++) {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        rs_meta_pb->set_rowset_id(id.to_string());
        rs_meta_pb->set_start_version(i);
        rs_meta_pb->set_end_version(i);
        rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
    }

    int64_t max_version = -1;
    graph.construct_version_graph(rs_meta, &max_version);

    {
        std::vector<Version> version_path;
        EXPECT_EQ(graph.capture_consistent_versions(Version(0, 8), &version_path).ok(), true);
        EXPECT_EQ(version_path.size(), 3);
        EXPECT_EQ(version_path[0].first, 0);
        EXPECT_EQ(version_path[0].second, 5);
        EXPECT_EQ(version_path[1].first, 6);
        EXPECT_EQ(version_path[1].second, 7);
        EXPECT_EQ(version_path[2].first, 8);
        EXPECT_EQ(version_path[2].second, 8);
    }

    EXPECT_EQ(graph.delete_version_from_graph(Version(0, 5)).ok(), true);
    for (int i = 0; i < 10; i = i + 2) {
        EXPECT_EQ(graph.delete_version_from_graph(Version(i, i + 1)).ok(), true);
    }
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(graph.delete_version_from_graph(Version(i, i)).ok(), true);
    }

    EXPECT_EQ(graph._version_graph.size(), 0);
}

// NOLINTNEXTLINE
TEST(VersionGraphTest, max_continuous_version) {
    VersionGraph graph;
    RowsetId id;
    id.init(2, 3, 0, 0);
    auto gen_meta = [&](const std::vector<std::pair<int64_t, int64_t>>& versions) {
        std::vector<RowsetMetaSharedPtr> rs_meta;
        for (auto& version : versions) {
            auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
            rs_meta_pb->set_rowset_id(id.to_string());
            rs_meta_pb->set_start_version(version.first);
            rs_meta_pb->set_end_version(version.second);
            rs_meta.emplace_back(std::make_shared<RowsetMeta>(rs_meta_pb));
        }
        return rs_meta;
    };
    auto input1 = gen_meta({{0, 1}, {2, 2}, {3, 3}, {4, 4}, {6, 6}});
    graph.construct_version_graph(input1, nullptr);
    EXPECT_EQ(graph.max_continuous_version(), 4);
    EXPECT_EQ(graph.min_readable_version(), 1);
    graph.add_version_to_graph(Version(5, 5));
    EXPECT_EQ(graph.max_continuous_version(), 6);
    EXPECT_EQ(graph.min_readable_version(), 1);
    graph.add_version_to_graph(Version(7, 7));
    EXPECT_EQ(graph.max_continuous_version(), 7);
    EXPECT_EQ(graph.min_readable_version(), 1);

    // The following case may be happened in schema change
    // 1. Schema change first remove the existing rowset
    // 2. Add new version rowset into new tablet
    graph.delete_version_from_graph(Version(0, 1));
    EXPECT_EQ(graph.min_readable_version(), 1);
    for (int i = 2; i <= 7; i++) {
        graph.delete_version_from_graph(Version(i, i));
        EXPECT_EQ(graph.min_readable_version(), i);
    }
    EXPECT_EQ(graph.max_continuous_version(), 7);
    graph.add_version_to_graph(Version(0, 10));
    EXPECT_EQ(graph.max_continuous_version(), 10);
}

} // namespace starrocks
