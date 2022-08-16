// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/version_graph.h"

#include <gtest/gtest.h>

namespace starrocks::vectorized {

TEST(VersionGraphTest, capture) {
    VersionGraph graph;

    std::vector<RowsetMetaSharedPtr> rs_meta;
    for (int i = 0; i < 10; i++) {
        rs_meta.emplace_back(std::make_shared<RowsetMeta>());
        rs_meta.back()->set_start_version(i);
        rs_meta.back()->set_end_version(i);
    }

    for (int i = 0; i < 10; i = i + 2) {
        rs_meta.emplace_back(std::make_shared<RowsetMeta>());
        rs_meta.back()->set_start_version(i);
        rs_meta.back()->set_end_version(i + 1);
    }

    rs_meta.emplace_back(std::make_shared<RowsetMeta>());
    rs_meta.back()->set_start_version(0);
    rs_meta.back()->set_end_version(5);

    rs_meta.emplace_back(std::make_shared<RowsetMeta>());
    rs_meta.back()->set_start_version(11);
    rs_meta.back()->set_end_version(11);

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
    graph.reconstruct_version_graph(rs_meta, &max_version);

    EXPECT_EQ(max_version, -1);
}

TEST(VersionGraphTest, multi_edge) {
    VersionGraph graph;

    std::vector<RowsetMetaSharedPtr> rs_meta;
    rs_meta.emplace_back(std::make_shared<RowsetMeta>());
    rs_meta.back()->set_start_version(0);
    rs_meta.back()->set_end_version(5);

    for (int i = 0; i < 10; i = i + 2) {
        rs_meta.emplace_back(std::make_shared<RowsetMeta>());
        rs_meta.back()->set_start_version(i);
        rs_meta.back()->set_end_version(i + 1);
    }

    for (int i = 0; i < 10; i++) {
        rs_meta.emplace_back(std::make_shared<RowsetMeta>());
        rs_meta.back()->set_start_version(i);
        rs_meta.back()->set_end_version(i);
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

TEST(VersionGraphTest, remove_version) {
    VersionGraph graph;

    std::vector<RowsetMetaSharedPtr> rs_meta;
    rs_meta.emplace_back(std::make_shared<RowsetMeta>());
    rs_meta.back()->set_start_version(0);
    rs_meta.back()->set_end_version(5);

    for (int i = 0; i < 10; i = i + 2) {
        rs_meta.emplace_back(std::make_shared<RowsetMeta>());
        rs_meta.back()->set_start_version(i);
        rs_meta.back()->set_end_version(i + 1);
    }

    for (int i = 0; i < 10; i++) {
        rs_meta.emplace_back(std::make_shared<RowsetMeta>());
        rs_meta.back()->set_start_version(i);
        rs_meta.back()->set_end_version(i);
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

TEST(VersionGraphTest, max_continuous_version) {
    VersionGraph graph;
    auto gen_meta = [&](const std::vector<std::pair<int64_t, int64_t>>& versions) {
        std::vector<RowsetMetaSharedPtr> rs_meta;
        for (auto& version : versions) {
            rs_meta.emplace_back(std::make_shared<RowsetMeta>());
            rs_meta.back()->set_start_version(version.first);
            rs_meta.back()->set_end_version(version.second);
        }
        return rs_meta;
    };
    auto input1 = gen_meta({{0, 1}, {2, 2}, {3, 3}, {4, 4}, {6, 6}});
    graph.construct_version_graph(input1, NULL);
    EXPECT_EQ(graph.max_continuous_version(), 4);
    graph.add_version_to_graph(Version(5, 5));
    EXPECT_EQ(graph.max_continuous_version(), 6);
    graph.add_version_to_graph(Version(7, 7));
    EXPECT_EQ(graph.max_continuous_version(), 7);
}

} // namespace starrocks::vectorized