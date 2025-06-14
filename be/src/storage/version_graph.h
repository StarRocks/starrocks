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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/version_graph.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_meta.h"

namespace starrocks {

// It is used to represent Graph vertex.
struct Vertex {
    int64_t value = 0;
    // one vertex to other vertex may have multi same edge
    // this is just for compatibility with previous implementations.
    std::list<Vertex*> edges;

    Vertex(int64_t v) : value(v) {}
};

/// VersionGraph class which is implemented to build and maintain total versions of rowsets.
/// This class use adjacency-matrix represent rowsets version and links. A vertex is a version
/// and a link is the _version object of a rowset (from start version to end version + 1).
/// Use this class, when given a spec_version, we can get a version path which is the shortest path
/// in the graph.
class VersionGraph {
public:
    /// Use rs_metas to construct the graph including vertex and edges, and return the
    /// max_version in metas.
    void construct_version_graph(const std::vector<RowsetMetaSharedPtr>& rs_metas, int64_t* max_version);
    /// Add a version to this graph, graph will add the version and edge in version.
    void add_version_to_graph(const Version& version);
    /// Delete a version from graph. Notice that this del operation only remove this edges and
    /// remain the vertex.
    /// NOTICE: we assume only redundant versions will be deleted, so deleting edge does not
    /// move max_continuous_version backward
    Status delete_version_from_graph(const Version& version);
    /// Given a spec_version, this method can find a version path which is the shortest path
    /// in the graph. The version paths are added to version_path as return info.
    Status capture_consistent_versions(const Version& spec_version, std::vector<Version>* version_path) const;

    // Get max continuous version from 0
    int64_t max_continuous_version() const { return _max_continuous_version; }

    void update_max_continuous_version();

    int64_t min_readable_version() const { return _min_readable_version; }

private:
    /// Private method add a version to graph.
    std::unique_ptr<Vertex>& _add_vertex_to_graph(int64_t vertex_value);

    int64_t _get_max_continuous_version_from(int64_t version);
    void _add_version_to_graph(const Version& version);

    // OLAP version contains two parts, [start_version, end_version]. In order
    // to construct graph, the OLAP version has two corresponding vertex, one
    // vertex's value is version.start_version, the other is
    // version.end_version + 1.
    // Use adjacency list to describe version graph.
    std::unordered_map<int64_t, std::unique_ptr<Vertex>> _version_graph;
    // max continuous version from -1
    int64_t _max_continuous_version{-1};
    // minReadableVersion should:
    // >= compaction output's version.second if compaction's input rowsets are stale and removed from graph
    // <= _max_continuous_version
    // for example, suppose graph is:
    // [0-5] 6 7 8 9 10 11 12
    //       \-[6-10]-/
    // minReadableVersion is 5
    // and when 6, 7, 8, 9, 10 are stale and removed from graph by GC, graph became
    // [0-5] [6-10] 11 12
    // minReadableVersion will be updated to 10
    int64_t _min_readable_version{-1};

    // print log rate limit by tablet id
    int64_t _tablet_id{0};
};

/// TimestampedVersion class which is implemented to maintain multi-version path of rowsets.
/// This compaction info of a rowset includes start version, end version and the create time.
class TimestampedVersion {
public:
    /// TimestampedVersion construction function. Use rowset version and create time to build a TimestampedVersion.
    TimestampedVersion(const Version& version, int64_t create_time)
            : _version(version.first, version.second), _create_time(create_time) {}

    ~TimestampedVersion() = default;

    /// Return the rowset version of TimestampedVersion record.
    Version version() const { return _version; }
    /// Return the rowset create_time of TimestampedVersion record.
    int64_t get_create_time() { return _create_time; }

    /// Compare two version trackers.
    bool operator!=(const TimestampedVersion& rhs) const { return _version != rhs._version; }

    /// Compare two version trackers.
    bool operator==(const TimestampedVersion& rhs) const { return _version == rhs._version; }

    /// Judge if a tracker contains the other.
    bool contains(const TimestampedVersion& other) const { return _version.contains(other._version); }

private:
    Version _version;
    int64_t _create_time;
};

using TimestampedVersionSharedPtr = std::shared_ptr<TimestampedVersion>;

/// TimestampedVersionPathContainer class is used to maintain a path timestamped version path
/// and record the max create time in a path version. Once a timestamped version is added, the max_create_time
/// will compare with the version timestamp and be refreshed.
class TimestampedVersionPathContainer {
public:
    /// TimestampedVersionPathContainer construction function, max_create_time is assigned to 0.
    TimestampedVersionPathContainer() = default;

    /// Return the max create time in a path version.
    int64_t max_create_time() { return _max_create_time; }

    /// Add a timestamped version to timestamped_versions_container. Once a timestamped version is added,
    /// the max_create_time will compare with the version timestamp and be refreshed.
    void add_timestamped_version(const TimestampedVersionSharedPtr& version);

    /// Return the timestamped_versions_container as const type.
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions();

private:
    std::vector<TimestampedVersionSharedPtr> _timestamped_versions_container;
    int64_t _max_create_time{0};
};

using PathVersionListSharedPtr = std::shared_ptr<TimestampedVersionPathContainer>;

/// TimestampedVersionTracker class is responsible to track all rowsets version links of a tablet.
/// This class not only records the graph of all versions, but also records the paths which will be removed
/// after the path is expired.
class TimestampedVersionTracker {
public:
    /// Construct rowsets version tracker by rs_metas and stale version path map.
    void construct_versioned_tracker(const std::vector<RowsetMetaSharedPtr>& rs_metas);

    /// Add a version to tracker, this version is a new version rowset, not merged rowset.
    void add_version(const Version& version);

    /// Add a version path with stale_rs_metas, this versions in version path
    /// are merged rowsets.  These rowsets are tracked and removed after they are expired.
    /// TabletManager sweep these rowsets using tracker by timing.
    void add_stale_path_version(const std::vector<RowsetMetaSharedPtr>& stale_rs_metas);

    /// Given a spec_version, this method can find a version path which is the shortest path
    /// in the graph. The version paths are added to version_path as return info.
    /// If this version not in main version, version_path can be included expired rowset.
    Status capture_consistent_versions(const Version& spec_version, std::vector<Version>* version_path) const;

    /// Capture all expired path version.
    /// When the last rowset createtime of a path greater than expired time  which can be expressed
    /// "now() - tablet_rowset_stale_sweep_time_sec" , this path will be remained.
    /// Otherwise, this path will be added to path_version.
    void capture_expired_paths(int64_t stale_sweep_endtime, std::vector<int64_t>* path_version) const;

    /// Fetch all versions with a path_version.
    PathVersionListSharedPtr fetch_path_version_by_id(int64_t path_id);

    /// Fetch all versions with a path_version, at the same time remove this path from the tracker.
    /// Next time, fetch this path, it will return empty.
    PathVersionListSharedPtr fetch_and_delete_path_by_id(int64_t path_id);

    /// Print all expired version path in a tablet.
    std::string _get_current_path_map_str();

    /// Get json document of _stale_version_path_map. Fill the path_id and version_path
    /// list in the document. The parameter path arr is used as return variable.
    void get_stale_version_path_json_doc(rapidjson::Document& path_arr);

    // Get max continuous version from 0
    int64_t get_max_continuous_version() const;

    void update_max_continuous_version();

    int64_t get_min_readable_version() const;

private:
    /// Construct rowsets version tracker with stale rowsets.
    void _construct_versioned_tracker(const std::vector<RowsetMetaSharedPtr>& rs_metas);

private:
    // This variable records the id of path version which will be dispatched to next path version,
    // it is not persisted.
    int64_t _next_path_id = 1;

    // path_version -> list of path version,
    // This variable is used to maintain the map from path version and it's all version.
    std::map<int64_t, PathVersionListSharedPtr> _stale_version_path_map;

    VersionGraph _version_graph;
};

} // namespace starrocks
