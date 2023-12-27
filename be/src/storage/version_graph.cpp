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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/version_graph.cpp

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

#include "storage/version_graph.h"

#include <cctz/time_zone.h>

#include <memory>
#include <queue>

#include "common/logging.h"
#include "util/ratelimit.h"
#include "util/time.h"

namespace starrocks {

void TimestampedVersionTracker::_construct_versioned_tracker(const std::vector<RowsetMetaSharedPtr>& rs_metas) {
    int64_t max_version = 0;

    // construct the rowset graph
    _version_graph.construct_version_graph(rs_metas, &max_version);
}

void TimestampedVersionTracker::construct_versioned_tracker(const std::vector<RowsetMetaSharedPtr>& rs_metas) {
    if (rs_metas.empty()) {
        VLOG(3) << "there is no version in the header.";
        return;
    }
    _stale_version_path_map.clear();
    _next_path_id = 1;
    _construct_versioned_tracker(rs_metas);
}

void TimestampedVersionTracker::get_stale_version_path_json_doc(rapidjson::Document& path_arr) {
    auto path_arr_iter = _stale_version_path_map.begin();

    // do loop version path
    while (path_arr_iter != _stale_version_path_map.end()) {
        auto path_id = path_arr_iter->first;
        auto path_version_path = path_arr_iter->second;

        rapidjson::Document item;
        item.SetObject();
        // add path_id to item
        auto path_id_str = std::to_string(path_id);
        rapidjson::Value path_id_value;
        path_id_value.SetString(path_id_str.c_str(), path_id_str.length(), path_arr.GetAllocator());
        item.AddMember("path id", path_id_value, path_arr.GetAllocator());

        // add max create time to item
        auto time_zone = cctz::local_time_zone();

        auto tp = std::chrono::system_clock::from_time_t(path_version_path->max_create_time());
        auto create_time_str = cctz::format("%Y-%m-%d %H:%M:%S %z", tp, time_zone);

        rapidjson::Value create_time_value;
        create_time_value.SetString(create_time_str.c_str(), create_time_str.length(), path_arr.GetAllocator());
        item.AddMember("last create time", create_time_value, path_arr.GetAllocator());

        // add path list to item
        std::stringstream path_list_stream;
        path_list_stream << path_id_str;
        auto path_list_ptr = path_version_path->timestamped_versions();
        auto path_list_iter = path_list_ptr.begin();
        while (path_list_iter != path_list_ptr.end()) {
            path_list_stream << " -> ";
            path_list_stream << "[";
            path_list_stream << (*path_list_iter)->version().first;
            path_list_stream << "-";
            path_list_stream << (*path_list_iter)->version().second;
            path_list_stream << "]";
            path_list_iter++;
        }
        std::string path_list = path_list_stream.str();
        rapidjson::Value path_list_value;
        path_list_value.SetString(path_list.c_str(), path_list.length(), path_arr.GetAllocator());
        item.AddMember("path list", path_list_value, path_arr.GetAllocator());

        // add item to path_arr
        path_arr.PushBack(item, path_arr.GetAllocator());

        path_arr_iter++;
    }
}

int64_t TimestampedVersionTracker::get_max_continuous_version() const {
    return _version_graph.max_continuous_version();
}

void TimestampedVersionTracker::update_max_continuous_version() {
    _version_graph.update_max_continuous_version();
}

int64_t TimestampedVersionTracker::get_min_readable_version() const {
    return _version_graph.min_readable_version();
}

void TimestampedVersionTracker::add_version(const Version& version) {
    _version_graph.add_version_to_graph(version);
}

void TimestampedVersionTracker::add_stale_path_version(const std::vector<RowsetMetaSharedPtr>& stale_rs_metas) {
    if (stale_rs_metas.empty()) {
        VLOG(3) << "there is no version in the stale_rs_metas.";
        return;
    }

    PathVersionListSharedPtr ptr(new TimestampedVersionPathContainer());
    for (const auto& rs : stale_rs_metas) {
        TimestampedVersionSharedPtr vt_ptr(new TimestampedVersion(rs->version(), rs->creation_time()));
        ptr->add_timestamped_version(vt_ptr);
    }

    std::vector<TimestampedVersionSharedPtr>& timestamped_versions = ptr->timestamped_versions();

    struct TimestampedVersionPtrCompare {
        bool operator()(const TimestampedVersionSharedPtr& ptr1, const TimestampedVersionSharedPtr& ptr2) {
            return ptr1->version().first < ptr2->version().first;
        }
    };
    sort(timestamped_versions.begin(), timestamped_versions.end(), TimestampedVersionPtrCompare());
    _stale_version_path_map[_next_path_id] = ptr;
    _next_path_id++;
}

// Capture consistent versions from graph
Status TimestampedVersionTracker::capture_consistent_versions(const Version& spec_version,
                                                              std::vector<Version>* version_path) const {
    return _version_graph.capture_consistent_versions(spec_version, version_path);
}

void TimestampedVersionTracker::capture_expired_paths(int64_t stale_sweep_endtime,
                                                      std::vector<int64_t>* path_version_vec) const {
    auto iter = _stale_version_path_map.begin();

    while (iter != _stale_version_path_map.end()) {
        int64_t max_create_time = iter->second->max_create_time();
        if (max_create_time <= stale_sweep_endtime) {
            int64_t path_version = iter->first;
            path_version_vec->push_back(path_version);
        }
        iter++;
    }
}

PathVersionListSharedPtr TimestampedVersionTracker::fetch_path_version_by_id(int64_t path_id) {
    if (_stale_version_path_map.count(path_id) == 0) {
        VLOG(3) << "path version " << path_id << " does not exist!";
        return nullptr;
    }

    return _stale_version_path_map[path_id];
}

PathVersionListSharedPtr TimestampedVersionTracker::fetch_and_delete_path_by_id(int64_t path_id) {
    if (_stale_version_path_map.count(path_id) == 0) {
        VLOG(3) << "path version " << path_id << " does not exist!";
        return nullptr;
    }

    PathVersionListSharedPtr ptr = fetch_path_version_by_id(path_id);

    _stale_version_path_map.erase(path_id);

    for (auto& version : ptr->timestamped_versions()) {
        _version_graph.delete_version_from_graph(version->version());
    }
    return ptr;
}

std::string TimestampedVersionTracker::_get_current_path_map_str() {
    std::stringstream tracker_info;
    tracker_info << "current expired next_path_id " << _next_path_id << std::endl;

    auto iter = _stale_version_path_map.begin();
    while (iter != _stale_version_path_map.end()) {
        tracker_info << "current expired path_version " << iter->first;
        std::vector<TimestampedVersionSharedPtr>& timestamped_versions = iter->second->timestamped_versions();
        auto version_path_iter = timestamped_versions.begin();
        int64_t max_create_time = -1;
        while (version_path_iter != timestamped_versions.end()) {
            if (max_create_time < (*version_path_iter)->get_create_time()) {
                max_create_time = (*version_path_iter)->get_create_time();
            }
            tracker_info << " -> [";
            tracker_info << (*version_path_iter)->version().first;
            tracker_info << ",";
            tracker_info << (*version_path_iter)->version().second;
            tracker_info << "]";

            version_path_iter++;
        }

        tracker_info << std::endl;
        iter++;
    }
    return tracker_info.str();
}

void TimestampedVersionPathContainer::add_timestamped_version(const TimestampedVersionSharedPtr& version) {
    // compare and refresh _max_create_time
    if (version->get_create_time() > _max_create_time) {
        _max_create_time = version->get_create_time();
    }
    _timestamped_versions_container.push_back(version);
}

std::vector<TimestampedVersionSharedPtr>& TimestampedVersionPathContainer::timestamped_versions() {
    return _timestamped_versions_container;
}

void VersionGraph::update_max_continuous_version() {
    _max_continuous_version = _get_max_continuous_version_from(0);
}

void VersionGraph::construct_version_graph(const std::vector<RowsetMetaSharedPtr>& rs_metas, int64_t* max_version) {
    _version_graph.clear();
    _max_continuous_version = -1;
    _min_readable_version = -1;
    if (rs_metas.empty()) {
        VLOG(3) << "there is no version in the header.";
        return;
    }

    // Create edges for version graph according to TabletMeta's versions.
    for (const auto& rs_meta : rs_metas) {
        // Versions in header are unique.
        _add_version_to_graph(rs_meta->version());

        if (max_version != nullptr && *max_version < rs_meta->end_version()) {
            *max_version = rs_meta->end_version();
        }
    }
    _max_continuous_version = _get_max_continuous_version_from(0);
    _min_readable_version = -1;
    for (const auto& rs_meta : rs_metas) {
        const auto& version = rs_meta->version();
        if (version.second <= _max_continuous_version && version.first != version.second) {
            // it's a compacted version
            _min_readable_version = std::max(version.second, _min_readable_version);
        }
    }
    _tablet_id = rs_metas[0]->tablet_id();
}

void VersionGraph::add_version_to_graph(const Version& version) {
    _add_version_to_graph(version);
    if (version.first == _max_continuous_version + 1) {
        _max_continuous_version = _get_max_continuous_version_from(_max_continuous_version + 1);
    } else if (version.first == 0) {
        // We need to reconstruct max_continuous_version from zero if input version is starting from zero
        // e.g.
        // 1. Tablet A is doing schema change
        // 2. We create a new tablet B releated A, and we will create a initial rowset and _max_continuous_version
        //    will be updated to 1
        // 3. Tablet A has a rowset R with version (0, m)
        // 4. Schema change will try convert R
        // 5. The start version of R (0) is not equal to `_max_continuous_version + 1`, and the _max_continuous_version
        //    will not update
        _max_continuous_version = _get_max_continuous_version_from(0);
    }
}

void VersionGraph::_add_version_to_graph(const Version& version) {
    // Add version.first as new vertex of version graph if not exist.
    auto& start_vertex = _add_vertex_to_graph(version.first);
    // Add version.second + 1 as new vertex of version graph if not exist.
    auto& end_vertex = _add_vertex_to_graph(version.second + 1);

    // sorted by version, we assume one vertex has few edge so that we use O(n) algorithm
    auto end_vertex_it = start_vertex->edges.begin();
    while (end_vertex_it != start_vertex->edges.end()) {
        if ((*end_vertex_it)->value < end_vertex->value) {
            break;
        }
        end_vertex_it++;
    }
    start_vertex->edges.insert(end_vertex_it, end_vertex.get());

    // We add reverse edge(from end_version to start_version) to graph
    auto start_vertex_it = end_vertex->edges.begin();
    while (start_vertex_it != end_vertex->edges.end()) {
        if ((*start_vertex_it)->value < start_vertex->value) {
            break;
        }
        start_vertex_it++;
    }
    end_vertex->edges.insert(start_vertex_it, start_vertex.get());
}

Status VersionGraph::delete_version_from_graph(const Version& version) {
    int64_t start_vertex_value = version.first;
    int64_t end_vertex_value = version.second + 1;
    auto start_iter = _version_graph.find(start_vertex_value);
    auto end_iter = _version_graph.find(end_vertex_value);

    if (start_iter == _version_graph.end() || end_iter == _version_graph.end()) {
        LOG(WARNING) << "vertex for version does not exists. "
                     << "version=" << version.first << "-" << version.second;
        return Status::NotFound("Not found version");
    }

    // Remove edge and its reverse edge.
    // When there are same versions in edges, just remove the frist version.
    auto start_edges_iter = start_iter->second->edges.begin();
    while (start_edges_iter != start_iter->second->edges.end()) {
        if ((*start_edges_iter)->value == end_vertex_value) {
            start_iter->second->edges.erase(start_edges_iter);
            break;
        }
        start_edges_iter++;
    }

    auto end_edges_iter = end_iter->second->edges.begin();
    while (end_edges_iter != end_iter->second->edges.end()) {
        if ((*end_edges_iter)->value == start_vertex_value) {
            end_iter->second->edges.erase(end_edges_iter);
            break;
        }
        end_edges_iter++;
    }

    if (start_iter->second->edges.empty()) {
        _version_graph.erase(start_iter);
    }

    if (end_iter->second->edges.empty()) {
        _version_graph.erase(end_iter);
    }

    if (version.second <= _max_continuous_version) {
        _min_readable_version = std::max(_min_readable_version, version.second);
    }
    return Status::OK();
}

std::unique_ptr<Vertex>& VersionGraph::_add_vertex_to_graph(int64_t vertex_value) {
    // Vertex with vertex_value already exists.
    auto iter = _version_graph.find(vertex_value);
    if (iter != _version_graph.end()) {
        VLOG(3) << "vertex with vertex value already exists. value=" << vertex_value;
        return iter->second;
    }

    _version_graph[vertex_value] = std::make_unique<Vertex>(vertex_value);

    return _version_graph[vertex_value];
}

int64_t VersionGraph::_get_max_continuous_version_from(int64_t version) {
    auto iter = _version_graph.find(version);
    if (iter == _version_graph.end()) {
        // should not happen, just log error and return from version
        LOG(ERROR) << "_get_max_continuous_version_from failed: from version not found " << version;
        return version;
    }
    Vertex* cur = iter->second.get();
    while (true) {
        if (cur->edges.empty()) {
            break;
        }
        auto largest_next = cur->edges.front();
        if (largest_next->value < cur->value) {
            break;
        }
        cur = largest_next;
    }
    return cur->value - 1;
}

Status VersionGraph::capture_consistent_versions(const Version& spec_version,
                                                 std::vector<Version>* version_path) const {
    if (spec_version.first > spec_version.second) {
        LOG(WARNING) << "invalid specified version. "
                     << "spec_version=" << spec_version.first << "-" << spec_version.second;
        return Status::InternalError("Invalid specified version");
    }

    auto start_vertex_iter = _version_graph.find(spec_version.first);
    auto end_vertex_iter = _version_graph.find(spec_version.second + 1);

    if (start_vertex_iter == _version_graph.end() || end_vertex_iter == _version_graph.end()) {
        RATE_LIMIT_BY_TAG(_tablet_id,
                          LOG(WARNING) << "fail to find path in version_graph. "
                                       << "spec_version: " << spec_version.first << "-" << spec_version.second
                                       << " tablet_id: " << _tablet_id,
                          1000);
        return Status::NotFound("Version not found");
    }

    int64_t end_value = spec_version.second + 1;
    auto cur_vertex = start_vertex_iter->second.get();
    while (cur_vertex->value != end_value) {
        Vertex* next_vertex = nullptr;
        for (const auto edge : cur_vertex->edges) {
            // reverse edge
            if (edge->value < cur_vertex->value) {
                break;
            }

            // cross edge
            // NOTICE: we assume if a version can be reached, then every down path can lead to that version
            // so using greedy algorithm is OK
            if (edge->value > end_value) {
                continue;
            }

            next_vertex = edge;
            break;
        }

        if (next_vertex != nullptr) {
            if (version_path != nullptr) {
                version_path->emplace_back(cur_vertex->value, next_vertex->value - 1);
            }
            cur_vertex = next_vertex;
        } else {
            LOG(WARNING) << "fail to find path in version_graph. "
                         << "spec_version: " << spec_version.first << "-" << spec_version.second;
            return Status::NotFound("Version not found");
        }
    }

    if (VLOG_ROW_IS_ON && version_path != nullptr) {
        std::stringstream shortest_path_for_debug;
        for (const auto& version : *version_path) {
            shortest_path_for_debug << version << ' ';
        }
        VLOG_ROW << "success to find path for spec_version. spec_version=" << spec_version
                 << ", path=" << shortest_path_for_debug.str();
    }

    return Status::OK();
}

} // namespace starrocks
