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

#pragma once

#include <gen_cpp/olap_file.pb.h>

#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "gen_cpp/AgentService_types.h"

namespace starrocks {
class FlatJsonConfig {
public:
    // Default max force-path columns when not specified by the caller.
    static constexpr int DEFAULT_COLUMN_PATHS_MAX = 200;

    // Constructor
    FlatJsonConfig();

    // Constructor with parameters
    FlatJsonConfig(bool enable, double nullFactor, double sparsityFactor, int maxColumnMax)
            : _flat_json_enable(enable),
              _flat_json_null_factor(nullFactor),
              _flat_json_sparsity_factor(sparsityFactor),
              _flat_json_max_column_max(maxColumnMax),
              _flat_json_column_paths_max(DEFAULT_COLUMN_PATHS_MAX) {}

    // Getters and Setters
    bool is_flat_json_enabled() const { return _flat_json_enable; }
    void set_flat_json_enabled(bool enable) { _flat_json_enable = enable; }

    double get_flat_json_null_factor() const { return _flat_json_null_factor; }
    void set_flat_json_null_factor(double factor) { _flat_json_null_factor = factor; }

    double get_flat_json_sparsity_factor() const { return _flat_json_sparsity_factor; }
    void set_flat_json_sparsity_factor(double factor) { _flat_json_sparsity_factor = factor; }

    int get_flat_json_max_column_max() const { return _flat_json_max_column_max; }
    void set_flat_json_max_column_max(int max) { _flat_json_max_column_max = max; }

    // Paths that must always be flattened, regardless of sparsity.
    // Stored in internal dot-separated format without a leading "$.".
    const std::unordered_set<std::string>& get_column_paths() const { return _flat_json_column_paths; }
    void set_column_paths(const std::vector<std::string>& paths) {
        _flat_json_column_paths.clear();
        for (const auto& p : paths) {
            _flat_json_column_paths.insert(p);
        }
    }

    int get_column_paths_max() const { return _flat_json_column_paths_max; }
    void set_column_paths_max(int max) { _flat_json_column_paths_max = max; }

    void to_pb(FlatJsonConfigPB* binlog_config_pb) {
        binlog_config_pb->set_flat_json_enable(_flat_json_enable);
        binlog_config_pb->set_flat_json_null_factor(_flat_json_null_factor);
        binlog_config_pb->set_flat_json_sparsity_factor(_flat_json_sparsity_factor);
        binlog_config_pb->set_flat_json_max_column_max(_flat_json_max_column_max);
        binlog_config_pb->clear_flat_json_column_paths();
        for (const auto& p : _flat_json_column_paths) {
            binlog_config_pb->add_flat_json_column_paths(p);
        }
        binlog_config_pb->set_flat_json_column_paths_max(_flat_json_column_paths_max);
    }

    // Update function using another FlatJsonConfig
    void update(const FlatJsonConfig& config) {
        _flat_json_enable = config.is_flat_json_enabled();
        _flat_json_null_factor = config.get_flat_json_null_factor();
        _flat_json_sparsity_factor = config.get_flat_json_sparsity_factor();
        _flat_json_max_column_max = config.get_flat_json_max_column_max();
        _flat_json_column_paths = config.get_column_paths();
        _flat_json_column_paths_max = config.get_column_paths_max();
    }

    void update(const TFlatJsonConfig& config) {
        _flat_json_enable = config.flat_json_enable;
        _flat_json_null_factor = config.flat_json_null_factor;
        _flat_json_sparsity_factor = config.flat_json_sparsity_factor;
        _flat_json_max_column_max = config.flat_json_column_max;
        if (config.__isset.flat_json_column_paths) {
            set_column_paths(std::vector<std::string>(config.flat_json_column_paths.begin(),
                                                     config.flat_json_column_paths.end()));
        }
        if (config.__isset.flat_json_column_paths_max && config.flat_json_column_paths_max > 0) {
            _flat_json_column_paths_max = static_cast<int>(config.flat_json_column_paths_max);
        }
    }

    void update(const FlatJsonConfigPB& flat_json_config_pb) {
        _flat_json_enable = flat_json_config_pb.flat_json_enable();
        _flat_json_null_factor = flat_json_config_pb.flat_json_null_factor();
        _flat_json_sparsity_factor = flat_json_config_pb.flat_json_sparsity_factor();
        _flat_json_max_column_max = flat_json_config_pb.flat_json_max_column_max();
        _flat_json_column_paths.clear();
        for (const auto& p : flat_json_config_pb.flat_json_column_paths()) {
            _flat_json_column_paths.insert(p);
        }
        if (flat_json_config_pb.has_flat_json_column_paths_max() &&
            flat_json_config_pb.flat_json_column_paths_max() > 0) {
            _flat_json_column_paths_max = static_cast<int>(flat_json_config_pb.flat_json_column_paths_max());
        }
    }

    // Copy Assignment
    FlatJsonConfig& operator=(const FlatJsonConfig& other) {
        if (this != &other) {
            _flat_json_enable = other._flat_json_enable;
            _flat_json_null_factor = other._flat_json_null_factor;
            _flat_json_sparsity_factor = other._flat_json_sparsity_factor;
            _flat_json_max_column_max = other._flat_json_max_column_max;
            _flat_json_column_paths = other._flat_json_column_paths;
            _flat_json_column_paths_max = other._flat_json_column_paths_max;
        }
        return *this;
    }

    std::string to_string() const {
        std::ostringstream oss;
        oss << "FlatJsonConfig{";
        oss << "flat_json_enable=" << (_flat_json_enable ? "true" : "false") << ", ";
        oss << "flat_json_null_factor=" << _flat_json_null_factor << ", ";
        oss << "flat_json_sparsity_factor=" << _flat_json_sparsity_factor << ", ";
        oss << "flat_json_max_column_max=" << _flat_json_max_column_max << ", ";
        oss << "flat_json_column_paths=[";
        bool first = true;
        for (const auto& p : _flat_json_column_paths) {
            if (!first) oss << ",";
            oss << p;
            first = false;
        }
        oss << "], ";
        oss << "flat_json_column_paths_max=" << _flat_json_column_paths_max;
        oss << "}";
        return oss.str();
    }

private:
    bool _flat_json_enable = false;
    double _flat_json_null_factor = 0;
    double _flat_json_sparsity_factor = 0;
    int _flat_json_max_column_max = 0;
    // Force-flatten paths (dot-separated, no leading "$.").
    std::unordered_set<std::string> _flat_json_column_paths;
    int _flat_json_column_paths_max = DEFAULT_COLUMN_PATHS_MAX;
};
} // namespace starrocks
