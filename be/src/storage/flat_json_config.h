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

#include "common/config.h"
#include "gen_cpp/AgentService_types.h"

namespace starrocks {
class FlatJsonConfig {
public:
    // Constructor
    FlatJsonConfig() = default;

    // Constructor with parameters
    FlatJsonConfig(bool enable, double nullFactor, double sparsityFactor, int maxColumnMax)
            : _flat_json_enable(enable),
              _flat_json_null_factor(nullFactor),
              _flat_json_sparsity_factor(sparsityFactor),
              _flat_json_max_column_max(maxColumnMax) {}

    // Getters and Setters
    bool is_flat_json_enabled() const { return _flat_json_enable; }
    void set_flat_json_enabled(bool enable) { _flat_json_enable = enable; }

    double get_flat_json_null_factor() const { return _flat_json_null_factor; }
    void set_flat_json_null_factor(double factor) { _flat_json_null_factor = factor; }

    double get_flat_json_sparsity_factor() const { return _flat_json_sparsity_factor; }
    void set_flat_json_sparsity_factor(double factor) { _flat_json_sparsity_factor = factor; }

    int get_flat_json_max_column_max() const { return _flat_json_max_column_max; }
    void set_flat_json_max_column_max(int max) { _flat_json_max_column_max = max; }

    void to_pb(FlatJsonConfigPB* binlog_config_pb) {
        binlog_config_pb->set_flat_json_enable(_flat_json_enable);
        binlog_config_pb->set_flat_json_null_factor(_flat_json_null_factor);
        binlog_config_pb->set_flat_json_sparsity_factor(_flat_json_sparsity_factor);
        binlog_config_pb->set_flat_json_max_column_max(_flat_json_max_column_max);
    }

    // Update function using another FlatJsonConfig
    void update(const FlatJsonConfig& config) {
        update(config.is_flat_json_enabled(), config.get_flat_json_null_factor(),
               config.get_flat_json_sparsity_factor(), config.get_flat_json_max_column_max());
    }

    void update(const TFlatJsonConfig& config) {
        update(config.flat_json_enable, config.flat_json_null_factor, config.flat_json_sparsity_factor,
               config.flat_json_column_max);
    }

    void update(const FlatJsonConfigPB& flat_json_config_pb) {
        update(flat_json_config_pb.flat_json_enable(), flat_json_config_pb.flat_json_null_factor(),
               flat_json_config_pb.flat_json_sparsity_factor(), flat_json_config_pb.flat_json_max_column_max());
    }

    // Copy Assignment
    FlatJsonConfig& operator=(const FlatJsonConfig& other) {
        if (this != &other) {
            _flat_json_enable = other._flat_json_enable;
            _flat_json_null_factor = other._flat_json_null_factor;
            _flat_json_sparsity_factor = other._flat_json_sparsity_factor;
            _flat_json_max_column_max = other._flat_json_max_column_max;
        }
        return *this;
    }

    // Update function using four parameters
    void update(bool enable, double nullFactor, double sparsityFactor, int maxColumnMax) {
        _flat_json_enable = enable;
        _flat_json_null_factor = nullFactor;
        _flat_json_sparsity_factor = sparsityFactor;
        _flat_json_max_column_max = maxColumnMax;
    }

    std::string to_string() const {
        std::ostringstream oss;
        oss << "FlatJsonConfig{";
        oss << "flat_json_enable=" << (_flat_json_enable ? "true" : "false") << ", ";
        oss << "flat_json_null_factor=" << _flat_json_null_factor << ", ";
        oss << "flat_json_sparsity_factor=" << _flat_json_sparsity_factor << ", ";
        oss << "flat_json_max_column_max=" << _flat_json_max_column_max;
        oss << "}";
        return oss.str();
    }

private:
    bool _flat_json_enable = false;
    double _flat_json_null_factor = config::json_flat_null_factor;
    double _flat_json_sparsity_factor = config::json_flat_sparsity_factor;
    int _flat_json_max_column_max = config::json_flat_column_max;
};
} // namespace starrocks