//
// Created by Kevin Xu on 2025/2/25.
//
#pragma once

#include <gen_cpp/olap_file.pb.h>

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
    bool isFlatJsonEnabled() const { return _flat_json_enable; }
    void setFlatJsonEnabled(bool enable) { _flat_json_enable = enable; }

    double getFlatJsonNullFactor() const { return _flat_json_null_factor; }
    void setFlatJsonNullFactor(double factor) { _flat_json_null_factor = factor; }

    double getFlatJsonSparsityFactor() const { return _flat_json_sparsity_factor; }
    void setFlatJsonSparsityFactor(double factor) { _flat_json_sparsity_factor = factor; }

    int getFlatJsonMaxColumnMax() const { return _flat_json_max_column_max; }
    void setFlatJsonMaxColumnMax(int max) { _flat_json_max_column_max = max; }


    void to_pb(FlatJsonConfigPB* binlog_config_pb) {
        binlog_config_pb->set_flat_json_enable(_flat_json_enable);
        binlog_config_pb->set_flat_json_null_factor(_flat_json_null_factor);
        binlog_config_pb->set_flat_json_sparsity_factor(_flat_json_sparsity_factor);
        binlog_config_pb->set_flat_json_max_column_max(_flat_json_max_column_max);
    }

    // Update function using another FlatJsonConfig
    void update(const FlatJsonConfig& config) {
        update(config.isFlatJsonEnabled(), config.getFlatJsonNullFactor(), config.getFlatJsonSparsityFactor(),
            config.getFlatJsonMaxColumnMax());
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

private:
    bool _flat_json_enable = false;
    double _flat_json_null_factor = 0.3;
    double _flat_json_sparsity_factor = 0.9;
    int _flat_json_max_column_max = 100;
};
}