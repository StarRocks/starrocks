// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <vector>

#include "exec/vectorized/hash_joiner.h"

namespace starrocks {
namespace pipeline {

using HashJoiner = starrocks::vectorized::HashJoiner;
using HashJoinerPtr = std::shared_ptr<HashJoiner>;
using HashJoiners = std::vector<HashJoinerPtr>;
class HashJoinerFactory;
using HashJoinerFactoryPtr = std::shared_ptr<HashJoinerFactory>;

class HashJoinerFactory {
public:
    HashJoinerFactory(starrocks::vectorized::HashJoinerParam& param, int dop)
            : _param(param), _builder_map(dop), _read_only_prober_map(dop) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);

    HashJoinerPtr create_prober(int driver_sequence) {
        if (!_need_read_only_prober()) {
            return create_builder(driver_sequence);
        }

        if (!_read_only_prober_map[driver_sequence]) {
            _read_only_prober_map[driver_sequence] = std::make_shared<HashJoiner>(_param, _read_only_probers);
            _read_only_probers.emplace_back(_read_only_prober_map[driver_sequence]);
        }
        return _read_only_prober_map[driver_sequence];
    }

    HashJoinerPtr create_builder(int driver_sequence) {
        if (_is_broadcast()) {
            driver_sequence = BROADCAST_BUILD_DRIVER_SEQUENCE;
        }

        if (!_builder_map[driver_sequence]) {
            _builder_map[driver_sequence] = std::make_shared<HashJoiner>(_param, _read_only_probers);
        }
        return _builder_map[driver_sequence];
    }

    const HashJoiners& get_read_only_probers() const { return _read_only_probers; }

private:
    bool _is_broadcast() const { return _param._distribution_mode == TJoinDistributionMode::BROADCAST; }

    bool _need_read_only_prober() const {
        const size_t prober_dop = _read_only_prober_map.size();
        return _is_broadcast() && prober_dop > 1;
    }

    // Broadcast join need only create one hash table, because all the HashJoinProbeOperators
    // use the same hash table with their own different probe states.
    static constexpr int BROADCAST_BUILD_DRIVER_SEQUENCE = 0;

    starrocks::vectorized::HashJoinerParam _param;
    // For broadcast join:
    // - There are one builder and `DOP` probers.
    // - Each prober references the same hash table from the builder and has its own probe state.
    //   - When DOP > 1, the prober and builder are different.
    //   - When DOP = 1, the prober and builder share the same joiner.
    //
    // For none-broadcast join:
    // - There are `DOP` joiners.
    // - The builder and joiner with the same driver sequence share the same joiner.
    HashJoiners _builder_map;
    HashJoiners _read_only_prober_map;
    HashJoiners _read_only_probers;
};

} // namespace pipeline
} // namespace starrocks
