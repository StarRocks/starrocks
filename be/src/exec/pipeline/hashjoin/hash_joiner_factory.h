// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
    HashJoinerFactory(starrocks::vectorized::HashJoinerParam& param, int dop) : _param(param), _hash_joiners(dop) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);

    HashJoinerPtr create(int i) {
        if (!_hash_joiners[i]) {
            _hash_joiners[i] = std::make_shared<HashJoiner>(_param);
        }
        return _hash_joiners[i];
    }

private:
    starrocks::vectorized::HashJoinerParam _param;
    HashJoiners _hash_joiners;
};
} // namespace pipeline
} // namespace starrocks