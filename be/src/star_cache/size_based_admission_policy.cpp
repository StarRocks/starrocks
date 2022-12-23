// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/size_based_admission_policy.h"

namespace starrocks::starcache {

SizeBasedAdmissionPolicy::SizeBasedAdmissionPolicy(const Config& config) 
    : _max_check_size(config.max_check_size)
    , _flush_probability(config.flush_probability)
    , _delete_probability(config.delete_probability)
    , _rand_generator(std::rand()) {}

BlockAdmission SizeBasedAdmissionPolicy::check_admission(const CacheItemPtr& cache_item, const BlockKey& block_key) {
    if (cache_item->size >= _max_check_size) {
        return BlockAdmission::FLUSH;
    }

    double size_ratio = static_cast<double>(cache_item->size) / _max_check_size;
    double probability = static_cast<double>(_rand_generator(), _rand_generator.max());
    if (probability < size_ratio * _flush_probability) {
        return BlockAdmission::FLUSH;
    }
    if (probability < size_ratio * _delete_probability) {
        return BlockAdmission::DELETE;
    }

    return BlockAdmission::SKIP;
}

} // namespace starrocks::starcache
