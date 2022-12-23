// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <random>
#include "star_cache/admission_policy.h"

namespace starrocks::starcache {

// The class to handle admission control logic based on object size
class SizeBasedAdmissionPolicy : public AdmissionPolicy {
public:
    struct Config {
        // Only the object that smaller than this size will be checked by this policy
        size_t max_check_size;
        // The probability to flush the small block
        double flush_probability;
        // The probability to delete the small block
        double delete_probability;
    };
    explicit SizeBasedAdmissionPolicy(const Config& config);
    ~SizeBasedAdmissionPolicy() = default;

	BlockAdmission check_admission(const CacheItemPtr& cache_item, const BlockKey& block_key) override;

private:
    size_t _max_check_size;
    double _flush_probability;
    double _delete_probability;
    std::minstd_rand _rand_generator;
};

} // namespace starrocks::starcache
