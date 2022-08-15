// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/driver_limiter.h"

namespace starrocks::pipeline {

StatusOr<DriverLimiter::TokenPtr> DriverLimiter::try_acquire(int num_drivers) {
    int prev_num_total_drivers = _num_total_drivers.fetch_add(num_drivers);
    if (prev_num_total_drivers >= _max_num_drivers) {
        _num_total_drivers.fetch_sub(num_drivers);
        return Status::TooManyTasks(
                strings::Substitute("BE has overloaded with $0 pipeline drivers", prev_num_total_drivers));
    }
    return std::make_unique<DriverLimiter::Token>(_num_total_drivers, num_drivers);
}

} // namespace starrocks::pipeline
