// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "util/int96.h"

#include "runtime/large_int_value.h"

namespace starrocks {

std::string int96_t::to_string() const {
    std::stringstream os;
    __int128 val128 = ((__int128)hi << 64) + lo;
    starrocks::operator<<(os, val128);
    return os.str();
}

std::ostream& operator<<(std::ostream& os, const int96_t& val) {
    __int128 val128 = ((__int128)val.hi << 64) + val.lo;
    starrocks::operator<<(os, val128);
    return os;
}

} // namespace starrocks
