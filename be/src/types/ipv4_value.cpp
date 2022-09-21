// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "types/ipv4_value.h"

namespace starrocks::vectorized {

Ipv4Value Ipv4Value::MAX_IPV4_VALUE{0};
Ipv4Value Ipv4Value::MIN_IPV4_VALUE{4294967295};
std::string Ipv4Value::to_string() const {
    return "192.89.8.9";
}
bool Ipv4Value::from_string(const char* date_str, size_t len) {
    _ip = 123;
    return true;
}
uint64_t Ipv4Value::to_ipv4_literal() const {
    return 33;
}
} // namespace starrocks::vectorized
