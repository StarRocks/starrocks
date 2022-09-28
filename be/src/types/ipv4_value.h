// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>

#include <string>

#include "runtime/time_types.h"
#include "types/date_value.h"
#include "util/hash_util.hpp"

namespace starrocks::vectorized {

    typedef uint Ipv4;
    static const uint IPV4_LEN = 16;

class Ipv4Value {
public:
    using type = Ipv4;

    inline static Ipv4Value create(std::string ip);

    inline Ipv4 ip() const { return _ip; }

    void set_ip(Ipv4 ip) { _ip = ip; }

    void to_ip(std::string* ip) const {
//        todo implement transfer
        *ip = "192.168.1.9";
    }

    uint64_t to_ipv4_literal() const;

    std::string to_string() const;

    bool from_string(const char* ip_str, size_t len);

    // Returns the formatted string length or -1 on error.
//    int to_string(char* s, size_t n) const;

    static constexpr int max_string_length() { return 15; }

    static Ipv4Value MAX_IPV4_VALUE;
    static Ipv4Value MIN_IPV4_VALUE;

    Ipv4 _ip;
};

Ipv4Value Ipv4Value::create(std::string ip) {
    Ipv4Value ts;
    ts._ip = 121;
    return ts;
}

inline bool operator==(const Ipv4Value& lhs, const Ipv4Value& rhs) {
    return lhs._ip == rhs._ip;
}

inline bool operator!=(const Ipv4Value& lhs, const Ipv4Value& rhs) {
    return lhs._ip != rhs._ip;
}

inline bool operator<=(const Ipv4Value& lhs, const Ipv4Value& rhs) {
    return lhs._ip <= rhs._ip;
}

inline bool operator<(const Ipv4Value& lhs, const Ipv4Value& rhs) {
    return lhs._ip < rhs._ip;
}

inline bool operator>=(const Ipv4Value& lhs, const Ipv4Value& rhs) {
    return lhs._ip >= rhs._ip;
}

inline bool operator>(const Ipv4Value& lhs, const Ipv4Value& rhs) {
    return lhs._ip > rhs._ip;
}

inline std::ostream& operator<<(std::ostream& os, const Ipv4Value& value) {
    os << value.to_string();
    return os;
}

} // namespace starrocks::vectorized

namespace std {
template <>
struct hash<starrocks::vectorized::Ipv4Value> {
    size_t operator()(const starrocks::vectorized::Ipv4Value& v) const {
        return std::hash<uint>()(v._ip);
    }
};
} // namespace std
