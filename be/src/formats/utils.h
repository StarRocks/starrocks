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

#include <boost/algorithm/string.hpp>
#include <future>
#include <string>

namespace starrocks {
class Utils {
public:
    static std::string format_name(const std::string& col_name, bool case_sensitive) {
        return case_sensitive ? col_name : boost::algorithm::to_lower_copy(col_name);
    }
};

template <typename T>
std::future<T> make_ready_future(T&& t) {
    std::promise<T> p;
    p.set_value(std::forward<T>(t));
    return p.get_future();
}

template <typename T>
std::future<T> make_ready_future(const T& t) {
    T copy = t;
    std::promise<T> p;
    p.set_value(std::forward<T>(copy));
    return p.get_future();
}

namespace formats {

inline static const std::string PARQUET = "parquet";
inline static const std::string ORC = "orc";
inline static const std::string CSV = "csv";

} // namespace formats
} // namespace starrocks
