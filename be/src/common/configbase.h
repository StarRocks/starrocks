// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <butil/containers/doubly_buffered_data.h>

#include <cstdint>
#include <iosfwd>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"

namespace starrocks {
class Status;

namespace config {

struct ConfigInfo {
    std::string name;
    std::string value;
    std::string type;
    std::string defval;
    bool valmutable;

    bool operator<(const ConfigInfo& rhs) const { return name < rhs.name; }

    bool operator==(const ConfigInfo& rhs) const = default;
};

inline std::ostream& operator<<(std::ostream& os, const ConfigInfo& info) {
    os << "ConfigInfo{"
       << "name=\"" << info.name << "\","
       << "value=\"" << info.value << "\","
       << "type=" << info.type << ","
       << "default=\"" << info.defval << "\","
       << "mutable=" << info.valmutable << "}";
    return os;
}

// A wrapper on std::string, it's safe to read/write MutableString concurrently.
class MutableString {
public:
    MutableString() = default;
    ~MutableString() = default;

    // Disallow copy and move, because no usage now.
    MutableString(const MutableString&) = delete;
    void operator=(const MutableString&) = delete;
    MutableString(MutableString&&) = delete;
    void operator=(MutableString&&) = delete;

    std::string value() const;

    operator std::string() const { return value(); }

    MutableString& operator=(std::string s);

private:
    static bool update_value(std::string& bg, std::string new_value) {
        bg = std::move(new_value);
        return true;
    }

    mutable butil::DoublyBufferedData<std::string> _str;
};

inline std::string MutableString::value() const {
    butil::DoublyBufferedData<std::string>::ScopedPtr ptr;
    _str.Read(&ptr);
    return *ptr;
}

inline MutableString& MutableString::operator=(std::string s) {
    _str.Modify(update_value, std::move(s));
    return *this;
}

inline std::ostream& operator<<(std::ostream& os, const MutableString& s) {
    return os << s.value();
}

#define DECLARE_FIELD(FIELD_TYPE, FIELD_NAME) extern FIELD_TYPE FIELD_NAME;

// NOTE: alias configs must be defined after the true config, otherwise there will be a compile error
#define CONF_Alias(name, alias)
#define CONF_Bool(name, defaultstr) DECLARE_FIELD(bool, name)
#define CONF_Int16(name, defaultstr) DECLARE_FIELD(int16_t, name)
#define CONF_Int32(name, defaultstr) DECLARE_FIELD(int32_t, name)
#define CONF_Int64(name, defaultstr) DECLARE_FIELD(int64_t, name)
#define CONF_Double(name, defaultstr) DECLARE_FIELD(double, name)
#define CONF_String(name, defaultstr) DECLARE_FIELD(std::string, name)
#define CONF_String_enum(name, defaultstr, enums) DECLARE_FIELD(std::string, name)
#define CONF_Bools(name, defaultstr) DECLARE_FIELD(std::vector<bool>, name)
#define CONF_Int16s(name, defaultstr) DECLARE_FIELD(std::vector<int16_t>, name)
#define CONF_Int32s(name, defaultstr) DECLARE_FIELD(std::vector<int32_t>, name)
#define CONF_Int64s(name, defaultstr) DECLARE_FIELD(std::vector<int64_t>, name)
#define CONF_Doubles(name, defaultstr) DECLARE_FIELD(std::vector<double>, name)
#define CONF_Strings(name, defaultstr) DECLARE_FIELD(std::vector<std::string>, name)
#define CONF_mBool(name, defaultstr) DECLARE_FIELD(bool, name)
#define CONF_mInt16(name, defaultstr) DECLARE_FIELD(int16_t, name)
#define CONF_mInt32(name, defaultstr) DECLARE_FIELD(int32_t, name)
#define CONF_mInt64(name, defaultstr) DECLARE_FIELD(int64_t, name)
#define CONF_mDouble(name, defaultstr) DECLARE_FIELD(double, name)
#define CONF_mString(name, defaultstr) DECLARE_FIELD(MutableString, name)

// Initialize configurations from a config file.
bool init(const char* filename);

// Initialize configurations from a input stream.
bool init(std::istream& input);

Status set_config(const std::string& field, const std::string& value);

Status rollback_config(const std::string& field);

std::vector<ConfigInfo> list_configs();

void TEST_clear_configs();

} // namespace config
} // namespace starrocks

template <>
struct fmt::formatter<starrocks::config::MutableString> : formatter<std::string> {
    auto format(const starrocks::config::MutableString& s, format_context& ctx) const {
        return formatter<std::string>::format(s.value(), ctx);
    }
};
