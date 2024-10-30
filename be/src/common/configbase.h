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
#include <fmt/format.h>

#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

#ifdef __IN_CONFIGBASE_CPP__

#include <cassert>
#include <optional>

#include "gutil/strings/join.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#endif

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

#ifdef __IN_CONFIGBASE_CPP__
bool strtox(const std::string& valstr, bool& retval);
bool strtox(const std::string& valstr, int16_t& retval);
bool strtox(const std::string& valstr, int32_t& retval);
bool strtox(const std::string& valstr, int64_t& retval);
bool strtox(const std::string& valstr, double& retval);
bool strtox(const std::string& valstr, std::string& retval);
bool strtox(const std::string& valstr, MutableString& retval);

class Field {
public:
    explicit Field(const char* type, const char* name, void* storage, const char* defval, bool valmutable)
            : _type(type), _name(name), _storage(storage), _defval(defval), _valmutable(valmutable) {
        _s_field_map.insert(std::make_pair(std::string(_name), this));
    }

    virtual ~Field() = default;

    // Disallow copy
    Field(const Field&) = delete;
    // Disallow assign
    void operator=(const Field&) = delete;
    // Disallow move ctor
    Field(Field&&) = delete;
    // Disallow move assign
    void operator=(Field&&) = delete;

    const char* type() const { return _type; }

    const char* name() const { return _name; }

    const char* defval() const { return _defval; }

    bool valmutable() const { return _valmutable; }

    bool set_value(std::string value);

    bool rollback();

    virtual std::string value() const = 0;

    static void clear_fields() { _s_field_map.clear(); }

    static std::map<std::string, Field*>& fields() { return _s_field_map; }

    static std::optional<Field*> get(const std::string& name_or_alias);

protected:
    inline static std::map<std::string, Field*> _s_field_map{};

    virtual bool parse_value(const std::string& value) = 0;

    const char* _type;
    const char* _name;
    void* _storage;
    const char* _defval;
    bool _valmutable;
    std::string _last_set_val;
    std::string _current_set_val;
};

template <typename T, typename = void>
class FieldImpl;

template <typename T>
class FieldImpl<T> : public Field {
public:
    FieldImpl(const char* type, const char* name, void* storage, const char* defval, bool valmutable)
            : Field(type, name, storage, defval, valmutable) {}

    std::string value() const override { return fmt::format("{}", *reinterpret_cast<T*>(_storage)); }

    bool parse_value(const std::string& valstr) override { return strtox(valstr, *reinterpret_cast<T*>(_storage)); }
};

//// FieldImpl<std::vector<T>>
template <typename T>
class FieldImpl<std::vector<T>> : public Field {
public:
    FieldImpl(const char* type, const char* name, void* storage, const char* defval, bool valmutable)
            : Field(type, name, storage, defval, valmutable) {}

    std::string value() const override {
        auto as_str = [](const T& v) { return fmt::format("{}", v); };
        const auto& v = *reinterpret_cast<const std::vector<T>*>(_storage);
        return JoinMapped(v, as_str, ",");
    }

    bool parse_value(const std::string& valstr) override {
        std::vector<T> tmp;
        std::vector<std::string> parts = strings::Split(valstr, ",");
        for (auto& part : parts) {
            T v;
            StripWhiteSpace(&part);
            if (part.empty()) {
                continue;
            }
            if (!strtox(part, v)) {
                return false;
            }
            tmp.emplace_back(std::move(v));
        }
        auto& value = *reinterpret_cast<std::vector<T>*>(_storage);
        value.swap(tmp);
        return true;
    }
};

class Alias {
public:
    explicit Alias(const char* alias, Field* field) {
        assert(strcmp(field->name(), alias) != 0);
        [[maybe_unused]] auto [_, ok] = Field::fields().emplace(std::string(alias), field);
        if (!ok) {
            std::cerr << fmt::format("The alias name '{}' for config '{}' already used, please choose another one\n",
                                     alias, field->name());
            std::abort();
        }
    }
};

template <typename T>
class EnumField : public FieldImpl<T> {
    using Base = FieldImpl<T>;

public:
    EnumField(const char* type, const char* name, void* storage, const char* defval, bool valmutable,
              std::string enums_)
            : FieldImpl<T>(type, name, storage, defval, valmutable), raw_enum_values(std::move(enums_)) {}

    bool parse_value(const std::string& valstr) override {
        if (enums.empty()) {
            std::vector<std::string> parts = strings::Split(raw_enum_values, ",");
            for (auto& part : parts) {
                StripWhiteSpace(&part);
                if (!Base::parse_value(part)) {
                    return false;
                }
                auto v = *reinterpret_cast<T*>(Field::_storage);
                enums.emplace(std::move(v));
            }
        }
        if (!Base::parse_value(valstr)) {
            return false;
        }
        auto value = *reinterpret_cast<T*>(Field::_storage);
        return enums.find(value) != enums.end();
    }

private:
    std::set<T> enums;
    std::string raw_enum_values;
};

#endif // __IN_CONFIGBASE_CPP__

#define DEFINE_FIELD(FIELD_TYPE, FIELD_NAME, FIELD_DEFAULT, VALMUTABLE, TYPE_NAME) \
    FIELD_TYPE FIELD_NAME;                                                         \
    static FieldImpl<FIELD_TYPE> field_##FIELD_NAME(TYPE_NAME, #FIELD_NAME, &FIELD_NAME, FIELD_DEFAULT, VALMUTABLE);

#define DEFINE_ALIAS(REAL_NAME, ALIAS_NAME) static Alias alias_##ALIAS_NAME(#ALIAS_NAME, &(field_##REAL_NAME));

#define DECLARE_FIELD(FIELD_TYPE, FIELD_NAME) extern FIELD_TYPE FIELD_NAME;

#define DEFINE_ENUM_FIELD(FIELD_TYPE, FIELD_NAME, FIELD_DEFAULT, VALMUTABLE, TYPE_NAME, ENUM_SET)                   \
    FIELD_TYPE FIELD_NAME;                                                                                          \
    static EnumField<FIELD_TYPE> field_##FIELD_NAME(TYPE_NAME, #FIELD_NAME, &FIELD_NAME, FIELD_DEFAULT, VALMUTABLE, \
                                                    ENUM_SET);

#ifdef __IN_CONFIGBASE_CPP__
// NOTE: alias configs must be defined after the true config, otherwise there will be a compile error
#define CONF_Alias(name, alias) DEFINE_ALIAS(name, alias)
#define CONF_Bool(name, defaultstr) DEFINE_FIELD(bool, name, defaultstr, false, "bool")
#define CONF_Int16(name, defaultstr) DEFINE_FIELD(int16_t, name, defaultstr, false, "int16")
#define CONF_Int32(name, defaultstr) DEFINE_FIELD(int32_t, name, defaultstr, false, "int32")
#define CONF_Int64(name, defaultstr) DEFINE_FIELD(int64_t, name, defaultstr, false, "int64")
#define CONF_Double(name, defaultstr) DEFINE_FIELD(double, name, defaultstr, false, "double")
#define CONF_String(name, defaultstr) DEFINE_FIELD(std::string, name, defaultstr, false, "string")
#define CONF_String_enum(name, defaultstr, enums) \
    DEFINE_ENUM_FIELD(std::string, name, defaultstr, false, "string", enums)
#define CONF_Bools(name, defaultstr) DEFINE_FIELD(std::vector<bool>, name, defaultstr, false, "list<bool>")
#define CONF_Int16s(name, defaultstr) DEFINE_FIELD(std::vector<int16_t>, name, defaultstr, false, "list<int16>")
#define CONF_Int32s(name, defaultstr) DEFINE_FIELD(std::vector<int32_t>, name, defaultstr, false, "list<int32>")
#define CONF_Int64s(name, defaultstr) DEFINE_FIELD(std::vector<int64_t>, name, defaultstr, false, "list<int64>")
#define CONF_Doubles(name, defaultstr) DEFINE_FIELD(std::vector<double>, name, defaultstr, false, "list<double>")
#define CONF_Strings(name, defaultstr) DEFINE_FIELD(std::vector<std::string>, name, defaultstr, false, "list<string>")
#define CONF_mBool(name, defaultstr) DEFINE_FIELD(bool, name, defaultstr, true, "bool")
#define CONF_mInt16(name, defaultstr) DEFINE_FIELD(int16_t, name, defaultstr, true, "int16")
#define CONF_mInt32(name, defaultstr) DEFINE_FIELD(int32_t, name, defaultstr, true, "int32")
#define CONF_mInt64(name, defaultstr) DEFINE_FIELD(int64_t, name, defaultstr, true, "int64")
#define CONF_mDouble(name, defaultstr) DEFINE_FIELD(double, name, defaultstr, true, "double")
#define CONF_mString(name, defaultstr) DEFINE_FIELD(MutableString, name, defaultstr, true, "string")
#else
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
#endif

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
    auto format(const starrocks::config::MutableString& s, format_context& ctx) {
        return formatter<std::string>::format(s.value(), ctx);
    }
};
