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
#include <map>
#include <mutex>
#include <string>
#include <vector>

#ifdef __IN_CONFIGBASE_CPP__
#include <fmt/format.h>

#include "gutil/strings/join.h"
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

class Field {
public:
    Field(const char* type, const char* name, void* storage, const char* defval, bool valmutable)
            : _type(type), _name(name), _storage(storage), _defval(defval), _valmutable(valmutable) {}

    virtual ~Field() = default;

    const char* type() const { return _type; }

    const char* name() const { return _name; }

    const char* defval() const { return _defval; }

    bool valmutable() const { return _valmutable; }

    virtual std::string value() const;

    virtual bool parse_value(const std::string& value);

protected:
    const char* _type;
    const char* _name;
    void* _storage;
    const char* _defval;
    bool _valmutable;
};

template <typename T, typename = void>
class FieldImpl;

//// FieldImpl<bool/int16_t/int32_t/int64_t/double>
template <typename T>
class FieldImpl<T, typename std::enable_if_t<std::is_arithmetic<T>::value>> final : public Field {
public:
    FieldImpl(const char* type, const char* name, void* storage, const char* defval, bool valmutable)
            : Field(type, name, storage, defval, valmutable) {}

    std::string value() const override { return fmt::format("{}", *reinterpret_cast<T*>(_storage)); }

    bool parse_value(const std::string& valstr) override { return strtox(valstr, *reinterpret_cast<T*>(_storage)); }
};

//// FieldImpl<std::string/MutableString>
template <typename T>
class FieldImpl<T, typename std::enable_if_t<std::is_same_v<T, std::string> || std::is_same_v<T, MutableString>>> final
        : public Field {
public:
    FieldImpl(const char* type, const char* name, void* storage, const char* defval, bool valmutable)
            : Field(type, name, storage, defval, valmutable) {}

    std::string value() const override { return *reinterpret_cast<T*>(_storage); }

    bool parse_value(const std::string& valstr) override {
        *reinterpret_cast<T*>(_storage) = valstr;
        return true;
    }
};

//// FieldImpl<std::vector<T>>
template <typename T>
class FieldImpl<std::vector<T>> final : public Field {
public:
    FieldImpl(const char* type, const char* name, void* storage, const char* defval, bool valmutable)
            : Field(type, name, storage, defval, valmutable) {}

    std::string value() const override {
        auto as_str = [](const T& v) { return fmt::format("{}", v); };
        const auto& v = *reinterpret_cast<const std::vector<T>*>(_storage);
        return JoinMapped(v, as_str, ",");
    }

    bool parse_value(const std::string& valstr) override { return false; }
};

class Register {
public:
    inline static std::map<std::string, Field*> _s_field_map{};

    explicit Register(Field* field) { _s_field_map.insert(std::make_pair(std::string(field->name()), field)); }
};

// Configuration properties load from config file.
class Properties {
public:
    bool load(const char* filename);
    bool get(const std::string& key, const std::string& defstr, Field* field) const;

private:
    std::map<std::string, std::string> file_conf_map;
};

#endif // __IN_CONFIGBASE_CPP__

#define DEFINE_FIELD(FIELD_TYPE, FIELD_NAME, FIELD_DEFAULT, VALMUTABLE, TYPE_NAME)                          \
    FIELD_TYPE FIELD_NAME;                                                                                  \
    static FieldImpl<FIELD_TYPE> reg_field_##FIELD_NAME(TYPE_NAME, #FIELD_NAME, &FIELD_NAME, FIELD_DEFAULT, \
                                                        VALMUTABLE);                                        \
    static Register reg_##FIELD_NAME(&reg_field_##FIELD_NAME);

#define DECLARE_FIELD(FIELD_TYPE, FIELD_NAME) extern FIELD_TYPE FIELD_NAME;

#ifdef __IN_CONFIGBASE_CPP__
#define CONF_Bool(name, defaultstr) DEFINE_FIELD(bool, name, defaultstr, false, "bool")
#define CONF_Int16(name, defaultstr) DEFINE_FIELD(int16_t, name, defaultstr, false, "int16")
#define CONF_Int32(name, defaultstr) DEFINE_FIELD(int32_t, name, defaultstr, false, "int32")
#define CONF_Int64(name, defaultstr) DEFINE_FIELD(int64_t, name, defaultstr, false, "int64")
#define CONF_Double(name, defaultstr) DEFINE_FIELD(double, name, defaultstr, false, "double")
#define CONF_String(name, defaultstr) DEFINE_FIELD(std::string, name, defaultstr, false, "string")
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
#define CONF_Bool(name, defaultstr) DECLARE_FIELD(bool, name)
#define CONF_Int16(name, defaultstr) DECLARE_FIELD(int16_t, name)
#define CONF_Int32(name, defaultstr) DECLARE_FIELD(int32_t, name)
#define CONF_Int64(name, defaultstr) DECLARE_FIELD(int64_t, name)
#define CONF_Double(name, defaultstr) DECLARE_FIELD(double, name)
#define CONF_String(name, defaultstr) DECLARE_FIELD(std::string, name)
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

bool init(const char* filename);

Status set_config(const std::string& field, const std::string& value);

std::vector<ConfigInfo> list_configs();

} // namespace config
} // namespace starrocks
