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
#include <cstdint>
#include <map>
<<<<<<< HEAD
#include <mutex>
=======
#include <set>
>>>>>>> 75f45f8f5f ([Feature] add be config brpc_connection_type (#42824))
#include <string>
#include <vector>

namespace starrocks {
class Status;

namespace config {

struct ConfigInfo {
    std::string name;
    std::string value;
    std::string type;
    std::string defval;
    bool valmutable;
};

class Register {
public:
    struct Field {
        const char* type = nullptr;
        const char* name = nullptr;
        void* storage = nullptr;
        const char* defval = nullptr;
        bool valmutable = false;
        Field(const char* ftype, const char* fname, void* fstorage, const char* fdefval, bool fvalmutable)
                : type(ftype), name(fname), storage(fstorage), defval(fdefval), valmutable(fvalmutable) {}

        // Get the field value as string
        std::string value() const;
    };

public:
    static std::map<std::string, Field>* _s_field_map;

<<<<<<< HEAD
public:
    Register(const char* ftype, const char* fname, void* fstorage, const char* fdefval, bool fvalmutable) {
        if (_s_field_map == nullptr) {
            _s_field_map = new std::map<std::string, Field>();
=======
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
>>>>>>> 75f45f8f5f ([Feature] add be config brpc_connection_type (#42824))
        }
        Field field(ftype, fname, fstorage, fdefval, fvalmutable);
        _s_field_map->insert(std::make_pair(std::string(fname), field));
    }
};

<<<<<<< HEAD
#define DEFINE_FIELD(FIELD_TYPE, FIELD_NAME, FIELD_DEFAULT, VALMUTABLE) \
    FIELD_TYPE FIELD_NAME;                                              \
    static Register reg_##FIELD_NAME(#FIELD_TYPE, #FIELD_NAME, &FIELD_NAME, FIELD_DEFAULT, VALMUTABLE);
=======
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
>>>>>>> 75f45f8f5f ([Feature] add be config brpc_connection_type (#42824))

#define DECLARE_FIELD(FIELD_TYPE, FIELD_NAME) extern FIELD_TYPE FIELD_NAME;

#define DEFINE_ENUM_FIELD(FIELD_TYPE, FIELD_NAME, FIELD_DEFAULT, VALMUTABLE, TYPE_NAME, ENUM_SET)                   \
    FIELD_TYPE FIELD_NAME;                                                                                          \
    static EnumField<FIELD_TYPE> field_##FIELD_NAME(TYPE_NAME, #FIELD_NAME, &FIELD_NAME, FIELD_DEFAULT, VALMUTABLE, \
                                                    ENUM_SET);

#ifdef __IN_CONFIGBASE_CPP__
<<<<<<< HEAD
#define CONF_Bool(name, defaultstr) DEFINE_FIELD(bool, name, defaultstr, false)
#define CONF_Int16(name, defaultstr) DEFINE_FIELD(int16_t, name, defaultstr, false)
#define CONF_Int32(name, defaultstr) DEFINE_FIELD(int32_t, name, defaultstr, false)
#define CONF_Int64(name, defaultstr) DEFINE_FIELD(int64_t, name, defaultstr, false)
#define CONF_Double(name, defaultstr) DEFINE_FIELD(double, name, defaultstr, false)
#define CONF_String(name, defaultstr) DEFINE_FIELD(std::string, name, defaultstr, false)
#define CONF_Bools(name, defaultstr) DEFINE_FIELD(std::vector<bool>, name, defaultstr, false)
#define CONF_Int16s(name, defaultstr) DEFINE_FIELD(std::vector<int16_t>, name, defaultstr, false)
#define CONF_Int32s(name, defaultstr) DEFINE_FIELD(std::vector<int32_t>, name, defaultstr, false)
#define CONF_Int64s(name, defaultstr) DEFINE_FIELD(std::vector<int64_t>, name, defaultstr, false)
#define CONF_Doubles(name, defaultstr) DEFINE_FIELD(std::vector<double>, name, defaultstr, false)
#define CONF_Strings(name, defaultstr) DEFINE_FIELD(std::vector<std::string>, name, defaultstr, false)
#define CONF_mBool(name, defaultstr) DEFINE_FIELD(bool, name, defaultstr, true)
#define CONF_mInt16(name, defaultstr) DEFINE_FIELD(int16_t, name, defaultstr, true)
#define CONF_mInt32(name, defaultstr) DEFINE_FIELD(int32_t, name, defaultstr, true)
#define CONF_mInt64(name, defaultstr) DEFINE_FIELD(int64_t, name, defaultstr, true)
#define CONF_mDouble(name, defaultstr) DEFINE_FIELD(double, name, defaultstr, true)
#define CONF_mString(name, defaultstr) DEFINE_FIELD(std::string, name, defaultstr, true)
=======
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
>>>>>>> 75f45f8f5f ([Feature] add be config brpc_connection_type (#42824))
#else
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
#define CONF_mString(name, defaultstr) DECLARE_FIELD(std::string, name)
#endif

// Configuration properties load from config file.
class Properties {
public:
    bool load(const char* filename);
    template <typename T>
    bool get(const char* key, const char* defstr, T& retval) const;

private:
    std::map<std::string, std::string> file_conf_map;
};

extern Properties props;

// Full configurations.
extern std::map<std::string, std::string>* full_conf_map;

bool init(const char* filename, bool fillconfmap = false);

Status set_config(const std::string& field, const std::string& value);

std::mutex* get_mstring_conf_lock();

std::vector<ConfigInfo> list_configs();

} // namespace config
} // namespace starrocks
