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

// NOTE:
// Implementation detail for config field definitions.
// Only include this header from .cpp files (before including config.h)
// when the translation unit must instantiate the CONF_* entries.

// clang-format off
#include "common/configbase.h"
// clang-format on

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <optional>

#include "gutil/strings/ascii_ctype.h"
#include "gutil/strings/join.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"

namespace starrocks::config {

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

    Field(const Field&) = delete;
    void operator=(const Field&) = delete;
    Field(Field&&) = delete;
    void operator=(Field&&) = delete;

    const char* type() const { return _type; }
    const char* name() const { return _name; }
    const char* defval() const { return _defval; }
    bool valmutable() const { return _valmutable; }

    // allow_fallback lets a field whose declaration opted into it replace a value it cannot accept
    // with the value it was declared with, instead of failing. Only the config file is parsed that
    // way; a value set at runtime is rejected so the caller sees the error.
    bool set_value(std::string value, bool allow_fallback);
    bool rollback();
    virtual std::string value() const = 0;

    static void clear_fields() { _s_field_map.clear(); }
    static std::map<std::string, Field*>& fields() { return _s_field_map; }
    static std::optional<Field*> get(const std::string& name_or_alias);

protected:
    inline static std::map<std::string, Field*> _s_field_map{};

    virtual bool parse_value(const std::string& value, bool allow_fallback) = 0;

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

    bool parse_value(const std::string& valstr, bool /*allow_fallback*/) override {
        return strtox(valstr, *reinterpret_cast<T*>(_storage));
    }
};

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

    bool parse_value(const std::string& valstr, bool /*allow_fallback*/) override {
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
              std::string enums_, bool fallback_to_default)
            : FieldImpl<T>(type, name, storage, defval, valmutable), _fallback_to_default(fallback_to_default) {
        std::vector<std::string> parts = strings::Split(enums_, ",");
        for (auto& part : parts) {
            StripWhiteSpace(&part);
            // An empty part declares the empty string as an accepted value, e.g. ",enabled".
            auto [it, inserted] = _enums.emplace(normalize(part), part);
            // A value repeated verbatim is harmless, but two values that only differ in case cannot
            // both be matched, so the declaration is rejected outright.
            if (!inserted && it->second != part) {
                std::cerr << fmt::format("Config '{}' declares enum values '{}' and '{}' that differ only in case\n",
                                         name, it->second, part);
                std::abort();
            }
        }
    }

    // Values are matched case-insensitively, but what gets written to the config variable is always
    // the spelling declared in the CONF_*_enum macro, so consumers can keep comparing it exactly.
    bool parse_value(const std::string& valstr, bool allow_fallback) override {
        if (auto it = _enums.find(normalize(valstr)); it != _enums.end()) {
            return Base::parse_value(it->second, allow_fallback);
        }
        // Reject before assigning, so a rejected value never lands in the config variable.
        if (!allow_fallback || !_fallback_to_default) {
            return false;
        }
        auto def = _enums.find(normalize(Field::_defval));
        if (def == _enums.end()) {
            // The declared default is not one of the declared values; there is nothing to fall back
            // on, so report it the same way an undeclarable default is reported.
            return false;
        }
        record_config_fallback({Field::_name, valstr, def->second, allowed_values()});
        return Base::parse_value(def->second, allow_fallback);
    }

private:
    std::string allowed_values() const {
        std::vector<std::string> declared;
        declared.reserve(_enums.size());
        for (const auto& entry : _enums) {
            declared.emplace_back(entry.second);
        }
        return JoinStrings(declared, ",");
    }

    static std::string normalize(const std::string& value) {
        std::string normalized = value;
        for (auto& c : normalized) {
            c = ascii_tolower(c);
        }
        return normalized;
    }

    // Normalized spelling -> the spelling declared in the CONF_*_enum macro.
    std::map<std::string, std::string> _enums;
    // Whether a value from the config file that matches nothing falls back to the declared default.
    bool _fallback_to_default;
};

#define DEFINE_FIELD(FIELD_TYPE, FIELD_NAME, FIELD_DEFAULT, VALMUTABLE, TYPE_NAME) \
    FIELD_TYPE FIELD_NAME;                                                         \
    static FieldImpl<FIELD_TYPE> field_##FIELD_NAME(TYPE_NAME, #FIELD_NAME, &FIELD_NAME, FIELD_DEFAULT, VALMUTABLE);

#define DEFINE_ENUM_FIELD(FIELD_TYPE, FIELD_NAME, FIELD_DEFAULT, VALMUTABLE, TYPE_NAME, ENUM_SET, FALLBACK) \
    FIELD_TYPE FIELD_NAME;                                                                                  \
    static EnumField<FIELD_TYPE> field_##FIELD_NAME(TYPE_NAME, #FIELD_NAME, &FIELD_NAME, FIELD_DEFAULT,     \
                                                    VALMUTABLE, ENUM_SET, FALLBACK);

#define DEFINE_ALIAS(REAL_NAME, ALIAS_NAME) static Alias alias_##ALIAS_NAME(#ALIAS_NAME, &(field_##REAL_NAME));

#undef CONF_Alias
#undef CONF_Bool
#undef CONF_Int16
#undef CONF_Int32
#undef CONF_Int64
#undef CONF_Double
#undef CONF_String
#undef CONF_String_enum
#undef CONF_Bools
#undef CONF_Int16s
#undef CONF_Int32s
#undef CONF_Int64s
#undef CONF_Doubles
#undef CONF_Strings
#undef CONF_mBool
#undef CONF_mInt16
#undef CONF_mInt32
#undef CONF_mInt64
#undef CONF_mDouble
#undef CONF_mString
#undef CONF_mString_enum
#undef CONF_mString_enum_or_default

// NOTE: alias configs must be defined after the true config, otherwise there will be a compile error
#define CONF_Alias(name, alias) DEFINE_ALIAS(name, alias)
#define CONF_Bool(name, defaultstr) DEFINE_FIELD(bool, name, defaultstr, false, "bool")
#define CONF_Int16(name, defaultstr) DEFINE_FIELD(int16_t, name, defaultstr, false, "int16")
#define CONF_Int32(name, defaultstr) DEFINE_FIELD(int32_t, name, defaultstr, false, "int32")
#define CONF_Int64(name, defaultstr) DEFINE_FIELD(int64_t, name, defaultstr, false, "int64")
#define CONF_Double(name, defaultstr) DEFINE_FIELD(double, name, defaultstr, false, "double")
#define CONF_String(name, defaultstr) DEFINE_FIELD(std::string, name, defaultstr, false, "string")
#define CONF_String_enum(name, defaultstr, enums) \
    DEFINE_ENUM_FIELD(std::string, name, defaultstr, false, "string", enums, false)
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
#define CONF_mString_enum(name, defaultstr, enums) \
    DEFINE_ENUM_FIELD(MutableString, name, defaultstr, true, "string", enums, false)
#define CONF_mString_enum_or_default(name, defaultstr, enums) \
    DEFINE_ENUM_FIELD(MutableString, name, defaultstr, true, "string", enums, true)

} // namespace starrocks::config
