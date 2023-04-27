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
#include <mutex>
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

public:
    Register(const char* ftype, const char* fname, void* fstorage, const char* fdefval, bool fvalmutable) {
        if (_s_field_map == nullptr) {
            _s_field_map = new std::map<std::string, Field>();
        }
        Field field(ftype, fname, fstorage, fdefval, fvalmutable);
        _s_field_map->insert(std::make_pair(std::string(fname), field));
    }
};

#define DEFINE_FIELD(FIELD_TYPE, FIELD_NAME, FIELD_DEFAULT, VALMUTABLE) \
    FIELD_TYPE FIELD_NAME;                                              \
    static Register reg_##FIELD_NAME(#FIELD_TYPE, #FIELD_NAME, &FIELD_NAME, FIELD_DEFAULT, VALMUTABLE);

#define DECLARE_FIELD(FIELD_TYPE, FIELD_NAME) extern FIELD_TYPE FIELD_NAME;

#ifdef __IN_CONFIGBASE_CPP__
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
