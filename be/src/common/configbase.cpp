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

#include <strings.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <regex>
#include <string>

#define __IN_CONFIGBASE_CPP__
#include "common/config.h"
#undef __IN_CONFIGBASE_CPP__

#include <fmt/format.h>

#include "common/status.h"

namespace starrocks::config {

Properties props;

// trim string
std::string& trim(std::string& s) {
    // rtrim
    s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
    // ltrim
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
    return s;
}

// Split string by '='.
void splitkv(const std::string& s, std::string& k, std::string& v) {
    const char sep = '=';
    size_t start = 0;
    size_t end = 0;
    if ((end = s.find(sep, start)) != std::string::npos) {
        k = s.substr(start, end - start);
        v = s.substr(end + 1);
    } else {
        k = s;
        v = "";
    }
}

// Replace env variables.
bool replaceenv(std::string& s) {
    std::size_t pos = 0;
    std::size_t start = 0;
    while ((start = s.find("${", pos)) != std::string::npos) {
        std::size_t end = s.find('}', start + 2);
        if (end == std::string::npos) {
            return false;
        }
        std::string envkey = s.substr(start + 2, end - start - 2);
        const char* envval = std::getenv(envkey.c_str());
        if (envval == nullptr) {
            return false;
        }
        s.erase(start, end - start + 1);
        s.insert(start, envval);
        pos = start + strlen(envval);
    }
    return true;
}

template <typename T>
bool strtox(const std::string& valstr, std::vector<T>& retval) {
    std::stringstream ss(valstr);
    std::string item;
    T t;
    while (std::getline(ss, item, ',')) {
        if (!strtox(trim(item), t)) {
            return false;
        }
        retval.push_back(t);
    }
    return true;
}

bool strtox(const std::string& valstr, bool& retval) {
    if (strcasecmp(valstr.c_str(), "true") == 0 || strcmp(valstr.c_str(), "1") == 0) {
        retval = true;
    } else if (strcasecmp(valstr.c_str(), "false") == 0 || strcmp(valstr.c_str(), "0") == 0) {
        retval = false;
    } else {
        return false;
    }
    return true;
}

template <typename T>
bool strtointeger(const std::string& valstr, T& retval) {
    if (valstr.length() == 0) {
        return false; // empty-string is only allowed for string type.
    }
    char* end;
    errno = 0;
    const char* valcstr = valstr.c_str();
    int64_t ret64 = strtoll(valcstr, &end, 10);
    if (errno || end != valcstr + strlen(valcstr)) {
        return false; // bad parse
    }
    T tmp = retval;
    retval = static_cast<T>(ret64);
    if (retval != ret64) {
        retval = tmp;
        return false;
    }
    return true;
}

bool strtox(const std::string& valstr, int16_t& retval) {
    return strtointeger(valstr, retval);
}

bool strtox(const std::string& valstr, int32_t& retval) {
    return strtointeger(valstr, retval);
}

bool strtox(const std::string& valstr, int64_t& retval) {
    return strtointeger(valstr, retval);
}

bool strtox(const std::string& valstr, double& retval) {
    if (valstr.length() == 0) {
        return false; // empty-string is only allowed for string type.
    }
    char* end = nullptr;
    errno = 0;
    const char* valcstr = valstr.c_str();
    retval = strtod(valcstr, &end);
    if (errno || end != valcstr + strlen(valcstr)) {
        return false; // bad parse
    }
    return true;
}

bool strtox(const std::string& valstr, std::string& retval) {
    retval = valstr;
    return true;
}

bool strtox(const std::string& valstr, MutableString& retval) {
    retval = valstr;
    return true;
}

// Load conf file.
bool Properties::load(const char* filename) {
    // If 'filename' is null, use the empty props.
    if (filename == nullptr) {
        return true;
    }

    // Open the conf file
    std::ifstream input(filename);
    if (!input.is_open()) {
        std::cerr << "config::load() failed to open the file:" << filename << std::endl;
        return false;
    }

    // load properties
    std::string line;
    std::string key;
    std::string value;
    std::regex doris_start("^doris_");
    line.reserve(512);
    while (input) {
        // Read one line at a time.
        std::getline(input, line);

        // Remove left and right spaces.
        trim(line);

        // Ignore comments.
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // Read key and value.
        splitkv(line, key, value);
        trim(key);
        trim(value);

        // compatible with doris_config
        key = std::regex_replace(key, doris_start, "");

        if (key.compare("webserver_port") == 0) {
            // Avoid overwriting the existing config.
            if (file_conf_map.find("be_http_port") != file_conf_map.end()) {
                continue;
            }

            key = "be_http_port";
        }

        // Insert into 'file_conf_map'.
        file_conf_map[key] = value;
    }

    // Close the conf file.
    input.close();

    return true;
}

bool Properties::get(const std::string& key, const std::string& defstr, Field* field) const {
    const auto& it = file_conf_map.find(key);
    std::string valstr = (it != file_conf_map.end()) ? it->second : defstr;
    trim(valstr);
    if (!replaceenv(valstr)) {
        return false;
    }
    return field->parse_value(valstr);
}

template <typename T>
bool update(const std::string& value, T& retval) {
    std::string valstr(value);
    trim(valstr);
    if (!replaceenv(valstr)) {
        return false;
    }
    return strtox(valstr, retval);
}

template <typename T>
std::ostream& operator<<(std::ostream& out, const std::vector<T>& v) {
    size_t last = v.size() - 1;
    for (size_t i = 0; i < v.size(); ++i) {
        out << v[i];
        if (i != last) {
            out << ", ";
        }
    }
    return out;
}

// Init conf fields.
bool init(const char* filename) {
    // Load properties file.
    if (!props.load(filename)) {
        return false;
    }

    // Set conf fields.
    for (const auto& [name, field] : Register::_s_field_map) {
        if (!props.get(name, field->defval(), field)) {
            std::cerr << "config field error: " << name << '\n';
            return false;
        }
    }

    return true;
}

Status set_config(const std::string& field, const std::string& value) {
    auto it = Register::_s_field_map.find(field);
    if (it == Register::_s_field_map.end()) {
        return Status::NotFound(fmt::format("'{}' is not found", field));
    }
    if (!it->second->valmutable()) {
        return Status::NotSupported(fmt::format("'{}' is immutable", field));
    }
    if (!it->second->parse_value(value)) {
        return Status::InvalidArgument(fmt::format("{}: Invalid config value: '{}'", field, value));
    }
    return Status::OK();
}

std::vector<ConfigInfo> list_configs() {
    std::vector<ConfigInfo> infos;
    for (const auto& [name, field] : Register::_s_field_map) {
        auto& info = infos.emplace_back();
        info.name = field->name();
        info.value = field->value();
        info.type = field->type();
        info.defval = field->defval();
        info.valmutable = field->valmutable();
    }
    return infos;
}

} // namespace starrocks::config
