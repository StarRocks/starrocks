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
#include <regex>
#include <set>
#include <string>

#define __IN_CONFIGBASE_CPP__
#include "common/config.h"
#undef __IN_CONFIGBASE_CPP__

#include <fmt/format.h>

#include "common/status.h"

namespace starrocks::config {

// Replace env variables.
Status replaceenv(std::string& s) {
    std::size_t pos = 0;
    std::size_t start = 0;
    while ((start = s.find("${", pos)) != std::string::npos) {
        std::size_t end = s.find('}', start + 2);
        if (end == std::string::npos) {
            return Status::InvalidArgument(s);
        }
        std::string envkey = s.substr(start + 2, end - start - 2);
        const char* envval = std::getenv(envkey.c_str());
        if (envval == nullptr) {
            return Status::InvalidArgument(fmt::format("Non-existent environment variable: {}", envkey));
        }
        s.erase(start, end - start + 1);
        s.insert(start, envval);
        pos = start + strlen(envval);
    }
    return Status::OK();
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

inline bool parse_key_value_pairs(std::istream& input) {
    std::string line;
    std::string key;
    std::string value;
    std::regex doris_start("^doris_");
    line.reserve(512);
    std::set<Field*> assigned_fields;
    while (input) {
        // Read one line at a time.
        std::getline(input, line);

        // Remove left and right spaces.
        StripWhiteSpace(&line);

        // Ignore comments.
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // Read key and value.
        std::pair<std::string, std::string> kv = strings::Split(line, strings::delimiter::Limit("=", 1));
        StripWhiteSpace(&kv.first);

        // compatible with doris_config
        kv.first = std::regex_replace(kv.first, doris_start, "");

        auto op_field = Field::get(kv.first);
        if (!op_field.has_value()) {
            // A valid env var name should be: [A-Z_][A-Z0-9_]*
            static const std::string ENV_CHARSET("ABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890_");
            if (kv.first.find_first_not_of(ENV_CHARSET) != std::string::npos) {
                // only report error when the var name appears not to be valid, not strict here.
                std::cerr << fmt::format("Ignored unknown config: {}\n", kv.first);
            }
            continue;
        }
        auto field = op_field.value();
        if (assigned_fields.count(field) > 0) {
            std::cerr << fmt::format("Duplicate assignment to config '{}', previous assignment will be ignored\n",
                                     field->name());
        }
        assigned_fields.insert(field);
        if (bool r = field->set_value(kv.second); !r) {
            std::cerr << fmt::format("Invalid value of config '{}': '{}'\n", kv.first, kv.second);
            return false;
        }
    }
    return true;
}

std::optional<Field*> Field::get(const std::string& name_or_alias) {
    auto ret = std::optional<Field*>();
    auto it = fields().find(name_or_alias);
    if (it != fields().end()) {
        ret.emplace(it->second);
    }
    return ret;
}

bool Field::set_value(std::string value) {
    if (auto st = replaceenv(value); !st.ok()) {
        return false;
    }
    StripWhiteSpace(&value);
    bool success = parse_value(value);
    if (success) {
        _last_set_val.swap(_current_set_val);
        _current_set_val = value;
    }
    return success;
}

bool Field::rollback() {
    bool success = parse_value(_last_set_val);
    if (success) {
        _current_set_val.swap(_last_set_val);
        _last_set_val.clear();
    }
    return success;
}

// Init conf fields.
bool init(const char* filename) {
    std::ifstream input;
    if (filename != nullptr) {
        input.open(filename);
        if (input.fail()) {
            std::cerr << "Fail to open " << filename << std::endl;
            return false;
        }
    }
    return init(input);
}

inline bool init_from_default_values() {
    for (const auto& [name, field] : Field::fields()) {
        if (!field->set_value(field->defval())) {
            std::cerr << fmt::format("Invalid default value of config '{}': '{}'\n", name, field->defval());
            return false;
        }
    }
    return true;
}

bool init(std::istream& input) {
    if (!init_from_default_values()) {
        return false;
    }

    return parse_key_value_pairs(input);
}

Status set_config(const std::string& field, const std::string& value) {
    auto it = Field::fields().find(field);
    if (it == Field::fields().end()) {
        return Status::NotFound(fmt::format("'{}' is not found", field));
    }
    if (!it->second->valmutable()) {
        return Status::NotSupported(fmt::format("'{}' is immutable", field));
    }
    if (!it->second->set_value(value)) {
        return Status::InvalidArgument(fmt::format("Invalid value of config '{}': '{}'", field, value));
    }
    return Status::OK();
}

Status rollback_config(const std::string& field) {
    auto it = Field::fields().find(field);
    if (it == Field::fields().end()) {
        return Status::NotFound(fmt::format("'{}' is not found in rollback", field));
    }

    if (!it->second->rollback()) {
        return Status::InvalidArgument(fmt::format("Invalid value of config '{}' in rollback", field));
    }
    return Status::OK();
}

std::vector<ConfigInfo> list_configs() {
    std::vector<ConfigInfo> infos;
    for (const auto& [name, field] : Field::fields()) {
        auto& info = infos.emplace_back();
        info.name = field->name();
        info.value = field->value();
        info.type = field->type();
        info.defval = field->defval();
        info.valmutable = field->valmutable();
    }
    return infos;
}

void TEST_clear_configs() {
    Field::clear_fields();
}

} // namespace starrocks::config
