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

// Configuration properties load from config file.
class Properties {
public:
    bool load(std::istream& input);

    std::optional<std::string> get(const std::string& key) const;

private:
    std::map<std::string, std::string> _configs;
};

inline std::optional<std::string> Properties::get(const std::string& key) const {
    auto it = _configs.find(key);
    if (it == _configs.end()) {
        return {};
    } else {
        return {it->second};
    }
}

inline bool Properties::load(std::istream& input) {
    std::string line;
    std::string key;
    std::string value;
    std::regex doris_start("^doris_");
    line.reserve(512);
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
        StripWhiteSpace(&kv.second);

        // compatible with doris_config
        kv.first = std::regex_replace(kv.first, doris_start, "");

        if (kv.first.compare("webserver_port") == 0) {
            kv.first = "be_http_port";
        }

        auto [_, ok] = _configs.insert(kv);
        if (!ok) {
            std::cerr << "Duplicate configuration item encountered: " << kv.first << '\n';
            return false;
        }
    }
    return true;
}

// Init conf fields.
bool init(const char* filename) {
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

    bool ret = init(input);

    // Close the conf file.
    input.close();

    return ret;
}

bool init(std::istream& input) {
    Properties props;
    if (!props.load(input)) {
        return false;
    }

    // Set conf fields.
    for (const auto& [name, field] : Field::fields()) {
        std::string value = props.get(name).value_or(field->defval());
        StripWhiteSpace(&value);
        auto st = replaceenv(value);
        if (!st.ok()) {
            std::cerr << st << '\n';
            return false;
        }
        if (!field->parse_value(value)) {
            std::cerr << name << ": Invalid config value: '" << value << '\n';
            return false;
        }
    }
    return true;
}

Status set_config(const std::string& field, const std::string& value) {
    auto it = Field::fields().find(field);
    if (it == Field::fields().end()) {
        return Status::NotFound(fmt::format("'{}' is not found", field));
    }
    if (!it->second->valmutable()) {
        return Status::NotSupported(fmt::format("'{}' is immutable", field));
    }
    std::string real_value = value;
    RETURN_IF_ERROR(replaceenv(real_value));
    if (!it->second->parse_value(real_value)) {
        return Status::InvalidArgument(fmt::format("{}: Invalid config value: '{}'", field, value));
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
