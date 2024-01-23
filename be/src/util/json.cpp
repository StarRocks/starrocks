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

#include "util/json.h"

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "simdjson.h"
#include "util/json_converter.h"
#include "velocypack/ValueType.h"
#include "velocypack/vpack.h"

namespace starrocks {

static bool is_json_start_char(char ch) {
    return ch == '{' || ch == '[' || ch == '"';
}

StatusOr<JsonValue> JsonValue::parse_json_or_string(const Slice& src) {
    if (src.size > kJSONLengthLimit) {
        return Status::NotSupported("JSON string exceed maximum length 16MB");
    }
    try {
        if (src.empty()) {
            return JsonValue(emptyStringJsonSlice());
        }
        // Check the first character for its type
        auto end = src.get_data() + src.get_size();
        auto iter = std::find_if_not(src.get_data(), end, std::iswspace);
        if (iter != end && is_json_start_char(*iter)) {
            // Parse it as an object or array
            auto b = vpack::Parser::fromJson(src.get_data(), src.get_size());
            JsonValue res;
            res.assign(*b);
            return res;
        } else {
            // Consider it as a sub-type string
            return from_string(src);
        }
    } catch (const vpack::Exception& e) {
        return fromVPackException(e);
    }
    return Status::OK();
}

Status JsonValue::parse(const Slice& src, JsonValue* out) {
    ASSIGN_OR_RETURN(auto json_value, parse_json_or_string(src));
    *out = std::move(json_value);
    return {};
}

StatusOr<JsonValue> JsonValue::parse(const Slice& src) {
    JsonValue json;
    RETURN_IF_ERROR(parse(src, &json));
    return json;
}

JsonValue JsonValue::from_null() {
    return JsonValue(nullJsonSlice());
}

JsonValue JsonValue::from_none() {
    return JsonValue(noneJsonSlice());
}

JsonValue JsonValue::from_int(int64_t value) {
    vpack::Builder builder;
    builder.add(vpack::Value(value));
    return JsonValue(builder.slice());
}

JsonValue JsonValue::from_uint(uint64_t value) {
    vpack::Builder builder;
    builder.add(vpack::Value(value));
    return JsonValue(builder.slice());
}

JsonValue JsonValue::from_bool(bool value) {
    vpack::Builder builder;
    builder.add(vpack::Value(value));
    return JsonValue(builder.slice());
}

JsonValue JsonValue::from_double(double value) {
    vpack::Builder builder;
    builder.add(vpack::Value(value));
    return JsonValue(builder.slice());
}

JsonValue JsonValue::from_string(const Slice& value) {
    vpack::Builder builder;
    builder.add(vpack::Value(value.to_string()));
    return JsonValue(builder.slice());
}

StatusOr<JsonValue> JsonValue::from_simdjson(simdjson::ondemand::value* value) {
    return convert_from_simdjson(*value);
}

StatusOr<JsonValue> JsonValue::from_simdjson(simdjson::ondemand::object* obj) {
    return convert_from_simdjson(*obj);
}

size_t JsonValue::serialize(uint8_t* dst) const {
    memcpy(dst, binary_.data(), binary_.size());
    return serialize_size();
}

uint64_t JsonValue::serialize_size() const {
    return binary_.size();
}

// NOTE: JsonValue must be a valid JSON, which means to_string should not fail
StatusOr<std::string> JsonValue::to_string() const {
    if (binary_.empty() || to_vslice().type() == vpack::ValueType::None) {
        return "";
    }
    return callVPack<std::string>([this]() {
        VSlice slice = to_vslice();
        vpack::Options options = vpack::Options::Defaults;
        options.singleLinePrettyPrint = true;

        std::string result;
        return slice.toJson(result, &options);
    });
}

std::string JsonValue::to_string_uncheck() const {
    auto res = to_string();
    if (res.ok()) {
        return res.value();
    } else {
        return "";
    }
}

vpack::Slice JsonValue::to_vslice() const {
    return vpack::Slice((const uint8_t*)binary_.data());
}

static inline int cmpDouble(double left, double right) {
    if (std::isless(left, right)) {
        return -1;
    } else if (std::isgreater(left, right)) {
        return 1;
    }
    return 0;
}

static int sliceCompare(const vpack::Slice& left, const vpack::Slice& right) {
    if (left.isObject() && right.isObject()) {
        for (auto it : vpack::ObjectIterator(left)) {
            auto sub = right.get(it.key.stringRef());
            if (!sub.isNone()) {
                int x = sliceCompare(it.value, sub);
                if (x != 0) {
                    return x;
                }
            } else {
                return 1;
            }
        }
        return left.length() - right.length();
    } else if (left.isArray() && right.isArray()) {
        if (left.length() != right.length()) {
            return left.length() - right.length();
        }
        for (size_t i = 0; i < left.length(); i++) {
            auto left_item = left.at(i);
            auto right_item = right.at(i);
            int x = sliceCompare(left_item, right_item);
            if (x != 0) {
                return x;
            }
        }
        return 0;
    } else if (vpack::valueTypeGroup(left.type()) == vpack::valueTypeGroup(right.type())) {
        // 1. type are exactly same
        // 2. type are both number, but could smallInt/Int/Double
        if (left.type() == right.type()) {
            switch (left.type()) {
            case vpack::ValueType::Bool:
                return left.getBool() - right.getBool();
            case vpack::ValueType::SmallInt:
            case vpack::ValueType::Int:
            case vpack::ValueType::UInt:
                return left.getInt() - right.getInt();
            case vpack::ValueType::Double: {
                return cmpDouble(left.getDouble(), right.getDouble());
            }
            case vpack::ValueType::String:
                return left.stringRef().compare(right.stringRef());
            default:
                // other types like illegal, none, min, max are considered equal
                return 0;
            }
        } else if (left.isInteger() && right.isInteger()) {
            return left.getInt() - right.getInt();
        } else {
            return cmpDouble(left.getNumber<double>(), right.getNumber<double>());
        }
    } else {
        if (left.type() == vpack::ValueType::MinKey) {
            return -1;
        }
        if (right.type() == vpack::ValueType::MinKey) {
            return 1;
        }
        if (left.type() == vpack::ValueType::MaxKey) {
            return 1;
        }
        if (right.type() == vpack::ValueType::MaxKey) {
            return -1;
        }
        return (int)left.type() - (int)right.type();
    }
    return 0;
}

int JsonValue::compare(const JsonValue& rhs) const {
    auto left = to_vslice();
    auto right = rhs.to_vslice();
    return sliceCompare(left, right);
}

int JsonValue::compare(const Slice& lhs, const Slice& rhs) {
    vpack::Slice ls;
    if (lhs.size > 0) {
        ls = vpack::Slice((const uint8_t*)lhs.data);
    } else {
        ls = vpack::Slice::noneSlice();
    }
    vpack::Slice rs;
    if (rhs.size > 0) {
        rs = vpack::Slice((const uint8_t*)rhs.data);
    } else {
        rs = vpack::Slice::noneSlice();
    }

    return sliceCompare(ls, rs);
}

int64_t JsonValue::hash() const {
    return to_vslice().normalizedHash();
}

Slice JsonValue::get_slice() const {
    return Slice(binary_);
}

JsonType JsonValue::get_type() const {
    return fromVPackType(to_vslice().type());
}

StatusOr<bool> JsonValue::get_bool() const {
    return callVPack<bool>([this]() { return to_vslice().getBool(); });
}

StatusOr<int64_t> JsonValue::get_int() const {
    return callVPack<int64_t>([this]() { return to_vslice().getNumber<int64_t>(); });
}

StatusOr<uint64_t> JsonValue::get_uint() const {
    return callVPack<uint64_t>([this]() { return to_vslice().getNumber<uint64_t>(); });
}

StatusOr<double> JsonValue::get_double() const {
    return callVPack<double>([this]() { return to_vslice().getNumber<double>(); });
}

StatusOr<Slice> JsonValue::get_string() const {
    return callVPack<Slice>([this]() {
        vpack::ValueLength len;
        const char* str = to_vslice().getString(len);
        return Slice(str, len);
    });
}

StatusOr<JsonValue> JsonValue::get_obj(const std::string& key) const {
    return callVPack<JsonValue>([this, &key]() {
        auto ss = to_vslice().get(key);
        return JsonValue(ss);
    });
}

bool JsonValue::is_null() const {
    return to_vslice().isNull();
}

bool JsonValue::is_none() const {
    return to_vslice().isNone();
}

std::ostream& operator<<(std::ostream& os, const JsonValue& json) {
    return os << json.to_string_uncheck();
}

} //namespace starrocks
