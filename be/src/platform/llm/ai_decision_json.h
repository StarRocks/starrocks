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

#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <new>
#include <string_view>
#include <unordered_set>
#include <vector>

// Private protocol helpers shared only by the decision input and provider.
namespace starrocks::decision_json {

// RapidJSON assumes allocations succeed. Propagate allocation rejection to
// the existing query/dispatcher exception boundary instead of dereferencing NULL.
template <typename Allocator = rapidjson::CrtAllocator>
class AIJsonAllocator : public Allocator {
public:
    void* Malloc(size_t size) {
        void* pointer = Allocator::Malloc(size);
        if (size != 0 && pointer == nullptr) throw std::bad_alloc();
        return pointer;
    }
    void* Realloc(void* pointer, size_t old_size, size_t new_size) {
        void* result = Allocator::Realloc(pointer, old_size, new_size);
        if (new_size != 0 && result == nullptr) throw std::bad_alloc();
        return result;
    }
};

using AIJsonPoolAllocator = rapidjson::MemoryPoolAllocator<AIJsonAllocator<>>;
using AIJsonValue = rapidjson::GenericValue<rapidjson::UTF8<>, AIJsonPoolAllocator>;
using AIJsonDocument = rapidjson::GenericDocument<rapidjson::UTF8<>, AIJsonPoolAllocator, AIJsonAllocator<>>;
using AIJsonStringBuffer = rapidjson::GenericStringBuffer<rapidjson::UTF8<>, AIJsonAllocator<>>;
using AIJsonWriter = rapidjson::Writer<AIJsonStringBuffer, rapidjson::UTF8<>, rapidjson::UTF8<>, AIJsonAllocator<>>;

using Value = AIJsonValue;
constexpr unsigned kParseFlags = rapidjson::kParseIterativeFlag | rapidjson::kParseValidateEncodingFlag;

inline bool parse_complete_json(std::string_view json, AIJsonDocument* doc) {
    rapidjson::MemoryStream input(json.data(), json.size());
    doc->ParseStream<kParseFlags>(input);
    // RapidJSON treats a raw NUL as end-of-input even inside a sized buffer.
    // Reject an unconsumed suffix before preserving or embedding the JSON.
    return !doc->HasParseError() && input.Tell() == json.size();
}

inline std::string_view string_view(const Value& value) {
    return {value.GetString(), value.GetStringLength()};
}

inline const Value* field(const Value& object, std::string_view key) {
    if (!object.IsObject()) return nullptr;
    Value name(rapidjson::StringRef(key.data(), key.size()));
    auto found = object.FindMember(name);
    return found == object.MemberEnd() ? nullptr : &found->value;
}

inline bool unique_fields(const Value& object) {
    if (!object.IsObject()) return false;
    std::unordered_set<std::string_view> names;
    for (const auto& member : object.GetObject()) {
        if (!names.emplace(string_view(member.name)).second) return false;
    }
    return true;
}

inline bool unique_fields_recursively(const Value& root) {
    std::vector<const Value*> pending{&root};
    while (!pending.empty()) {
        const auto* value = pending.back();
        pending.pop_back();
        if (value->IsObject()) {
            if (!unique_fields(*value)) return false;
            for (const auto& member : value->GetObject()) pending.push_back(&member.value);
        } else if (value->IsArray()) {
            for (const auto& child : value->GetArray()) pending.push_back(&child);
        }
    }
    return true;
}

} // namespace starrocks::decision_json
