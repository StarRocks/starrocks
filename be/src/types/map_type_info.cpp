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

#include "types/map_type_info.h"

#include "common/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "util/mem_util.hpp"

namespace starrocks {

class MapTypeInfo final : public TypeInfo {
public:
    virtual ~MapTypeInfo() = default;
    explicit MapTypeInfo(TypeInfoPtr key_type, TypeInfoPtr value_type)
            : _key_type(std::move(key_type)), _value_type(std::move(value_type)) {}

    void shallow_copy(void* dest, const void* src) const override { CHECK(false); }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override { CHECK(false); }

    void direct_copy(void* dest, const void* src) const override { CHECK(false); }

    Status from_string(void* buf, const std::string& scan_key) const override {
        return Status::NotSupported("Not supported function");
    }

    std::string to_string(const void* src) const override { return "{}"; }

    void set_to_max(void* buf) const override { DCHECK(false) << "set_to_max of list is not implemented."; }

    void set_to_min(void* buf) const override { DCHECK(false) << "set_to_min of list is not implemented."; }

    size_t size() const override { return 16; }

    LogicalType type() const override { return TYPE_MAP; }

    const TypeInfoPtr& key_type() const { return _key_type; }
    const TypeInfoPtr& value_type() const { return _value_type; }

protected:
    int _datum_cmp_impl(const Datum& left, const Datum& right) const override {
        CHECK(false) << "not implemented";
        return -1;
    }

private:
    TypeInfoPtr _key_type;
    TypeInfoPtr _value_type;
};

TypeInfoPtr get_map_type_info(const TypeInfoPtr& key_type, const TypeInfoPtr& value_type) {
    return std::make_shared<MapTypeInfo>(key_type, value_type);
}

const TypeInfoPtr& get_key_type_info(const TypeInfo* type_info) {
    auto map_type = down_cast<const MapTypeInfo*>(type_info);
    return map_type->key_type();
}

const TypeInfoPtr& get_value_type_info(const TypeInfo* type_info) {
    auto map_type = down_cast<const MapTypeInfo*>(type_info);
    return map_type->value_type();
}

} // namespace starrocks
