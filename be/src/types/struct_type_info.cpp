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

#include "common/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "types/map_type_info.h"
#include "util/mem_util.hpp"

namespace starrocks {

class StructTypeInfo final : public TypeInfo {
public:
    virtual ~StructTypeInfo() = default;
    explicit StructTypeInfo(std::vector<TypeInfoPtr> field_types) : _field_types(std::move(field_types)) {}

    void shallow_copy(void* dest, const void* src) const override { CHECK(false); }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override { CHECK(false); }

    void direct_copy(void* dest, const void* src, MemPool* mem_pool) const override { CHECK(false); }

    Status from_string(void* buf, const std::string& scan_key) const override {
        return Status::NotSupported("Not supported function");
    }

    std::string to_string(const void* src) const override { return "{}"; }

    void set_to_max(void* buf) const override { DCHECK(false) << "set_to_max of list is not implemented."; }

    void set_to_min(void* buf) const override { DCHECK(false) << "set_to_min of list is not implemented."; }

    size_t size() const override { return 16; }

    LogicalType type() const override { return TYPE_STRUCT; }

    const std::vector<TypeInfoPtr>& field_types() const { return _field_types; }

protected:
    int _datum_cmp_impl(const Datum& left, const Datum& right) const override {
        CHECK(false) << "not implemented";
        return -1;
    }

private:
    std::vector<TypeInfoPtr> _field_types;
};

TypeInfoPtr get_struct_type_info(std::vector<TypeInfoPtr> field_types) {
    return std::make_shared<StructTypeInfo>(std::move(field_types));
}

const std::vector<TypeInfoPtr>& get_struct_field_types(const TypeInfo* type_info) {
    auto struct_type = down_cast<const StructTypeInfo*>(type_info);
    return struct_type->field_types();
}

} // namespace starrocks
