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

#include "types/array_type_info.h"

#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "util/mem_util.hpp"

namespace starrocks {

class ArrayTypeInfo final : public TypeInfo {
public:
    virtual ~ArrayTypeInfo() = default;
    explicit ArrayTypeInfo(const TypeInfoPtr& item_type_info)
            : _item_type_info(item_type_info), _item_size(item_type_info->size()) {}

    void shallow_copy(void* dest, const void* src) const override {
        unaligned_store<Collection>(dest, unaligned_load<Collection>(src));
    }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override {
        Collection dest_value;
        auto src_value = unaligned_load<Collection>(src);

        dest_value.length = src_value.length;

        size_t item_size = src_value.length * _item_size;
        size_t nulls_size = src_value.has_null ? src_value.length : 0;
        dest_value.data = mem_pool->allocate(item_size + nulls_size);
        assert(dest_value.data != nullptr);
        dest_value.has_null = src_value.has_null;
        dest_value.null_signs = src_value.has_null ? reinterpret_cast<uint8_t*>(dest_value.data) + item_size : nullptr;

        // copy null_signs
        if (src_value.has_null) {
            memory_copy(dest_value.null_signs, src_value.null_signs, sizeof(uint8_t) * src_value.length);
        }

        // copy item
        for (uint32_t i = 0; i < src_value.length; ++i) {
            if (dest_value.is_null_at(i)) {
                auto* item = reinterpret_cast<Collection*>((uint8_t*)dest_value.data + i * _item_size);
                item->data = nullptr;
                item->length = 0;
                item->has_null = false;
                item->null_signs = nullptr;
            } else {
                _item_type_info->deep_copy((uint8_t*)(dest_value.data) + i * _item_size,
                                           (uint8_t*)(src_value.data) + i * _item_size, mem_pool);
            }
        }
        unaligned_store<Collection>(dest, dest_value);
    }

    void direct_copy(void* dest, const void* src) const override { CHECK(false); }

    Status from_string(void* buf, const std::string& scan_key) const override {
        return Status::NotSupported("Not supported function");
    }

    std::string to_string(const void* src) const override {
        auto src_value = unaligned_load<Collection>(src);
        std::string result = "[";

        for (size_t i = 0; i < src_value.length; ++i) {
            if (src_value.has_null && src_value.null_signs[i]) {
                result += "NULL";
            } else {
                result += _item_type_info->to_string((uint8_t*)(src_value.data) + i * _item_size);
            }
            if (i != src_value.length - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    }

    void set_to_max(void* buf) const override { DCHECK(false) << "set_to_max of list is not implemented."; }

    void set_to_min(void* buf) const override { DCHECK(false) << "set_to_min of list is not implemented."; }

    size_t size() const override { return sizeof(Collection); }

    LogicalType type() const override { return TYPE_ARRAY; }

    const TypeInfoPtr& item_type_info() const { return _item_type_info; }

protected:
    int _datum_cmp_impl(const Datum& left, const Datum& right) const override {
        CHECK(false) << "not implemented";
        return -1;
    }

private:
    TypeInfoPtr _item_type_info;
    const size_t _item_size;
};

TypeInfoPtr get_array_type_info(const TypeInfoPtr& item_type) {
    return std::make_shared<ArrayTypeInfo>(item_type);
}

const TypeInfoPtr& get_item_type_info(const TypeInfo* type_info) {
    auto array_type_info = down_cast<const ArrayTypeInfo*>(type_info);
    return array_type_info->item_type_info();
}

} // namespace starrocks
