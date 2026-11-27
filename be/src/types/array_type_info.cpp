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

#include "base/hash/unaligned_access.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "types/collection.h"
#include "types/type_info.h"

namespace starrocks {

class ArrayTypeInfo final : public TypeInfo {
public:
    explicit ArrayTypeInfo(const TypeInfoPtr& item_type_info)
            : _item_type_info(item_type_info), _item_size(item_type_info->size()) {}

    ~ArrayTypeInfo() override = default;

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
                result += _item_type_info->to_string(reinterpret_cast<uint8_t*>(src_value.data) + i * _item_size);
            }
            if (i + 1 != src_value.length) {
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
    const TypeInfoPtr _item_type_info;
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
