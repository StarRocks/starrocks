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

#include "types/row_id_type_info.h"

#include "common/logging.h"
#include "runtime/mem_pool.h"

namespace starrocks {

class RowIdTypeInfo final : public TypeInfo {
public:
    RowIdTypeInfo() = default;
    virtual ~RowIdTypeInfo() = default;

    void shallow_copy(void* dest, const void* src) const override { CHECK(false); }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override { CHECK(false); }

    void direct_copy(void* dest, const void* src) const override { CHECK(false); }

    Status from_string(void* buf, const std::string& scan_key) const override {
        return Status::NotSupported("Not supported function");
    }

    std::string to_string(const void* src) const override { return "{}"; }

    void set_to_max(void* buf) const override { DCHECK(false) << "set_to_max of list is not implemented."; }

    void set_to_min(void* buf) const override { DCHECK(false) << "set_to_min of list is not implemented."; }

    size_t size() const override { return 12; }

    LogicalType type() const override { return TYPE_ROW_ID; }

protected:
    int _datum_cmp_impl(const Datum& left, const Datum& right) const override {
        CHECK(false) << "not implemented";
        return -1;
    }
};

TypeInfoPtr get_row_id_type_info() {
    return std::make_shared<RowIdTypeInfo>();
}

} // namespace starrocks
