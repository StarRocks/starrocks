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

#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks {

using SlotTypeDescInfo = std::tuple<std::string, TypeDescriptor, bool>;
using SlotTypeDescInfoArray = std::vector<SlotTypeDescInfo>;
using SlotTypeInfo = std::tuple<std::string, LogicalType, bool>;
using SlotTypeInfoArray = std::vector<SlotTypeInfo>;

class DescTblHelper {
public:
    static void generate_desc_tuple(const SlotTypeDescInfoArray& slot_infos,
                                    TDescriptorTableBuilder* desc_tbl_builder) {
        TTupleDescriptorBuilder tuple_builder;
        for (auto& slot_info : slot_infos) {
            auto& name = std::get<0>(slot_info);
            auto type = std::get<1>(slot_info);
            auto& is_nullable = std::get<2>(slot_info);
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(type)
                    .length(type.len)
                    .precision(type.precision)
                    .scale(type.scale)
                    .nullable(is_nullable);
            slot_desc_builder.column_name(name);
            tuple_builder.add_slot(slot_desc_builder.build());
        }
        tuple_builder.build(desc_tbl_builder);
    }

    static DescriptorTbl* generate_desc_tbl(RuntimeState* runtime_state, ObjectPool& obj_pool,
                                            const std::vector<SlotTypeDescInfoArray>& slot_infos) {
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        for (auto& slot_info : slot_infos) {
            if (slot_info.empty()) {
                continue;
            }
            generate_desc_tuple(slot_info, &desc_tbl_builder);
        }
        DescriptorTbl* desc_tbl = nullptr;
        auto st = DescriptorTbl::create(runtime_state, &obj_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                                        config::vector_chunk_size);
        st.permit_unchecked_error();
        return desc_tbl;
    }

    static std::vector<SlotTypeDescInfoArray> create_slot_type_desc_info_arrays(
            const std::vector<SlotTypeInfoArray>& slot_info_arrays) {
        std::vector<SlotTypeDescInfoArray> slot_type_desc_info_arrays;
        for (auto slot_info_array : slot_info_arrays) {
            SlotTypeDescInfoArray slot_type_desc_infos;
            for (auto slot_info : slot_info_array) {
                slot_type_desc_infos.emplace_back(std::get<0>(slot_info),
                                                  TypeDescriptor::from_logical_type(std::get<1>(slot_info)),
                                                  std::get<2>(slot_info));
            }
            slot_type_desc_info_arrays.emplace_back(std::move(slot_type_desc_infos));
        }
        return slot_type_desc_info_arrays;
    }
};
} // namespace starrocks
