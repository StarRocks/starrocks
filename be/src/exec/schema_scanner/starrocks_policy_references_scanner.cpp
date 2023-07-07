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

#include "exec/schema_scanner/starrocks_policy_references_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc StarrocksPolicyReferencesScanner::_s_policy_references_columns[] = {
        //   name,       type,          size
        {"POLICY_DATABASE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"POLICY_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"POLICY_TYPE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"REF_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"REF_DATABASE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"REF_OBJECT_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"REF_COLUMN", TYPE_VARCHAR, sizeof(StringValue), true},
};

StarrocksPolicyReferencesScanner::StarrocksPolicyReferencesScanner()
        : SchemaScanner(_s_policy_references_columns,
                        sizeof(_s_policy_references_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

StarrocksPolicyReferencesScanner::~StarrocksPolicyReferencesScanner() = default;

Status StarrocksPolicyReferencesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TGetPolicyReferencesRequest policy_references_param;
    if (nullptr != _param->ip && 0 != _param->port) {
        int32_t timeout = static_cast<int32_t>(std::min(state->query_options().query_timeout * 1000, INT_MAX));
        RETURN_IF_ERROR(SchemaHelper::get_policy_references(*(_param->ip), _param->port, policy_references_param,
                                                            &_policy_references_result, timeout));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status StarrocksPolicyReferencesScanner::fill_chunk(ChunkPtr* chunk) {
    const TGetPolicyReferenceItem& policy_reference_item =
            _policy_references_result.policy_reference[_policy_references_index];

    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // POLICY_DATABASE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                if (policy_reference_item.__isset.policy_database) {
                    const std::string* str = &policy_reference_item.policy_database;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 2: {
            // POLICY_NAME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                if (policy_reference_item.__isset.policy_name) {
                    const std::string* str = &policy_reference_item.policy_name;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 3: {
            // POLICY_TYPE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                if (policy_reference_item.__isset.policy_type) {
                    const std::string* str = &policy_reference_item.policy_type;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 4: {
            // REF_CATALOG
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                if (policy_reference_item.__isset.ref_catalog) {
                    const std::string* str = &policy_reference_item.ref_catalog;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 5: {
            // REF_DATABASE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                if (policy_reference_item.__isset.ref_database) {
                    const std::string* str = &policy_reference_item.ref_database;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 6: {
            // REF_OBJECT_NAME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(6);
                if (policy_reference_item.__isset.ref_object_name) {
                    const std::string* str = &policy_reference_item.ref_object_name;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 7: {
            // REF_COLUMN
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(7);
                if (policy_reference_item.__isset.ref_column) {
                    const std::string* str = &policy_reference_item.ref_column;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        default:
            break;
        }
    }
    _policy_references_index++;
    return Status::OK();
}

Status StarrocksPolicyReferencesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_policy_references_index >= _policy_references_result.policy_reference.size()) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks