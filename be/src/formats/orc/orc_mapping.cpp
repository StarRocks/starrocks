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

#include "formats/orc/orc_mapping.h"

#include "common/statusor.h"

namespace starrocks {

const OrcMappingContext& OrcMapping::get_orc_mapping_context(size_t original_pos_in_table_definition) {
    auto it = _mapping.find(original_pos_in_table_definition);
    if (it == _mapping.end()) {
        DCHECK(false);
    }
    return it->second;
}

void OrcMapping::clear() {
    _mapping.clear();
}

void OrcMapping::add_context(size_t original_pos_in_table_definition, const OrcMappingPtr& child_mapping,
                             const orc::Type* orc_type, const ColumnAccessPathPtr* path) {
    _mapping.emplace(original_pos_in_table_definition, OrcMappingContext{child_mapping, orc_type, path});
}

Status OrcMapping::set_lazy_load_column_id(const uint64_t slot_pos, std::list<uint64_t>* column_id_list) {
    column_id_list->push_back(get_orc_mapping_context(slot_pos).orc_type->getColumnId());
    return Status::OK();
}

Status OrcMapping::set_include_column_id(const uint64_t slot_pos, const TypeDescriptor& desc,
                                         std::list<uint64_t>* column_id_list) {
    const OrcMappingContext& context = get_orc_mapping_context(slot_pos);
    if (desc.is_complex_type()) {
        // For complex types, we only select leaf column id
        RETURN_IF_ERROR(set_include_column_id_by_type(context, desc, column_id_list));
    } else {
        column_id_list->push_back(get_orc_mapping_context(slot_pos).orc_type->getColumnId());
    }
    return Status::OK();
}

Status OrcMapping::set_include_column_id_by_type(const OrcMappingContext& context, const TypeDescriptor& desc,
                                                 std::list<uint64_t>* column_id_list) {
    DCHECK(context.orc_type != nullptr);
    const ColumnAccessPathPtr* path = context.path;

    if (ColumnAccessPathUtil::is_select_all_subfields(path)) {
        // Means we select all subfields
        column_id_list->push_back(context.orc_type->getColumnId());
        return Status::OK();
    }

    // path not nullptr, means we only select needed subfields
    if (desc.is_struct_type()) {
        std::vector<bool> selected_subfields = ColumnAccessPathUtil::get_selected_subfields_for_struct(desc, path);
        for (size_t i = 0; i < desc.children.size(); i++) {
            if (!selected_subfields[i]) {
                // subfields are not selected, we don't need to include it
                continue;
            }
            const TypeDescriptor& child_type = desc.children[i];
            RETURN_IF_ERROR(set_include_column_id_by_type(context.orc_mapping->get_orc_mapping_context(i), child_type,
                                                          column_id_list));
        }
    } else if (desc.is_array_type()) {
        // We must include the element column always
        const TypeDescriptor& child_type = desc.children[0];
        RETURN_IF_ERROR(set_include_column_id_by_type(context.orc_mapping->get_orc_mapping_context(0), child_type,
                                                      column_id_list));
    } else if (desc.is_map_type()) {
        std::vector<bool> selected_subfields = ColumnAccessPathUtil::get_selected_subfields_for_map(desc, path);

        if (selected_subfields[0]) {
            const TypeDescriptor& key_type = desc.children[0];
            RETURN_IF_ERROR(set_include_column_id_by_type(context.orc_mapping->get_orc_mapping_context(0), key_type,
                                                          column_id_list));
        }

        if (selected_subfields[1]) {
            const TypeDescriptor& value_type = desc.children[1];
            RETURN_IF_ERROR(set_include_column_id_by_type(context.orc_mapping->get_orc_mapping_context(1), value_type,
                                                          column_id_list));
        }
    } else {
        // If it's a primitive type and ColumnAccessPath is not a nullptr, we include it directly
        column_id_list->push_back(context.orc_type->getColumnId());
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<OrcMapping>> OrcMappingFactory::build_mapping(
        const std::vector<SlotDescriptor*>& slot_descs, const orc::Type& root_orc_type, const bool use_orc_column_names,
        const std::vector<std::string>* hive_column_names,
        const std::unordered_map<std::string, ColumnAccessPathPtr>* column_name_2_column_access_path_mapping,
        const OrcMappingOptions& options) {
    std::unique_ptr<OrcMapping> orc_mapping = std::make_unique<OrcMapping>();
    Status res;
    // All mapping relation will only affect the first level,
    // Struct subfields still mapping according to subfield name.
    if (use_orc_column_names) {
        // If enable use_orc_column_names[Default is false], in first level, we will use column name to
        // build mapping relation.
        // This function is used for UT and Broker Load now.
        res = _init_orc_mapping_with_orc_column_names(orc_mapping, slot_descs, &root_orc_type,
                                                      column_name_2_column_access_path_mapping, options);
    } else {
        // Use the column names in hive_column_names to establish a mapping,
        // the first column name in hive_column_names is mapped to the first column in orc, and so on.
        // NOTICE: The column order in SlotDescriptor[] is different from hive_column_names, so we need to build
        // two mapping in below function.
        res = _init_orc_mapping_with_hive_column_names(orc_mapping, slot_descs, &root_orc_type, hive_column_names,
                                                       column_name_2_column_access_path_mapping, options);
    }
    if (!res.ok()) {
        return res;
    }
    return orc_mapping;
}

Status OrcMappingFactory::_check_orc_type_can_convert_2_logical_type(const orc::Type* orc_source_type,
                                                                     const TypeDescriptor& slot_target_type,
                                                                     const OrcMappingOptions& options) {
    bool can_convert = true;
    if (orc_source_type->getKind() == orc::TypeKind::LIST) {
        can_convert = slot_target_type.is_array_type();
    } else if (orc_source_type->getKind() == orc::TypeKind::MAP) {
        can_convert = slot_target_type.is_map_type();
    } else if (orc_source_type->getKind() == orc::TypeKind::STRUCT) {
        can_convert = slot_target_type.is_struct_type();
    }

    //TODO Other logical type not check now!

    if (!can_convert) {
        return Status::NotSupported(
                strings::Substitute("Not support to convert orc's type from $0 to $1, filename = $2",
                                    orc_source_type->toString(), slot_target_type.debug_string(), options.filename));
    }
    return Status::OK();
}

Status OrcMappingFactory::_init_orc_mapping_with_orc_column_names(
        std::unique_ptr<OrcMapping>& mapping, const std::vector<SlotDescriptor*>& slot_descs,
        const orc::Type* orc_root_type,
        const std::unordered_map<std::string, ColumnAccessPathPtr>* column_name_2_column_access_path_mapping,
        const OrcMappingOptions& options) {
    // build mapping for orc [orc field name -> pos in orc]
    std::unordered_map<std::string, size_t> orc_fieldname_2_pos;
    for (size_t i = 0; i < orc_root_type->getSubtypeCount(); i++) {
        std::string col_name = Utils::format_name(orc_root_type->getFieldName(i), options.case_sensitive);
        orc_fieldname_2_pos.emplace(col_name, i);
    }

    for (size_t i = 0; i < slot_descs.size(); i++) {
        SlotDescriptor* slot_desc = slot_descs[i];
        if (slot_desc == nullptr) continue;

        std::string col_name = Utils::format_name(slot_desc->col_name(), options.case_sensitive);
        auto it = orc_fieldname_2_pos.find(col_name);
        if (it == orc_fieldname_2_pos.end()) {
            auto s = strings::Substitute("OrcMappingFactory::_init_orc_mapping not found column name $0, file = $1",
                                         col_name, options.filename);
            return Status::NotFound(s);
        }

        const orc::Type* orc_sub_type = orc_root_type->getSubtype(it->second);

        RETURN_IF_ERROR(_check_orc_type_can_convert_2_logical_type(orc_sub_type, slot_desc->type(), options));

        OrcMappingPtr need_add_child_mapping = nullptr;

        const ColumnAccessPathPtr* path = nullptr;

        // handle nested mapping for complex type mapping
        if (slot_desc->type().is_complex_type()) {
            // Using un-formatted name
            path = ColumnAccessPathUtil::get_column_access_path_from_mapping(column_name_2_column_access_path_mapping,
                                                                             slot_desc->col_name());
            need_add_child_mapping = std::make_shared<OrcMapping>();
            const TypeDescriptor& origin_type = slot_desc->type();
            RETURN_IF_ERROR(_set_child_mapping(need_add_child_mapping, origin_type, orc_sub_type, path, options));
        }

        mapping->add_context(i, need_add_child_mapping, orc_sub_type, path);
    }
    return Status::OK();
}

Status OrcMappingFactory::_init_orc_mapping_with_hive_column_names(
        std::unique_ptr<OrcMapping>& mapping, const std::vector<SlotDescriptor*>& slot_descs,
        const orc::Type* orc_root_type, const std::vector<std::string>* hive_column_names,
        const std::unordered_map<std::string, ColumnAccessPathPtr>* column_name_2_column_access_path_mapping,
        const OrcMappingOptions& options) {
    DCHECK(hive_column_names != nullptr);

    // build mapping for [SlotDescriptor's name -> SlotDescriptor' pos]
    std::unordered_map<std::string, size_t> slot_descriptor_name_2_slot_descriptor_pos;
    for (size_t i = 0; i < slot_descs.size(); i++) {
        if (slot_descs[i] == nullptr) continue;
        slot_descriptor_name_2_slot_descriptor_pos.emplace(
                Utils::format_name(slot_descs[i]->col_name(), options.case_sensitive), i);
    }

    // build hive column names index.
    // if there are 64 columns in hive meta, but actually there are 63 columns in orc file
    // then we will read invalid column id.
    size_t read_column_size = std::min(hive_column_names->size(), orc_root_type->getSubtypeCount());

    for (size_t i = 0; i < read_column_size; i++) {
        const orc::Type* orc_sub_type = orc_root_type->getSubtype(i);

        const std::string find_column_name = Utils::format_name((*hive_column_names)[i], options.case_sensitive);
        auto it = slot_descriptor_name_2_slot_descriptor_pos.find(find_column_name);
        if (it == slot_descriptor_name_2_slot_descriptor_pos.end()) {
            // The column name in hive_column_names has no corresponding column name in slot_description
            // TODO(SmithCruise) This situation only happened in UT now, I'm not sure this situation will happened in production.
            // So here we don't report an error but skip it directly, just in case.
            continue;
            //  auto s = strings::Substitute("OrcMappingFactory::_init_orc_mapping not found column name $0", find_column_name);
            //  return Status::NotFound(s);
        }

        size_t pos_in_slot_descriptor = it->second;

        RETURN_IF_ERROR(_check_orc_type_can_convert_2_logical_type(
                orc_sub_type, slot_descs[pos_in_slot_descriptor]->type(), options));

        OrcMappingPtr need_add_child_mapping = nullptr;
        const ColumnAccessPathPtr* path = nullptr;
        // handle nested mapping for complex type mapping
        if (slot_descs[pos_in_slot_descriptor]->type().is_complex_type()) {
            path = ColumnAccessPathUtil::get_column_access_path_from_mapping(column_name_2_column_access_path_mapping,
                                                                             (*hive_column_names)[i]);
            need_add_child_mapping = std::make_shared<OrcMapping>();
            const TypeDescriptor& origin_type = slot_descs[pos_in_slot_descriptor]->type();
            RETURN_IF_ERROR(_set_child_mapping(need_add_child_mapping, origin_type, orc_sub_type, path, options));
        }

        mapping->add_context(pos_in_slot_descriptor, need_add_child_mapping, orc_sub_type, path);
    }
    return Status::OK();
}

// origin_type is TypeDescriptor in table definition
// orc_type is orc's type
Status OrcMappingFactory::_set_child_mapping(const OrcMappingPtr& mapping, const TypeDescriptor& origin_type,
                                             const orc::Type* orc_type, const ColumnAccessPathPtr* column_access_path,
                                             const OrcMappingOptions& options) {
    DCHECK(origin_type.is_complex_type());
    if (origin_type.type == LogicalType::TYPE_STRUCT) {
        if (orc_type->getKind() != orc::TypeKind::STRUCT) {
            return Status::InternalError(strings::Substitute(
                    "Orc nest type check error: expect type in this layer is STRUCT, actual type is $0, ",
                    orc_type->toString()));
        }

        std::unordered_map<std::string, size_t> tmp_orc_fieldname_2_pos;
        for (size_t i = 0; i < orc_type->getSubtypeCount(); i++) {
            std::string field_name = Utils::format_name(orc_type->getFieldName(i), options.case_sensitive);
            tmp_orc_fieldname_2_pos[field_name] = i;
        }

        for (size_t index = 0; index < origin_type.children.size(); index++) {
            std::string field_name = Utils::format_name(origin_type.field_names[index], options.case_sensitive);
            auto it = tmp_orc_fieldname_2_pos.find(field_name);
            if (it == tmp_orc_fieldname_2_pos.end()) {
                auto s = strings::Substitute(
                        "OrcChunkReader::_set_child_mapping "
                        "not found struct subfield $0, file = $1",
                        field_name, options.filename);
                return Status::NotFound(s);
            }
            const orc::Type* orc_child_type = orc_type->getSubtype(it->second);
            const TypeDescriptor& origin_child_type = origin_type.children[index];
            RETURN_IF_ERROR(_check_orc_type_can_convert_2_logical_type(orc_child_type, origin_child_type, options));

            OrcMappingPtr need_add_child_mapping = nullptr;

            const ColumnAccessPathPtr* subfield_column_access_path = nullptr;
            if (origin_child_type.is_complex_type()) {
                subfield_column_access_path = ColumnAccessPathUtil::get_struct_subfield_path(
                        column_access_path, origin_type.field_names[index]);
                need_add_child_mapping = std::make_shared<OrcMapping>();
                RETURN_IF_ERROR(_set_child_mapping(need_add_child_mapping, origin_child_type, orc_child_type,
                                                   subfield_column_access_path, options));
            }
            mapping->add_context(index, need_add_child_mapping, orc_child_type, subfield_column_access_path);
        }
    } else if (origin_type.type == LogicalType::TYPE_ARRAY) {
        if (orc_type->getKind() != orc::TypeKind::LIST) {
            return Status::InternalError(strings::Substitute(
                    "Orc nest type check error: expect type in this layer is LIST, actual type is $0, ",
                    orc_type->toString()));
        }
        const orc::Type* orc_child_type = orc_type->getSubtype(0);
        const TypeDescriptor& origin_child_type = origin_type.children[0];
        // Check Array's element can be converted
        RETURN_IF_ERROR(_check_orc_type_can_convert_2_logical_type(orc_child_type, origin_child_type, options));

        OrcMappingPtr need_add_child_mapping = nullptr;

        const ColumnAccessPathPtr* child_column_access_path = nullptr;
        if (origin_child_type.is_complex_type()) {
            child_column_access_path = ColumnAccessPathUtil::get_array_element_path(column_access_path);
            need_add_child_mapping = std::make_shared<OrcMapping>();
            RETURN_IF_ERROR(_set_child_mapping(need_add_child_mapping, origin_child_type, orc_child_type,
                                               child_column_access_path, options));
        }

        mapping->add_context(0, need_add_child_mapping, orc_child_type, child_column_access_path);
    } else if (origin_type.type == LogicalType::TYPE_MAP) {
        if (orc_type->getKind() != orc::TypeKind::MAP) {
            return Status::InternalError(strings::Substitute(
                    "Orc nest type check error: expect type in this layer is MAP, actual type is $0, ",
                    orc_type->toString()));
        }

        std::vector<bool> selected_subfields =
                ColumnAccessPathUtil::get_selected_subfields_for_map(origin_type, column_access_path);
        // Check Map's key can be converted
        if (selected_subfields[0]) {
            RETURN_IF_ERROR(_check_orc_type_can_convert_2_logical_type(orc_type->getSubtype(0), origin_type.children[0],
                                                                       options));
        }
        // Check Map's value can be converted
        if (selected_subfields[1]) {
            RETURN_IF_ERROR(_check_orc_type_can_convert_2_logical_type(orc_type->getSubtype(1), origin_type.children[1],
                                                                       options));
        }

        // Map's key must be primitive type
        mapping->add_context(0, nullptr, orc_type->getSubtype(0), nullptr);

        // Use for map's value
        OrcMappingPtr need_add_value_child_mapping = nullptr;

        // Map's key must be logical type, so we only consider value.
        const TypeDescriptor& origin_value_type = origin_type.children[1];

        const ColumnAccessPathPtr* value_column_access_path = nullptr;
        if (origin_value_type.is_complex_type()) {
            value_column_access_path = ColumnAccessPathUtil::get_map_values_path(column_access_path);
            need_add_value_child_mapping = std::make_shared<OrcMapping>();
            const orc::Type* orc_child_type = orc_type->getSubtype(1);
            RETURN_IF_ERROR(_set_child_mapping(need_add_value_child_mapping, origin_value_type, orc_child_type,
                                               value_column_access_path, options));
        }
        mapping->add_context(1, need_add_value_child_mapping, orc_type->getSubtype(1), value_column_access_path);
    } else {
        return Status::InternalError("Unreachable code in OrcMapping");
    }
    return Status::OK();
}

} // namespace starrocks
