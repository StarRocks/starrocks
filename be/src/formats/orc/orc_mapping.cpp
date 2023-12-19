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

const OrcMappingOrcType& OrcMapping::get_orc_type_child_mapping(size_t original_pos_in_table_definition) {
    auto it = _mapping.find(original_pos_in_table_definition);
    if (it == _mapping.end()) {
        DCHECK(false);
    }
    return it->second;
}

void OrcMapping::clear() {
    _mapping.clear();
}

void OrcMapping::add_mapping(size_t original_pos_in_table_definition, const OrcMappingPtr& child_mapping,
                             const orc::Type* orc_type) {
    _mapping.emplace(original_pos_in_table_definition, OrcMappingOrcType{child_mapping, orc_type});
}

Status OrcMapping::set_lazy_load_column_id(const uint64_t slot_pos, std::list<uint64_t>* column_id_list) {
    column_id_list->push_back(get_orc_type_child_mapping(slot_pos).orc_type->getColumnId());
    return Status::OK();
}

Status OrcMapping::set_include_column_id(const uint64_t slot_pos, const TypeDescriptor& desc,
                                         std::list<uint64_t>* column_id_list) {
    if (desc.is_complex_type()) {
        // For complex types, we only select leaf column id
        RETURN_IF_ERROR(
                set_include_column_id_by_type(get_orc_type_child_mapping(slot_pos).orc_mapping, desc, column_id_list));
    } else {
        column_id_list->push_back(get_orc_type_child_mapping(slot_pos).orc_type->getColumnId());
    }
    return Status::OK();
}

Status OrcMapping::set_include_column_id_by_type(const OrcMappingPtr& mapping, const TypeDescriptor& desc,
                                                 std::list<uint64_t>* column_id_list) {
    DCHECK(mapping != nullptr);
    DCHECK(desc.is_complex_type());
    if (desc.is_struct_type()) {
        for (size_t i = 0; i < desc.children.size(); i++) {
            const TypeDescriptor& child_type = desc.children[i];
            if (child_type.is_complex_type()) {
                RETURN_IF_ERROR(set_include_column_id_by_type(mapping->get_orc_type_child_mapping(i).orc_mapping,
                                                              child_type, column_id_list));
            } else {
                column_id_list->push_back(mapping->get_orc_type_child_mapping(i).orc_type->getColumnId());
            }
        }
    } else if (desc.is_array_type()) {
        const TypeDescriptor& child_type = desc.children[0];
        if (child_type.is_complex_type()) {
            RETURN_IF_ERROR(set_include_column_id_by_type(mapping->get_orc_type_child_mapping(0).orc_mapping,
                                                          child_type, column_id_list));
        } else {
            column_id_list->push_back(mapping->get_orc_type_child_mapping(0).orc_type->getColumnId());
        }
    } else if (desc.is_map_type()) {
        if (!desc.children[0].is_unknown_type()) {
            // Map's key must be logical type, we just include it.
            column_id_list->push_back(mapping->get_orc_type_child_mapping(0).orc_type->getColumnId());
        }
        if (!desc.children[1].is_unknown_type()) {
            const TypeDescriptor& child_value_type = desc.children[1];
            // Only value will be complex type
            if (child_value_type.is_complex_type()) {
                RETURN_IF_ERROR(set_include_column_id_by_type(mapping->get_orc_type_child_mapping(1).orc_mapping,
                                                              child_value_type, column_id_list));
            } else {
                column_id_list->push_back(mapping->get_orc_type_child_mapping(1).orc_type->getColumnId());
            }
        }
    } else {
        return Status::InternalError("Unreachable");
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<OrcMapping>> OrcMappingFactory::build_mapping(
        const std::vector<SlotDescriptor*>& slot_descs, const orc::Type& root_orc_type, const bool use_orc_column_names,
        const std::vector<std::string>* hive_column_names, const OrcMappingOptions& options) {
    std::unique_ptr<OrcMapping> orc_mapping = std::make_unique<OrcMapping>();
    Status res;
    // All mapping relation will only affect the first level,
    // Struct subfields still mapping according to subfield name.
    if (use_orc_column_names) {
        // If enable use_orc_column_names[Default is false], in first level, we will use column name to
        // build mapping relation.
        // This function is used for UT and Broker Load now.
        res = _init_orc_mapping_with_orc_column_names(orc_mapping, slot_descs, &root_orc_type, options);
    } else {
        // Use the column names in hive_column_names to establish a mapping,
        // the first column name in hive_column_names is mapped to the first column in orc, and so on.
        // NOTICE: The column order in SlotDescriptor[] is different from hive_column_names, so we need to build
        // two mapping in below function.
        res = _init_orc_mapping_with_hive_column_names(orc_mapping, slot_descs, &root_orc_type, hive_column_names,
                                                       options);
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
    // check orc type -> slot type desc
    if (orc_source_type->getKind() == orc::TypeKind::LIST) {
        can_convert &= slot_target_type.is_array_type();
    } else if (orc_source_type->getKind() == orc::TypeKind::MAP) {
        can_convert &= slot_target_type.is_map_type();
    } else if (orc_source_type->getKind() == orc::TypeKind::STRUCT) {
        can_convert &= slot_target_type.is_struct_type();
    }

    // check slot type desc -> orc type
    if (slot_target_type.is_array_type()) {
        can_convert &= orc_source_type->getKind() == orc::TypeKind::LIST;
    } else if (slot_target_type.is_map_type()) {
        can_convert &= orc_source_type->getKind() == orc::TypeKind::MAP;
    } else if (slot_target_type.is_struct_type()) {
        can_convert &= orc_source_type->getKind() == orc::TypeKind::STRUCT;
    }

    //TODO Other logical type not check now!

    if (!can_convert) {
        return Status::NotSupported(
                strings::Substitute("Orc's type $0 and Slot's type $1 can't convert to each other, filename = $2",
                                    orc_source_type->toString(), slot_target_type.debug_string(), options.filename));
    }
    return Status::OK();
}

Status OrcMappingFactory::_init_orc_mapping_with_orc_column_names(std::unique_ptr<OrcMapping>& mapping,
                                                                  const std::vector<SlotDescriptor*>& slot_descs,
                                                                  const orc::Type* orc_root_type,
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

        // handle nested mapping for complex type mapping
        if (slot_desc->type().is_complex_type()) {
            need_add_child_mapping = std::make_shared<OrcMapping>();
            const TypeDescriptor& origin_type = slot_desc->type();
            RETURN_IF_ERROR(_set_child_mapping(need_add_child_mapping, origin_type, orc_sub_type, options));
        }

        mapping->add_mapping(i, need_add_child_mapping, orc_sub_type);
    }
    return Status::OK();
}

Status OrcMappingFactory::_init_orc_mapping_with_hive_column_names(std::unique_ptr<OrcMapping>& mapping,
                                                                   const std::vector<SlotDescriptor*>& slot_descs,
                                                                   const orc::Type* orc_root_type,
                                                                   const std::vector<std::string>* hive_column_names,
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
        // handle nested mapping for complex type mapping
        if (slot_descs[pos_in_slot_descriptor]->type().is_complex_type()) {
            need_add_child_mapping = std::make_shared<OrcMapping>();
            const TypeDescriptor& origin_type = slot_descs[pos_in_slot_descriptor]->type();
            RETURN_IF_ERROR(_set_child_mapping(need_add_child_mapping, origin_type, orc_sub_type, options));
        }

        mapping->add_mapping(pos_in_slot_descriptor, need_add_child_mapping, orc_sub_type);
    }
    return Status::OK();
}

// origin_type is TypeDescriptor in table definition
// orc_type is orc's type
Status OrcMappingFactory::_set_child_mapping(const OrcMappingPtr& mapping, const TypeDescriptor& origin_type,
                                             const orc::Type* orc_type, const OrcMappingOptions& options) {
    DCHECK(origin_type.is_complex_type());
    if (origin_type.type == LogicalType::TYPE_STRUCT) {
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

            if (origin_child_type.is_complex_type()) {
                need_add_child_mapping = std::make_shared<OrcMapping>();
                RETURN_IF_ERROR(_set_child_mapping(need_add_child_mapping, origin_child_type, orc_child_type, options));
            }
            mapping->add_mapping(index, need_add_child_mapping, orc_child_type);
        }
    } else if (origin_type.type == LogicalType::TYPE_ARRAY) {
        const orc::Type* orc_child_type = orc_type->getSubtype(0);
        const TypeDescriptor& origin_child_type = origin_type.children[0];
        // Check Array's element can be converted
        RETURN_IF_ERROR(_check_orc_type_can_convert_2_logical_type(orc_child_type, origin_child_type, options));

        OrcMappingPtr need_add_child_mapping = nullptr;

        if (origin_child_type.is_complex_type()) {
            need_add_child_mapping = std::make_shared<OrcMapping>();
            RETURN_IF_ERROR(_set_child_mapping(need_add_child_mapping, origin_child_type, orc_child_type, options));
        }

        mapping->add_mapping(0, need_add_child_mapping, orc_child_type);
    } else if (origin_type.type == LogicalType::TYPE_MAP) {
        // Check Map's key can be converted
        if (!origin_type.children[0].is_unknown_type()) {
            RETURN_IF_ERROR(_check_orc_type_can_convert_2_logical_type(orc_type->getSubtype(0), origin_type.children[0],
                                                                       options));
        }
        // Check Map's value can be converted
        if (!origin_type.children[1].is_unknown_type()) {
            RETURN_IF_ERROR(_check_orc_type_can_convert_2_logical_type(orc_type->getSubtype(1), origin_type.children[1],
                                                                       options));
        }

        // Map's key must be primitive type
        mapping->add_mapping(0, nullptr, orc_type->getSubtype(0));

        // Use for map's value
        OrcMappingPtr need_add_value_child_mapping = nullptr;

        // Map's key must be logical type, so we only consider value.
        const TypeDescriptor& origin_child_type = origin_type.children[1];

        if (origin_child_type.is_complex_type()) {
            need_add_value_child_mapping = std::make_shared<OrcMapping>();
            const orc::Type* orc_child_type = orc_type->getSubtype(1);
            RETURN_IF_ERROR(
                    _set_child_mapping(need_add_value_child_mapping, origin_child_type, orc_child_type, options));
        }
        mapping->add_mapping(1, need_add_value_child_mapping, orc_type->getSubtype(1));
    } else {
        return Status::InternalError("Unreachable");
    }
    return Status::OK();
}

} // namespace starrocks
