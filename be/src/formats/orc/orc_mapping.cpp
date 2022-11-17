// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/orc/orc_mapping.h"

namespace starrocks::vectorized {

static std::string format_column_name(const std::string& col_name, bool case_sensitive) {
    return case_sensitive ? col_name : boost::algorithm::to_lower_copy(col_name);
}

const OrcMappingOrOrcColumnId& OrcMapping::get_column_id_or_child_mapping(size_t original_pos_in_table_defination) {
    auto it = _mapping.find(original_pos_in_table_defination);
    if (it == _mapping.end()) {
        DCHECK(false);
    }
    return it->second;
}

void OrcMapping::clear() {
    _mapping.clear();
}

void OrcMapping::add_mapping(size_t original_pos_in_table_defination, size_t orc_column_id,
                             const OrcMappingPtr& child_mapping) {
    _mapping.emplace(original_pos_in_table_defination, OrcMappingOrOrcColumnId{child_mapping, orc_column_id});
}

Status OrcMapping::set_include_column_id(const uint64_t slot_pos, const TypeDescriptor& desc,
                                         std::list<uint64_t>* column_id_list) {
    column_id_list->push_back(get_column_id_or_child_mapping(slot_pos).orc_column_id);

    // Only complex type need include it's nested level.
    if (desc.is_complex_type()) {
        RETURN_IF_ERROR(set_include_column_id_by_type(get_column_id_or_child_mapping(slot_pos).orc_mapping, desc,
                                                      column_id_list));
    }
    return Status::OK();
}

Status OrcMapping::set_include_column_id_by_type(const OrcMappingPtr& mapping, const TypeDescriptor& desc,
                                                 std::list<uint64_t>* column_id_list) {
    DCHECK(mapping != nullptr);
    DCHECK(desc.is_complex_type());
    if (desc.is_struct_type()) {
        for (size_t i = 0; i < desc.children.size(); i++) {
            // only include selected fields
            if (desc.selected_fields.at(i)) {
                column_id_list->push_back(mapping->get_column_id_or_child_mapping(i).orc_column_id);
            }
        }

        for (size_t i = 0; i < desc.children.size(); i++) {
            if (!desc.selected_fields.at(i)) {
                continue;
            }
            const TypeDescriptor& child_type = desc.children.at(i);
            if (child_type.is_complex_type()) {
                RETURN_IF_ERROR(set_include_column_id_by_type(mapping->get_column_id_or_child_mapping(i).orc_mapping,
                                                              child_type, column_id_list));
            }
        }
    } else if (desc.is_array_type()) {
        column_id_list->push_back(mapping->get_column_id_or_child_mapping(0).orc_column_id);
        const TypeDescriptor& child_type = desc.children.at(0);
        if (child_type.is_complex_type()) {
            RETURN_IF_ERROR(set_include_column_id_by_type(mapping->get_column_id_or_child_mapping(0).orc_mapping,
                                                          child_type, column_id_list));
        }
    } else if (desc.is_map_type()) {
        if (desc.selected_fields.at(0)) {
            column_id_list->push_back(mapping->get_column_id_or_child_mapping(0).orc_column_id);
        }
        if (desc.selected_fields.at(1)) {
            column_id_list->push_back(mapping->get_column_id_or_child_mapping(1).orc_column_id);
            const TypeDescriptor& child_value_type = desc.children.at(1);
            // Only value will be complex type
            if (child_value_type.is_complex_type()) {
                RETURN_IF_ERROR(set_include_column_id_by_type(mapping->get_column_id_or_child_mapping(1).orc_mapping,
                                                              child_value_type, column_id_list));
            }
        }
    } else {
        DCHECK(false) << "Unreachable!";
    }
    return Status::OK();
}

std::unique_ptr<OrcMapping> OrcMappingFactory::build_mapping(const std::vector<SlotDescriptor*>& slot_descs,
                                                             const orc::Type& root_orc_type, const bool case_sensitve) {
    std::unique_ptr<OrcMapping> orc_mapping = std::make_unique<OrcMapping>();
    Status res = _init_orc_mapping(orc_mapping, slot_descs, root_orc_type, case_sensitve);
    if (!res.ok()) {
        LOG(WARNING) << res.to_string();
        return nullptr;
    }
    return orc_mapping;
}

Status OrcMappingFactory::_init_orc_mapping(std::unique_ptr<OrcMapping>& mapping,
                                            const std::vector<SlotDescriptor*>& slot_descs,
                                            const orc::Type& orc_root_type, const bool case_sensitve) {
    // build mapping for orc [orc field name -> pos in orc]
    std::unordered_map<std::string, size_t> orc_fieldname_2_pos;
    for (size_t i = 0; i < orc_root_type.getSubtypeCount(); i++) {
        std::string col_name = format_column_name(orc_root_type.getFieldName(i), case_sensitve);
        orc_fieldname_2_pos.emplace(col_name, i);
    }

    for (size_t i = 0; i < slot_descs.size(); i++) {
        SlotDescriptor* slot_desc = slot_descs[i];
        if (slot_desc == nullptr) continue;
        std::string col_name = format_column_name(slot_desc->col_name(), case_sensitve);
        auto it = orc_fieldname_2_pos.find(col_name);
        if (it == orc_fieldname_2_pos.end()) {
            auto s = strings::Substitute("OrcMappingFactory::_init_orc_mapping not found column name $0", col_name);
            return Status::NotFound(s);
        }

        const orc::Type& orc_sub_type = *orc_root_type.getSubtype(it->second);

        size_t need_add_column_id = orc_sub_type.getColumnId();
        OrcMappingPtr need_add_child_mapping = nullptr;

        // handle nested mapping for complex type mapping
        if (slot_desc->type().is_complex_type()) {
            need_add_child_mapping = std::make_shared<OrcMapping>();
            const TypeDescriptor& origin_type = slot_desc->type();
            RETURN_IF_ERROR(_set_child_mapping(need_add_child_mapping, origin_type, orc_sub_type, case_sensitve));
        }

        mapping->add_mapping(i, need_add_column_id, need_add_child_mapping);
    }
    return Status::OK();
}

// origin_type is TypeDescriptor in table defination
// orc_type is orc's type
Status OrcMappingFactory::_set_child_mapping(const OrcMappingPtr& mapping, const TypeDescriptor& origin_type,
                                             const orc::Type& orc_type, const bool case_sensitive) {
    DCHECK(origin_type.is_complex_type());
    if (origin_type.type == PrimitiveType::TYPE_STRUCT) {
        DCHECK(orc_type.getKind() == orc::TypeKind::STRUCT);

        std::unordered_map<std::string, size_t> tmp_orc_fieldname_2_pos;
        for (size_t i = 0; i < orc_type.getSubtypeCount(); i++) {
            std::string field_name = format_column_name(orc_type.getFieldName(i), case_sensitive);
            tmp_orc_fieldname_2_pos[field_name] = i;
        }

        for (size_t index = 0; index < origin_type.children.size(); index++) {
            std::string field_name = format_column_name(origin_type.field_names[index], case_sensitive);
            auto it = tmp_orc_fieldname_2_pos.find(field_name);
            if (it == tmp_orc_fieldname_2_pos.end()) {
                auto s = strings::Substitute(
                        "OrcChunkReader::_set_child_mapping "
                        "not found struct subfield $0",
                        field_name);
                return Status::NotFound(s);
            }
            const orc::Type& orc_child_type = *orc_type.getSubtype(it->second);

            size_t need_add_column_id = orc_child_type.getColumnId();
            OrcMappingPtr need_add_child_mapping = nullptr;

            const TypeDescriptor& origin_child_type = origin_type.children[index];
            if (origin_child_type.is_complex_type()) {
                need_add_child_mapping = std::make_shared<OrcMapping>();
                RETURN_IF_ERROR(
                        _set_child_mapping(need_add_child_mapping, origin_child_type, orc_child_type, case_sensitive));
            }
            mapping->add_mapping(index, need_add_column_id, need_add_child_mapping);
        }
    } else if (origin_type.type == PrimitiveType::TYPE_ARRAY) {
        DCHECK(orc_type.getKind() == orc::TypeKind::LIST);
        const TypeDescriptor& origin_child_type = origin_type.children[0];
        const orc::Type& orc_child_type = *orc_type.getSubtype(0);

        size_t need_add_column_id = orc_child_type.getColumnId();
        OrcMappingPtr need_add_child_mapping = nullptr;

        if (origin_child_type.is_complex_type()) {
            need_add_child_mapping = std::make_shared<OrcMapping>();
            RETURN_IF_ERROR(
                    _set_child_mapping(need_add_child_mapping, origin_child_type, orc_child_type, case_sensitive));
        }

        mapping->add_mapping(0, need_add_column_id, need_add_child_mapping);
    } else if (origin_type.type == PrimitiveType::TYPE_MAP) {
        DCHECK(orc_type.getKind() == orc::TypeKind::MAP);

        // Map's key must be primitivte type
        mapping->add_mapping(0, orc_type.getSubtype(0)->getColumnId(), nullptr);

        // Use for map's value
        size_t need_add_value_column_id = orc_type.getSubtype(1)->getColumnId();
        OrcMappingPtr need_add_vaule_child_mapping = nullptr;

        // Map's key must be primitive type, so we only consider value.
        const TypeDescriptor& origin_child_type = origin_type.children[1];

        if (origin_child_type.is_complex_type()) {
            need_add_vaule_child_mapping = std::make_shared<OrcMapping>();
            const orc::Type& orc_child_type = *orc_type.getSubtype(1);
            RETURN_IF_ERROR(_set_child_mapping(need_add_vaule_child_mapping, origin_child_type, orc_child_type,
                                               case_sensitive));
        }
        mapping->add_mapping(1, need_add_value_column_id, need_add_vaule_child_mapping);
    }
    return Status::OK();
}

} // namespace starrocks::vectorized