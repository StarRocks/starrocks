// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/orc/orc_mapping.h"

namespace starrocks::vectorized {

std::unique_ptr<OrcMapping> OrcMapping::build_mapping(const std::vector<SlotDescriptor*>& slot_descs,
                                                      const orc::Type& root_orc_type, const bool case_sensitve) {
    std::unique_ptr<OrcMapping> orc_mapping = std::make_unique<OrcMapping>();
    Status res = _init_orc_mapping(orc_mapping, slot_descs, root_orc_type, case_sensitve);
    if (!res.ok()) {
        LOG(WARNING) << res.to_string();
        return nullptr;
    }
    return orc_mapping;
}

size_t OrcMapping::get_column_id(size_t src_pos) {
    auto it = _mapping.find(src_pos);
    if (it == _mapping.end()) {
        DCHECK(false);
        // return large positive invalid number
        return -1;
    }
    return it->second;
}

const OrcMappingPtr OrcMapping::get_child_mapping(size_t src_pos) {
    auto it = _children.find(src_pos);
    if (it == _children.end()) {
        return nullptr;
    }
    return it->second;
}

void OrcMapping::add_mapping(size_t pos_in_src, size_t orc_column_id) {
    _mapping.emplace(pos_in_src, orc_column_id);
}

void OrcMapping::add_child(size_t pos_in_src, const OrcMappingPtr& child_mapping) {
    _children.emplace(pos_in_src, child_mapping);
}

void OrcMapping::clear() {
    _mapping.clear();
    _children.clear();
}

Status OrcMapping::_init_orc_mapping(std::unique_ptr<OrcMapping>& mapping,
                                     const std::vector<SlotDescriptor*>& slot_descs, const orc::Type& orc_root_type,
                                     const bool case_sensitve) {
    // build mapping for orc [orc field name -> pos in orc]
    std::unordered_map<std::string, size_t> orc_fieldname_2_pos;
    for (size_t i = 0; i < orc_root_type.getSubtypeCount(); i++) {
        std::string col_name = format_column_name(orc_root_type.getFieldName(i), case_sensitve);
        orc_fieldname_2_pos.emplace(col_name, i);
    }

    for (size_t i = 0; i < slot_descs.size(); i++) {
        SlotDescriptor* slot_desc = slot_descs[i];
        std::string col_name = format_column_name(slot_desc->col_name(), case_sensitve);
        auto it = orc_fieldname_2_pos.find(col_name);
        if (it == orc_fieldname_2_pos.end()) {
            auto s = strings::Substitute("OrcMapping::_init_orc_mapping not found column name $0", col_name);
            return Status::NotFound(s);
        }

        const orc::Type& orc_sub_type = *orc_root_type.getSubtype(it->second);
        mapping->add_mapping(i, orc_sub_type.getColumnId());

        // handle nested mapping for complex type mapping
        if (slot_desc->type().is_complex_type()) {
            OrcMappingPtr child_mapping = std::make_shared<OrcMapping>();
            mapping->add_child(i, child_mapping);
            const TypeDescriptor& origin_type = slot_desc->type();
            RETURN_IF_ERROR(_set_child_mapping(child_mapping, origin_type, orc_sub_type, case_sensitve));
        }
    }
    return Status::OK();
}

// origin_type is TypeDescriptor in table defination
// orc_type is orc's type
Status OrcMapping::_set_child_mapping(const OrcMappingPtr& mapping, const TypeDescriptor& origin_type,
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
            mapping->add_mapping(index, orc_child_type.getColumnId());

            const TypeDescriptor& origin_child_type = origin_type.children[index];
            if (origin_child_type.is_complex_type()) {
                OrcMappingPtr child_mapping = std::make_shared<OrcMapping>();
                mapping->add_child(index, child_mapping);
                RETURN_IF_ERROR(_set_child_mapping(child_mapping, origin_child_type, orc_child_type, case_sensitive));
            }
        }
    } else if (origin_type.type == PrimitiveType::TYPE_ARRAY) {
        DCHECK(orc_type.getKind() == orc::TypeKind::LIST);
        const TypeDescriptor& origin_child_type = origin_type.children[0];
        const orc::Type& orc_child_type = *orc_type.getSubtype(0);

        mapping->add_mapping(0, orc_child_type.getColumnId());

        if (origin_child_type.is_complex_type()) {
            OrcMappingPtr child_mapping = std::make_shared<OrcMapping>();
            mapping->add_child(0, child_mapping);
            RETURN_IF_ERROR(_set_child_mapping(child_mapping, origin_child_type, orc_child_type, case_sensitive));
        }
    } else if (origin_type.type == PrimitiveType::TYPE_MAP) {
        DCHECK(orc_type.getKind() == orc::TypeKind::MAP);

        mapping->add_mapping(0, orc_type.getSubtype(0)->getColumnId());
        mapping->add_mapping(1, orc_type.getSubtype(1)->getColumnId());

        // Map's key must be primitive type, so we only consider value.
        const TypeDescriptor& origin_child_type = origin_type.children[1];

        if (origin_child_type.is_complex_type()) {
            OrcMappingPtr child_mapping = std::make_shared<OrcMapping>();
            mapping->add_child(1, child_mapping);
            const orc::Type& orc_child_type = *orc_type.getSubtype(1);
            RETURN_IF_ERROR(_set_child_mapping(child_mapping, origin_child_type, orc_child_type, case_sensitive));
        }
    }
    return Status::OK();
}

Status OrcMapping::set_include_column_id(const uint64_t slot_pos, const TypeDescriptor& desc,
                                         std::list<uint64_t>* column_id_list) {
    column_id_list->push_back(get_column_id(slot_pos));

    // Only complex type need include it's nested level.
    if (desc.is_complex_type()) {
        RETURN_IF_ERROR(set_include_column_id_by_type(get_child_mapping(slot_pos), desc, column_id_list));
    }
    return Status::OK();
}

Status OrcMapping::set_include_column_id_by_type(const OrcMappingPtr mapping, const TypeDescriptor& desc,
                                                 std::list<uint64_t>* column_id_list) {
    DCHECK(mapping != nullptr);
    DCHECK(desc.is_complex_type());
    if (desc.is_struct_type()) {
        for (size_t i = 0; i < desc.children.size(); i++) {
            // only include selected fields
            if (desc.selected_fields.at(i)) {
                column_id_list->push_back(mapping->get_column_id(i));
            }
        }

        for (size_t i = 0; i < desc.children.size(); i++) {
            if (!desc.selected_fields.at(i)) {
                continue;
            }
            const TypeDescriptor& child_type = desc.children.at(i);
            if (child_type.is_complex_type()) {
                RETURN_IF_ERROR(
                        set_include_column_id_by_type(mapping->get_child_mapping(i), child_type, column_id_list));
            }
        }
    } else if (desc.is_array_type()) {
        column_id_list->push_back(mapping->get_column_id(0));
        const TypeDescriptor& child_type = desc.children.at(0);
        if (child_type.is_complex_type()) {
            RETURN_IF_ERROR(set_include_column_id_by_type(mapping->get_child_mapping(0), child_type, column_id_list));
        }
    } else if (desc.is_map_type()) {
        if (desc.selected_fields.at(0)) {
            column_id_list->push_back(mapping->get_column_id(0));
        }
        if (desc.selected_fields.at(1)) {
            column_id_list->push_back(mapping->get_column_id(1));
            const TypeDescriptor& child_value_type = desc.children.at(1);
            // Only value will be complex type
            if (child_value_type.is_complex_type()) {
                RETURN_IF_ERROR(
                        set_include_column_id_by_type(mapping->get_child_mapping(1), child_value_type, column_id_list));
            }
        }
    } else {
        DCHECK(false) << "Unreachable!";
    }
    return Status::OK();
}

std::string OrcMapping::format_column_name(const std::string& col_name, bool case_sensitive) {
    return case_sensitive ? col_name : boost::algorithm::to_lower_copy(col_name);
}

} // namespace starrocks::vectorized