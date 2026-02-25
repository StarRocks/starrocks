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

#include "column/copied_datum.h"
#include "column/datum_convert.h"
#include "fmt/format.h"
#include "storage/types.h"
#include "types/type_descriptor.h"

namespace starrocks {

class DatumVariant {
public:
    DatumVariant() = default;

    DatumVariant(const TypeInfoPtr& type, const Datum& value) : _type(type), _value(value) {}

    const TypeInfoPtr& type() const { return _type; }

    const Datum& value() const { return _value.get(); }

    int compare(const DatumVariant& other) const {
        DCHECK(_type->type() == other._type->type());
        return _type->cmp(_value.get(), other._value.get());
    }

    void to_proto(VariantPB* variant_pb) const;

    Status from_proto(const VariantPB& variant_pb);

    static Status from_proto(const VariantPB& variant_pb, Datum* dest_datum, TypeDescriptor* dest_type_desc = nullptr,
                             TypeInfoPtr* dest_type_info = nullptr, MemPool* mem_pool = nullptr);

private:
    TypeInfoPtr _type;
    CopiedDatum _value;
};

inline void DatumVariant::to_proto(VariantPB* variant_pb) const {
    *variant_pb->mutable_type() = TypeDescriptor::from_storage_type_info(_type.get()).to_protobuf();
    if (_value.get().is_null()) {
        variant_pb->set_variant_type(VariantTypePB::NULL_VALUE);
        return;
    }
    variant_pb->set_variant_type(VariantTypePB::NORMAL_VALUE);
    *variant_pb->mutable_value() = datum_to_string(_type.get(), _value.get());
}

inline Status DatumVariant::from_proto(const VariantPB& variant_pb) {
    if (!variant_pb.has_type()) {
        return Status::InvalidArgument("No type in variant");
    }
    Datum datum;
    RETURN_IF_ERROR(from_proto(variant_pb, &datum, nullptr, &_type));
    _value.set(datum);
    return Status::OK();
}

inline Status DatumVariant::from_proto(const VariantPB& variant_pb, Datum* dest_datum, TypeDescriptor* dest_type_desc,
                                       TypeInfoPtr* dest_type_info, MemPool* mem_pool) {
    if (!variant_pb.has_type()) {
        return Status::InvalidArgument("No type in variant");
    }

    auto type_desc = TypeDescriptor::from_protobuf(variant_pb.type());
    auto type_info = get_type_info(type_desc);
    if (type_info == nullptr) {
        return Status::InternalError(fmt::format("Unsupported type: {}", variant_pb.type().DebugString()));
    }

    if (variant_pb.variant_type() == VariantTypePB::NULL_VALUE) {
        dest_datum->set_null();
    } else if (variant_pb.variant_type() == VariantTypePB::NORMAL_VALUE && variant_pb.has_value()) {
        RETURN_IF_ERROR(datum_from_string(type_info.get(), dest_datum, variant_pb.value(), mem_pool));
    } else {
        return Status::InvalidArgument("Invalid variant value");
    }

    if (dest_type_desc != nullptr) {
        *dest_type_desc = std::move(type_desc);
    }
    if (dest_type_info != nullptr) {
        *dest_type_info = std::move(type_info);
    }
    return Status::OK();
}

} // namespace starrocks
