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
#include "runtime/types.h"
#include "storage/types.h"

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

private:
    TypeInfoPtr _type;
    CopiedDatum _value;
};

inline void DatumVariant::to_proto(VariantPB* variant_pb) const {
    *variant_pb->mutable_type() = TypeDescriptor::from_storage_type_info(_type.get()).to_protobuf();
    if (_value.get().is_null()) {
        return;
    }
    *variant_pb->mutable_value() = datum_to_string(_type.get(), _value.get());
}

inline Status DatumVariant::from_proto(const VariantPB& variant_pb) {
    if (!variant_pb.has_type()) {
        return Status::InvalidArgument("No type in variant");
    }
    _type = get_type_info(TypeDescriptor::from_protobuf(variant_pb.type()));
    Datum datum;
    if (!variant_pb.has_value()) {
        datum.set_null();
    } else {
        RETURN_IF_ERROR(datum_from_string(_type.get(), &datum, variant_pb.value(), nullptr));
    }
    _value.set(datum);
    return Status::OK();
}

} // namespace starrocks
