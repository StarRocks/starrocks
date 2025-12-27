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

#include "column/datum.h"
#include "column/datum_convert.h"
#include "runtime/types.h"
#include "storage/types.h"

namespace starrocks {

class DatumVariant {
public:
    DatumVariant() = default;

    DatumVariant(const TypeDescriptor& type, const Datum& value) : _type(type), _value(value) {}

    const TypeDescriptor& type() const { return _type; }

    const Datum& value() const { return _value; }

    int compare(const DatumVariant& other) const { return get_type_info(_type)->cmp(_value, other._value); }

    void to_proto(VariantPB* variantPB) const;

    Status from_proto(const VariantPB& variantPB);

private:
    TypeDescriptor _type;
    Datum _value;
};

inline void DatumVariant::to_proto(VariantPB* variantPB) const {
    *variantPB->mutable_type() = _type.to_protobuf();
    if (_value.is_null()) {
        return;
    }
    *variantPB->mutable_value() = datum_to_string(get_type_info(_type).get(), _value);
}

inline Status DatumVariant::from_proto(const VariantPB& variantPB) {
    if (!variantPB.has_type()) {
        return Status::InvalidArgument("No type in variant");
    }
    _type = TypeDescriptor::from_protobuf(variantPB.type());
    if (!variantPB.has_value()) {
        _value.set_null();
    } else {
        RETURN_IF_ERROR(datum_from_string(get_type_info(_type).get(), &_value, variantPB.value(), nullptr));
    }
    return Status::OK();
}

} // namespace starrocks
