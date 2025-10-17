// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "variant_shims.h"

#include <sstream>

// Bring in concrete types used by shim return values
#include "util/slice.h"
#include "runtime/decimalv2_value.h"
#include "types/date_value.h"

namespace starrocks {

// Variant implementations
Variant::Variant() : _type(VariantType::null_type) {}

Variant::Variant(const VariantMetadata& metadata, std::string_view value) : _type(VariantType::null_type) {
    // Empty implementation for macOS shim
}

Variant::~Variant() = default;

VariantType Variant::type() const {
    return _type;
}

bool Variant::get_bool() const {
    return false;
}

int8_t Variant::get_int8() const {
    return 0;
}

int16_t Variant::get_int16() const {
    return 0;
}

int32_t Variant::get_int32() const {
    return 0;
}

int64_t Variant::get_int64() const {
    return 0;
}

float Variant::get_float() const {
    return 0.0f;
}

double Variant::get_double() const {
    return 0.0;
}

std::string Variant::get_string() const {
    return "";
}

Slice Variant::get_binary() const {
    return Slice();
}

DecimalV2Value Variant::get_decimal4() const {
    return DecimalV2Value();
}

DecimalV2Value Variant::get_decimal8() const {
    return DecimalV2Value();
}

DecimalV2Value Variant::get_decimal16() const {
    return DecimalV2Value();
}

int64_t Variant::get_timestamp_micros() const {
    return 0;
}

int64_t Variant::get_timestamp_micros_ntz() const {
    return 0;
}

DateValue Variant::get_date() const {
    return DateValue();
}

UuidValue Variant::get_uuid() const {
    return UuidValue();
}

VariantMetadata Variant::metadata() const {
    return VariantMetadata();
}

// VariantMetadata implementations
std::string VariantMetadata::get_key(uint32_t id) const {
    return "key_" + std::to_string(id);
}

// GeoPoint implementations
GeoPoint::GeoPoint() = default;
GeoPoint::~GeoPoint() = default;

double GeoPoint::x() const {
    return 0.0;
}

double GeoPoint::y() const {
    return 0.0;
}

// Note: All Thrift-related to_string() and operator<< overloads are provided by
// generated code in gen_cpp. We intentionally avoid defining them here to
// prevent duplicate symbol/linker conflicts on macOS.

// ProcessVarAccessor shim for missing symbol
class ProcessVarAccessor {
public:
    void* function() {
        // Return dummy function pointer for macOS
        return nullptr;
    }
};

// Global instance to ensure symbol is available
ProcessVarAccessor g_process_var_accessor;

} // namespace starrocks
