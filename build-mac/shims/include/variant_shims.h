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

#pragma once

#include <cstdint>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>

// Forward declare global StringPiece and cctz::time_zone used throughout BE
class StringPiece;
namespace cctz {
class time_zone;
}

namespace starrocks {

// Forward declarations for BE types used by shim implementations
class Slice;                // be/src/util/slice.h
class DateValue;            // be/src/types/date_value.h
class DecimalV2Value;       // be/src/runtime/decimalv2_value.h

// Some projects use a UUID value type. Provide a minimal stub to satisfy linkage
class UuidValue {
public:
    UuidValue() = default;
    ~UuidValue() = default;
};

// Forward declarations for Thrift-generated types/enums under namespace starrocks
struct TStatusCode;
struct SampleMethod;
struct TExprNodeType;
struct TPartitionType;
struct TFileFormatType;
struct TResultSinkType;
struct TCompressionType;
struct TTransactionStatus;
struct TJoinDistributionMode;
struct TJoinOp;
struct TTxnType;
struct TLoadType;
struct TRuntimeFilterLayoutMode;
struct MVTaskType;
struct TAccessPathType;
struct TLoadSourceType;

class TDateLiteral;
class TPlanFragment;
class TNetworkAddress;
class TTabletLocation;
class TRuntimeFilterParams;
class TPartitionVersionInfo;
class TCreatePartitionResult;
class TCreatePartitionRequest;
class TGetLoadTxnStatusResult;
class TRuntimeFilterProberParams;
class TMVEpoch;
class TUniqueId;
class TImmutablePartitionResult;
class TImmutablePartitionRequest;

// Lightweight ref-counting base types used by EasyJson and others
namespace subtle {
class RefCountedBase {
public:
    virtual ~RefCountedBase();
    virtual void AddRef() const = 0;
    virtual void Release() const = 0;
};

class RefCountedThreadSafeBase {
public:
    virtual ~RefCountedThreadSafeBase();
    virtual void AddRef() const = 0;
    virtual void Release() const = 0;
};
} // namespace subtle

// Minimal Variant family used by some utility/debug code paths
enum class VariantType { null_type };

class VariantMetadata {
public:
    std::string get_key(uint32_t id) const;
};

class EasyJson {
public:
    enum ComplexTypeInitializer { kObject, kArray };

    EasyJson();
    explicit EasyJson(ComplexTypeInitializer);
    ~EasyJson();

    EasyJson Get(const std::string& key);
    EasyJson Get(int index);
    EasyJson operator[](const std::string& key);
    EasyJson operator[](int index);

    EasyJson& operator=(const std::string& val);
    template <typename T>
    EasyJson& operator=(T) {
        return *this;
    }

    EasyJson& SetObject();
    EasyJson& SetArray();
    EasyJson Set(const std::string& key, const std::string& val);
    template <typename T>
    EasyJson Set(const std::string& key, T) {
        return EasyJson();
    }

    EasyJson Set(int index, const std::string& val);
    template <typename T>
    EasyJson Set(int index, T) {
        return EasyJson();
    }

    EasyJson PushBack(const std::string& val);
    template <typename T>
    EasyJson PushBack(T) {
        return EasyJson();
    }

    std::string ToString() const;
};

class VariantUtil {
public:
    static std::string variant_to_json(std::string_view key, std::string_view path, std::stringstream& out,
                                       cctz::time_zone tz);
};

class GeoPoint {
public:
    GeoPoint();
    ~GeoPoint();
    double x() const;
    double y() const;
};

class Variant {
public:
    Variant();
    Variant(const VariantMetadata& metadata, std::string_view value);
    ~Variant();

    VariantType type() const;

    bool get_bool() const;
    int8_t get_int8() const;
    int16_t get_int16() const;
    int32_t get_int32() const;
    int64_t get_int64() const;
    float get_float() const;
    double get_double() const;
    std::string get_string() const;
    Slice get_binary() const;
    DecimalV2Value get_decimal4() const;
    DecimalV2Value get_decimal8() const;
    DecimalV2Value get_decimal16() const;
    int64_t get_timestamp_micros() const;
    int64_t get_timestamp_micros_ntz() const;
    DateValue get_date() const;
    UuidValue get_uuid() const;
    VariantMetadata metadata() const;

private:
    VariantType _type{VariantType::null_type};
};

} // namespace starrocks
