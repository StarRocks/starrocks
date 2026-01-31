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

namespace starrocks {

class CopiedDatum {
public:
    CopiedDatum() = default;

    CopiedDatum(const Datum& datum) : _datum(datum) { copy(); }

    CopiedDatum(const CopiedDatum& other) : CopiedDatum(other.get()) {}

    CopiedDatum(CopiedDatum&& other) noexcept : _datum(other.get()) { other._datum.set_null(); }

    void set(const Datum& datum) {
        release();
        _datum = datum;
        copy();
    }

    const Datum& get() const { return _datum; }

    CopiedDatum& operator=(const CopiedDatum& other) {
        if (this != &other) {
            set(other._datum);
        }
        return *this;
    }

    CopiedDatum& operator=(CopiedDatum&& other) noexcept {
        if (this != &other) {
            release();
            _datum = other._datum;
            other._datum.set_null();
        }
        return *this;
    }

    ~CopiedDatum() noexcept { release(); }

private:
    void copy() {
        _datum.visit([this](const auto& variant) {
            std::visit(overloaded{[](const std::monostate& arg) {}, [](const int8_t& arg) {}, [](const uint8_t& arg) {},
                                  [](const int16_t& arg) {}, [](const uint16_t& arg) {}, [](const uint24_t& arg) {},
                                  [](const int32_t& arg) {}, [](const uint32_t& arg) {}, [](const int64_t& arg) {},
                                  [](const uint64_t& arg) {}, [](const int96_t& arg) {}, [](const int128_t& arg) {},
                                  [](const int256_t& arg) {},
                                  [this](const Slice& value) {
                                      if (value.empty()) {
                                          return;
                                      }
                                      char* new_str = new char[value.size];
                                      ::memcpy(new_str, value.data, value.size);
                                      _datum.set(Slice(new_str, value.size));
                                  },
                                  [](const decimal12_t& arg) {}, [](const DecimalV2Value& arg) {},
                                  [](const float& arg) {}, [](const double& arg) {},
                                  [](const auto& value) { DCHECK(false) << "Unsupported datum type"; }},
                       variant);
        });
    }

    void release() noexcept {
        _datum.visit([](const auto& variant) {
            std::visit(overloaded{[](const std::monostate& arg) {}, [](const int8_t& arg) {}, [](const uint8_t& arg) {},
                                  [](const int16_t& arg) {}, [](const uint16_t& arg) {}, [](const uint24_t& arg) {},
                                  [](const int32_t& arg) {}, [](const uint32_t& arg) {}, [](const int64_t& arg) {},
                                  [](const uint64_t& arg) {}, [](const int96_t& arg) {}, [](const int128_t& arg) {},
                                  [](const int256_t& arg) {},
                                  [](const Slice& value) {
                                      if (value.empty()) {
                                          return;
                                      }
                                      delete[] value.data;
                                  },
                                  [](const decimal12_t& arg) {}, [](const DecimalV2Value& arg) {},
                                  [](const float& arg) {}, [](const double& arg) {}, [](const auto& arg) {}},
                       variant);
        });
    }

private:
    Datum _datum;
};

} // namespace starrocks
