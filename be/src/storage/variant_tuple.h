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

#include <vector>

#include "storage/datum_variant.h"

namespace starrocks {

class VariantTuple {
public:
    VariantTuple() = default;

    bool empty() const { return _values.empty(); }

    size_t size() const { return _values.size(); }

    void reserve(size_t n) { _values.reserve(n); }

    void append(const DatumVariant& value) { _values.push_back(value); }

    void append(DatumVariant&& value) { _values.push_back(std::move(value)); }

    template <typename... Args>
    void emplace(Args&&... args) {
        _values.emplace_back(std::forward<Args>(args)...);
    }

    const DatumVariant& operator[](size_t n) const { return _values[n]; }

    const DatumVariant& get(size_t n) const { return _values.at(n); }

    DatumVariant& operator[](size_t n) { return _values[n]; }

    DatumVariant& get(size_t n) { return _values.at(n); }

    const std::vector<DatumVariant>& values() const { return _values; }

    void clear() { _values.clear(); }

    int compare(const VariantTuple& other) const;

    void to_proto(TuplePB* tuple_pb) const;

    Status from_proto(const TuplePB& tuple_pb);

private:
    std::vector<DatumVariant> _values;
};

inline int VariantTuple::compare(const VariantTuple& other) const {
    auto this_len = this->_values.size();
    auto other_len = other._values.size();
    auto min_len = std::min(this_len, other_len);
    for (auto i = 0; i < min_len; ++i) {
        int ret = this->_values[i].compare(other._values[i]);
        if (0 != ret) {
            return ret;
        }
    }
    if (this_len < other_len) {
        return -1;
    } else if (this_len > other_len) {
        return 1;
    } else {
        return 0;
    }
}

inline void VariantTuple::to_proto(TuplePB* tuple_pb) const {
    tuple_pb->clear_values();
    for (const auto& value : _values) {
        value.to_proto(tuple_pb->add_values());
    }
}

inline Status VariantTuple::from_proto(const TuplePB& tuple_pb) {
    auto size = tuple_pb.values_size();
    _values.resize(size);
    for (auto i = 0; i < size; ++i) {
        RETURN_IF_ERROR(_values[i].from_proto(tuple_pb.values(i)));
    }
    return Status::OK();
}

} // namespace starrocks
