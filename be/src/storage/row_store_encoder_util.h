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

#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "gutil/endian.h"
#include "gutil/stringprintf.h"
#include "storage/primary_key_encoder.h"
#include "types/bitmap_value.h"

namespace starrocks {
//declare and defines of template class must be in a file, so we exctract it here

// Note: to_bigendian and encode_integral functions are now available in
// storage/primary_key_encoder.h under the encoding_utils namespace

template <class T>
inline void decode_integral(Slice* src, T* v) {
    if constexpr (std::is_signed<T>::value) {
        typedef typename std::make_unsigned<T>::type UT;
        UT uv = *(UT*)(src->data);
        uv = encoding_utils::to_bigendian(uv);
        uv ^= static_cast<UT>(1) << (sizeof(UT) * 8 - 1);
        *v = uv;
    } else {
        T nv = *(T*)(src->data);
        *v = encoding_utils::to_bigendian(nv);
    }
    src->remove_prefix(sizeof(T));
}

} // namespace starrocks