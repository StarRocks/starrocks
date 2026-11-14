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

#include "exprs/function_helper.h"

namespace starrocks {

class FileFunctions {
public:
    DEFINE_VECTORIZED_FN(to_file);
    DEFINE_VECTORIZED_FN(try_to_file);
    DEFINE_VECTORIZED_FN(fl_get_uri);
    DEFINE_VECTORIZED_FN(fl_get_offset);
    DEFINE_VECTORIZED_FN(fl_get_size);
    DEFINE_VECTORIZED_FN(fl_get_content_type);
    DEFINE_VECTORIZED_FN(fl_get_checksum);
    DEFINE_VECTORIZED_FN(fl_get_inline);

    // get_presigned_url(storage_volume, absolute_uri [, expiration_seconds]) -> VARCHAR
    // get_presigned_file(storage_volume, file [, expiration_seconds]) -> FILE
    // The storage volume is resolved by the FE, which sends its cloud configuration with the function; the
    // argument itself is not read here. The overloads with an expiration time have their own entry points so
    // that the trailing argument the planner appends to non-deterministic calls is never mistaken for one.
    // They keep no function state: each batch presigns through the file system of that cloud configuration.
    DEFINE_VECTORIZED_FN(get_presigned_url);
    DEFINE_VECTORIZED_FN(get_presigned_url_with_expiration);
    DEFINE_VECTORIZED_FN(get_presigned_file);
    DEFINE_VECTORIZED_FN(get_presigned_file_with_expiration);
};

} // namespace starrocks
