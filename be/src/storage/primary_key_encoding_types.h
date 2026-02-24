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

namespace starrocks {

/*
 * PrimaryKeyEncodingType is used to specify the encoding type of primary key.
 * V1: encode the primary key as original way.
 * V2: always encode the primary key as big endian.
 *
 * We introduce this enum class to support compatibility with existing code.
 * In general, we should encode the primary key using V2 which can
 * preserve the key order after encoding. But for the historical reasons,
 * if the there is only one pk column and it is not string type, it will
 * not be encoded as big endian. For compatibility, we need to support the
 * V1 encoding type to keep the original way.
 *
 * Currently, we only support V2 encoding type for range-distribution table
 * in share data mode.
 *
 * PK_ENCODING_TYPE_NONE is not a valid encoding type, it is used to indicate for
 * the non-primary key tablet for the compatibility issue and should not be used
 * in any primary key encode context.
*/
enum class PrimaryKeyEncodingType {
    PK_ENCODING_TYPE_NONE = 0,
    PK_ENCODING_TYPE_V1 = 1,
    PK_ENCODING_TYPE_V2 = 2,
};

} // namespace starrocks
