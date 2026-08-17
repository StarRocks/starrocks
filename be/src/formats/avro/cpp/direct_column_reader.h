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

#include <cctz/time_zone.h>

#include <avrocpp/Node.hh>
#include <avrocpp/Types.hh>
#include <avrocpp/ValidSchema.hh>
#include <memory>
#include <string_view>

#include "common/status.h"
#include "types/type_descriptor.h"

namespace avro {
class Decoder;
}

namespace starrocks {

class AdaptiveNullableColumn;

namespace avrocpp {

class DirectColumnReader;
using DirectColumnReaderUniquePtr = std::unique_ptr<DirectColumnReader>;

class DirectColumnReader {
public:
    virtual ~DirectColumnReader() = default;

    static DirectColumnReaderUniquePtr make(std::string_view col_name, const TypeDescriptor& type_desc,
                                            const avro::NodePtr& node, const cctz::time_zone& timezone);

    static void skip_node(avro::Decoder& decoder, const avro::NodePtr& node);

    virtual Status read_field(avro::Decoder& decoder, AdaptiveNullableColumn* column) = 0;
};

} // namespace avrocpp
} // namespace starrocks
