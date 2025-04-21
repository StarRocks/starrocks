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

#include "formats/avro/cpp/column_reader.h"

namespace starrocks::avrocpp {

class NullableColumnReader final : public ColumnReader {
public:
    explicit NullableColumnReader(ColumnReaderUniquePtr base_reader, bool invalid_as_null)
            : ColumnReader("", TypeDescriptor()),
              _base_reader(std::move(base_reader)),
              _invalid_as_null(invalid_as_null) {}
    ~NullableColumnReader() override = default;

    Status read_datum_for_adaptive_column(const avro::GenericDatum& datum, Column* column) override;
    Status read_datum(const avro::GenericDatum& datum, Column* column) override;

private:
    ColumnReaderUniquePtr _base_reader;
    bool _invalid_as_null;
};

} // namespace starrocks::avrocpp
