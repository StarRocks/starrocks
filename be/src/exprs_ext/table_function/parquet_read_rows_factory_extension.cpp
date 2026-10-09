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

#include "exprs/table_function/table_function_factory.h"
#include "exprs_ext/table_function/parquet_read_rows.h"

namespace starrocks {

namespace {

struct ParquetReadRowsFactoryExtensionRegistrar {
    ParquetReadRowsFactoryExtensionRegistrar() {
        register_builtin_table_function("parquet_read_rows", {TYPE_JSON}, {TYPE_VARCHAR, TYPE_BIGINT, TYPE_JSON},
                                        std::make_shared<ParquetReadRows>());
    }
};

ParquetReadRowsFactoryExtensionRegistrar k_parquet_read_rows_factory_extension_registrar;

} // namespace

} // namespace starrocks
