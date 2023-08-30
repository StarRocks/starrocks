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

#include "service/backend_base.h"

namespace starrocks {

// This class just forward rpc requests to actual handlers, used
// to bind multiple services on single port.
class ComputeService : public BackendServiceBase {
public:
<<<<<<< HEAD:be/src/service/service_cn/compute_service.h
    explicit ComputeService(ExecEnv* exec_env);

    ~ComputeService() override;
=======
    Status write_string(OutputStream* os, const Column& column, size_t row_num, const Options& options) const override;
    Status write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                               const Options& options) const override;
    bool read_string(Column* column, const Slice& s, const Options& options) const override;
    bool read_quoted_string(Column* column, const Slice& s, const Options& options) const override;
>>>>>>> 9e1acf2f39 ([Enhancement] Improve data lake csv reader performance further (#30137)):be/src/formats/csv/varbinary_converter.h
};

} // namespace starrocks
