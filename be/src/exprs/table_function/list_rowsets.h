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

#include <memory>

#include "exprs/table_function/table_function.h"

namespace starrocks {

namespace lake {
class TabletMetadataPB;
}

class ListRowsets final : public TableFunction {
    struct MyState final : public TableFunctionState {
        std::shared_ptr<const lake::TabletMetadataPB> metadata;

        ~MyState() override = default;

        void on_new_params() override { set_offset(0); }
    };

public:
    [[nodiscard]] Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new MyState();
        return Status::OK();
    }

    [[nodiscard]] Status prepare(TableFunctionState* /*state*/) const override { return Status::OK(); }

    [[nodiscard]] Status open(RuntimeState* /*runtime_state*/, TableFunctionState* /*state*/) const override {
        return Status::OK();
    }

    [[nodiscard]] Status close(RuntimeState* /*runtime_state*/, TableFunctionState* state) const override {
        delete state;
        return Status::OK();
    }

    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* base_state) const override;
};

} // namespace starrocks
