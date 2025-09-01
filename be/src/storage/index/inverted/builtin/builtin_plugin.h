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

#include "storage/index/inverted/inverted_plugin.h"

namespace starrocks {

class BuiltinPlugin : public InvertedPlugin {
public:
    static BuiltinPlugin& get_instance() {
        static BuiltinPlugin instance;
        return instance;
    }

    BuiltinPlugin(BuiltinPlugin const&) = delete;
    void operator=(BuiltinPlugin const&) = delete;

    Status create_inverted_index_writer(TypeInfoPtr typeinfo, [[maybe_unused]] std::string field_name, [[maybe_unused]] std::string path,
                                        TabletIndex* tablet_index, std::unique_ptr<InvertedWriter>* res) override;

    Status create_inverted_index_reader(std::string path, const std::shared_ptr<TabletIndex>& tablet_index,
                                        LogicalType field_type, std::unique_ptr<InvertedReader>* res) override;

private:
    BuiltinPlugin() {}
};

} // namespace starrocks