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

#include "common/status.h"
#include "common/statusor.h"
#include "storage/index/inverted/clucene/clucene_inverted_reader.h"
#include "storage/index/inverted/clucene/clucene_inverted_writer.h"
#include "storage/index/inverted/inverted_plugin.h"

namespace starrocks {

class CLucenePlugin : public InvertedPlugin {
public:
    static CLucenePlugin& get_instance() {
        static CLucenePlugin instance;
        return instance;
    }

    static bool is_index_files(const std::string& file) {
        return (file.find(".fdt", 0) != std::string::npos || file.find(".fdx", 0) != std::string::npos ||
                file.find(".fnm", 0) != std::string::npos || file.find(".frq", 0) != std::string::npos ||
                file.find(".nrm", 0) != std::string::npos || file.find(".prx", 0) != std::string::npos ||
                file.find(".tii", 0) != std::string::npos || file.find(".tis", 0) != std::string::npos ||
                file.find("null_bitmap", 0) != std::string::npos || file.find("segments", 0) != std::string::npos);
    }

    CLucenePlugin(CLucenePlugin const&) = delete;
    void operator=(CLucenePlugin const&) = delete;

    Status create_inverted_index_writer(TypeInfoPtr typeinfo, std::string field_name, std::string path,
                                        TabletIndex* tablet_index, std::unique_ptr<InvertedWriter>* res) override;

    Status create_inverted_index_reader(std::string path, const std::shared_ptr<TabletIndex>& tablet_index,
                                        LogicalType field_type, std::unique_ptr<InvertedReader>* res) override;

private:
    CLucenePlugin() {}
};

} // namespace starrocks