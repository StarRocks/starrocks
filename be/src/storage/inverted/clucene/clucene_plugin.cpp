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

#include "clucene_plugin.h"

#include "types/array_type_info.h"

namespace starrocks {

Status CLucenePlugin::create_inverted_index_writer(TypeInfoPtr typeinfo, std::string field_name, std::string directory,
                                                   TabletIndex* tablet_index, std::unique_ptr<InvertedWriter>* res) {
    return CLuceneInvertedWriter::create(typeinfo, field_name, directory, tablet_index, res);
}

Status CLucenePlugin::create_inverted_index_reader(std::string path, const std::shared_ptr<TabletIndex>& tablet_index,
                                                   LogicalType field_type, std::unique_ptr<InvertedReader>* res) {
    return CLuceneInvertedReader::create(path, tablet_index, field_type, res);
}

Status CLucenePlugin::get_inverted_index_compaction(std::shared_ptr<TabletIndex> tablet_index, Compaction* compaction) {
    return Status::InternalError("Not supported yet");
}

} // namespace starrocks