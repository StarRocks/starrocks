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

#include <CLucene.h>
#include <CLucene/document/Document.h>
#include <CLucene/document/Field.h>
#include <CLucene/index/IndexWriter.h>
#include <CLucene/util/bkd/bkd_writer.h>

#include <roaring/roaring.hh>
#include <utility>

#include "storage/inverted/inverted_index_option.h"
#include "storage/inverted/inverted_writer.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/rowset/common.h"
#include "storage/tablet_schema.h"

namespace starrocks {

const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
const int32_t MAX_BUFFER_DOCS = 100000000;
const int32_t MERGE_FACTOR = 100000000;
const int32_t MAX_LEAF_COUNT = 1024;
const int32_t MAX_DEPTH_IN_BKD_TREE = 32;
const float MAXMBSortInHeap = 512.0 * 8;
const float RAMBufferSizeMB = 512;
const int DIMS = 1;
const std::string empty_value;

class CLuceneInvertedWriter : public InvertedWriter {
public:
    CLuceneInvertedWriter(const CLuceneInvertedWriter&) = delete;

    const CLuceneInvertedWriter& operator=(const CLuceneInvertedWriter&) = delete;

    CLuceneInvertedWriter() = default;

    ~CLuceneInvertedWriter() override = default;

    static Status create(const TypeInfoPtr& typeinfo, const std::string& field_name, const std::string& directory,
                         TabletIndex* tablet_index, std::unique_ptr<InvertedWriter>* res);
};

} // namespace starrocks
