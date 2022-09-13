// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset_factory.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/rowset/rowset_factory.h"

#include <memory>

#include "beta_rowset.h"
#include "gen_cpp/olap_file.pb.h"
#include "rowset_writer_adapter.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset_writer.h"

namespace starrocks {

Status RowsetFactory::create_rowset(const TabletSchema* schema, const std::string& rowset_path,
                                    const RowsetMetaSharedPtr& rowset_meta, RowsetSharedPtr* rowset) {
    if (rowset_meta->rowset_type() == BETA_ROWSET) {
        *rowset = BetaRowset::create(schema, rowset_path, rowset_meta);
        RETURN_IF_ERROR((*rowset)->init());
        return Status::OK();
    }
    return Status::NotSupported("unsupported rowset type");
}

Status RowsetFactory::create_rowset_writer(const RowsetWriterContext& context, std::unique_ptr<RowsetWriter>* output) {
    if (UNLIKELY(context.rowset_type != BETA_ROWSET)) {
        return Status::NotSupported("unsupported rowset type");
    }

    auto tablet_schema = context.tablet_schema;
    auto memory_format_version = context.memory_format_version;
    auto storage_format_version = context.storage_format_version;

    if (memory_format_version == kDataFormatUnknown) {
        if (tablet_schema->contains_format_v1_column()) {
            memory_format_version = kDataFormatV1;
        } else if (tablet_schema->contains_format_v2_column()) {
            memory_format_version = kDataFormatV2;
        } else {
            memory_format_version = storage_format_version;
        }
    }

    if (storage_format_version != kDataFormatV1 && storage_format_version != kDataFormatV2) {
        LOG(WARNING) << "Invalid storage format version " << storage_format_version;
        return Status::InvalidArgument("invalid storage_format_version");
    }
    if (memory_format_version != kDataFormatV1 && memory_format_version != kDataFormatV2) {
        LOG(WARNING) << "Invalid memory format version " << memory_format_version;
        return Status::InvalidArgument("invalid memory_format_version");
    }

    if (memory_format_version != storage_format_version) {
        auto adapter_context = context;
        adapter_context.memory_format_version = memory_format_version;
        *output = std::make_unique<vectorized::RowsetWriterAdapter>(adapter_context);
    } else if (context.writer_type == kHorizontal) {
        *output = std::make_unique<HorizontalBetaRowsetWriter>(context);
    } else {
        DCHECK(context.writer_type == kVertical);
        *output = std::make_unique<VerticalBetaRowsetWriter>(context);
    }
    return (*output)->init();
}

} // namespace starrocks
