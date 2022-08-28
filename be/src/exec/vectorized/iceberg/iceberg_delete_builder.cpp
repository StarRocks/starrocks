// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "iceberg_delete_builder.h"

#include "exprs/vectorized/binary_predicate.h"
#include "exprs/vectorized/column_ref.h"
#include "exprs/vectorized/compound_predicate.h"
#include "exprs/vectorized/literal.h"
#include "iceberg_delete_file_iterator.h"

namespace starrocks::vectorized {

static const std::string kPosDeleteFilePathField = "file_path";
static const std::string kPosDeletePosField = "pos";

Status ParquetPositionDeleteBuilder::build(const std::string& timezone, std::set<std::int64_t> need_skip_rowids,
                                           std::string& file_path, int64_t file_length) {
    std::vector<SlotDescriptor*> src_slot_descriptors;
    src_slot_descriptors.emplace_back(new SlotDescriptor(kPosDeleteFilePathField));
    src_slot_descriptors.emplace_back(new SlotDescriptor(kPosDeletePosField));
    auto iter = new IcebergDeleteFileIterator();
    RETURN_IF_ERROR(iter->init(_fs, timezone, file_path, file_length, src_slot_descriptors, true));
    std::shared_ptr<::arrow::RecordBatch> batch;
    while (iter->has_next()) {
        batch = iter->next();
        ::arrow::StringArray* file_path_array = static_cast<arrow::StringArray*>(batch->column(0).get());
        ::arrow::Int64Array* pos_array = static_cast<arrow::Int64Array*>(batch->column(1).get());
        for (size_t row = 0; row < batch->num_rows(); row++) {
            if (file_path_array->Value(row).find(_datafile_path) != std::string::npos) {
                need_skip_rowids.emplace(pos_array->Value(row));
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks::vectorized