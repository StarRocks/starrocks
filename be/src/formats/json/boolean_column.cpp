// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "boolean_column.h"

#include "column/binary_column.h"
#include "gutil/strings/substitute.h"

namespace starrocks::vectorized {

static auto literal_0_slice{Slice{"0"}};
static auto literal_1_slice{Slice{"1"}};

Status add_boolean_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                          simdjson::ondemand::value* value) {
    auto binary_column = down_cast<BinaryColumn*>(column);

    try {
        simdjson::ondemand::json_type tp = value->type();
        switch (tp) {
        case simdjson::ondemand::json_type::boolean: {
            bool ok = value->get_bool();
            if (ok) {
                binary_column->append(literal_1_slice);
            } else {
                binary_column->append(literal_0_slice);
            }
            return Status::OK();
        }

        case simdjson::ondemand::json_type::number: {
            int64_t ok = value->get_int64();
            if (ok) {
                binary_column->append(literal_1_slice);
            } else {
                binary_column->append(literal_0_slice);
            }
            return Status::OK();
        }

        default: {
            auto err_msg = strings::Substitute("Failed to parse value as boolean, column=$0", name);
            return Status::DataQualityError(err_msg);
        }
        }
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as boolean, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

} // namespace starrocks::vectorized
