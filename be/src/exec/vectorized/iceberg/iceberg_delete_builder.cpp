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

Status ParquetEqualityDeleteBuilder::build(const std::string& timezone, std::vector<ExprContext*> conjunct_ctxs,
                                           std::string& file_path, int64_t file_length) {
    // No need to set src slot descriptors here since we
    // should read all the columns from equality delete file
    std::vector<SlotDescriptor*> src_slot_descriptors;
    auto iter = new IcebergDeleteFileIterator();
    RETURN_IF_ERROR(iter->init(_fs, timezone, file_path, file_length, src_slot_descriptors, false));
    std::shared_ptr<::arrow::RecordBatch> batch;
    std::unordered_map<std::string, SlotDescriptor*> name_to_slot;
    for (int i = 0; i < batch->num_columns(); ++i) {
        auto column_name = batch->schema()->field_names()[i];
        for (auto& slot : _materialize_slots) {
            auto col_name = slot->col_name();
            if (column_name == col_name) {
                name_to_slot[col_name] = slot;
            }
        }
    }
    while (iter->has_next()) {
        batch = iter->next();
        for (size_t row = 0; row < batch->num_rows(); row++) {
            std::vector<Expr*> conjuncts;
            for (int i = 0; i < batch->num_columns(); ++i) {
                auto column_name = batch->schema()->field_names()[i];
                if (name_to_slot.find(column_name) != name_to_slot.end()) {
                    SlotDescriptor* slot = name_to_slot[column_name];
                    TExprNode expr_node;
                    expr_node.opcode = TExprOpcode::NE;
                    expr_node.node_type = TExprNodeType::BINARY_PRED;
                    expr_node.num_children = 2;
                    expr_node.__isset.opcode = true;
                    expr_node.__isset.child_type = true;
                    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
                    ColumnPtr column_ptr = ColumnHelper::create_column(slot->type(), true);
                    switch (slot->type().type) {
                    case TYPE_BOOLEAN: {
                        expr_node.child_type = TPrimitiveType::BOOLEAN;
                        ::arrow::BooleanArray* bool_array = static_cast<arrow::BooleanArray*>(batch->column(i).get());
                        column_ptr->append_datum(Datum(bool_array->Value(row)));
                        break;
                    }
                    case TYPE_TINYINT:
                    case TYPE_SMALLINT:
                    case TYPE_INT: {
                        expr_node.child_type = TPrimitiveType::INT;
                        ::arrow::Int32Array* int_array = static_cast<arrow::Int32Array*>(batch->column(i).get());
                        column_ptr->append_datum(Datum(int_array->Value(row)));
                        break;
                    }

                    case TYPE_BIGINT:
                    case TYPE_LARGEINT: {
                        expr_node.child_type = TPrimitiveType::BIGINT;
                        ::arrow::Int64Array* int_array = static_cast<arrow::Int64Array*>(batch->column(i).get());
                        column_ptr->append_datum(Datum(int_array->Value(row)));
                        break;
                    }
                    case TYPE_FLOAT:
                    case TYPE_DOUBLE: {
                        expr_node.child_type = TPrimitiveType::DOUBLE;
                        ::arrow::DoubleArray* double_array = static_cast<arrow::DoubleArray*>(batch->column(i).get());
                        column_ptr->append_datum(Datum(double_array->Value(row)));
                        break;
                    }
                    case TYPE_DATE: {
                        expr_node.child_type = TPrimitiveType::DATE;
                        ::arrow::Date32Array* date_array = static_cast<arrow::Date32Array*>(batch->column(i).get());
                        column_ptr->append_datum(Datum(date_array->Value(row)));
                        break;
                    }
                    case TYPE_DATETIME: {
                        expr_node.child_type = TPrimitiveType::DATETIME;
                        ::arrow::Date64Array* date_array = static_cast<arrow::Date64Array*>(batch->column(i).get());
                        column_ptr->append_datum(Datum(date_array->Value(row)));
                        break;
                    }
                    case TYPE_TIME: {
                        expr_node.child_type = TPrimitiveType::TIME;
                        ::arrow::TimestampArray* time_array =
                                static_cast<arrow::TimestampArray*>(batch->column(i).get());
                        column_ptr->append_datum(Datum(time_array->Value(row)));
                        break;
                    }
                    case TYPE_CHAR:
                    case TYPE_VARCHAR:
                    case TYPE_DECIMALV2:
                    case TYPE_DECIMAL:
                    case TYPE_DECIMAL32:
                    case TYPE_DECIMAL64:
                    case TYPE_DECIMAL128:
                    case TYPE_PERCENTILE:
                    default:
                        LOG(WARNING) << "Current type has not been supported in equality file!" << file_path;
                        break;
                    }
                    auto expr = VectorizedBinaryPredicateFactory::from_thrift(expr_node);
                    // TODO: do refactor with smart pointer
                    expr->add_child(new ColumnRef(slot));
                    expr->add_child(new VectorizedLiteral(std::move(column_ptr), slot->type()));
                    conjuncts.emplace_back(expr);
                }
            }

            if (conjuncts.size() > 0) {
                TExprNode expr_node;
                expr_node.opcode = TExprOpcode::COMPOUND_AND;
                expr_node.node_type = TExprNodeType::BINARY_PRED;
                expr_node.num_children = conjuncts.size();
                expr_node.__isset.opcode = true;
                expr_node.__isset.child_type = true;
                expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
                auto expr = VectorizedCompoundPredicateFactory::from_thrift(expr_node);
                for (auto conjunct : conjuncts) {
                    expr->add_child(conjunct);
                }
                auto expr_context = new ExprContext(expr);
                conjunct_ctxs.emplace_back(expr_context);
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks::vectorized