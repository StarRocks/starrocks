// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/parquet_scanner.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "env/env_util.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {

#define ASSERT_STATUS_OK(stmt)                                     \
    do {                                                           \
        auto status = (stmt);                                      \
        std::cout << "Status:" << status.to_string() << std::endl; \
        ASSERT_TRUE(status.ok());                                  \
    } while (0)

class ParquetScannerTest : public ::testing::Test {
    std::vector<TBrokerRangeDesc> generate_ranges(const std::vector<std::string>& file_names,
                                                  int32_t num_columns_from_file,
                                                  const std::vector<std::string>& columns_from_path) {
        std::vector<TBrokerRangeDesc> ranges;
        ranges.resize(file_names.size());
        for (auto i = 0; i < file_names.size(); ++i) {
            TBrokerRangeDesc& range = ranges[i];
            range.__set_num_of_columns_from_file(num_columns_from_file);
            range.__set_columns_from_path(columns_from_path);
            range.__set_path(file_names[i]);
            range.file_type = TFileType::FILE_LOCAL;
        }
        return ranges;
    }

    using SlotInfo = std::tuple<std::string, TypeDescriptor, bool>;
    using SlotInfoArray = std::vector<SlotInfo>;

    void generate_desc_tuple(const SlotInfoArray& slot_infos, TDescriptorTableBuilder* desc_tbl_builder) {
        TTupleDescriptorBuilder tuple_builder;
        for (auto& slot_info : slot_infos) {
            auto& name = std::get<0>(slot_info);
            auto& type = std::get<1>(slot_info);
            auto& is_nullable = std::get<2>(slot_info);
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(type)
                    .length(type.len)
                    .precision(type.precision)
                    .scale(type.scale)
                    .nullable(is_nullable);
            slot_desc_builder.column_name(name);
            tuple_builder.add_slot(slot_desc_builder.build());
        }
        tuple_builder.build(desc_tbl_builder);
    }

    DescriptorTbl* generate_desc_tbl(const SlotInfoArray& src_slot_infos, const SlotInfoArray& dst_slot_infos) {
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        generate_desc_tuple(src_slot_infos, &desc_tbl_builder);
        if (!dst_slot_infos.empty()) {
            generate_desc_tuple(dst_slot_infos, &desc_tbl_builder);
        }
        DescriptorTbl* desc_tbl = nullptr;
        DescriptorTbl::create(&_obj_pool, desc_tbl_builder.desc_tbl(), &desc_tbl);
        return desc_tbl;
    }

    starrocks::TExpr create_column_ref(int32_t slot_id, const TypeDescriptor& type_desc, bool is_nullable) {
        starrocks::TExpr e = starrocks::TExpr();
        e.nodes.emplace_back(TExprNode());
        e.nodes[0].__set_type(type_desc.to_thrift());
        e.nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        e.nodes[0].__set_is_nullable(is_nullable);
        e.nodes[0].__set_slot_ref(TSlotRef());
        e.nodes[0].slot_ref.__set_slot_id((::starrocks::TSlotId)slot_id);
        e.nodes[0].use_vectorized = true;
        return e;
    }

    starrocks::TExpr create_cast_expr(const starrocks::TExpr& child, const TypeDescriptor& type_desc) {
        starrocks::TExpr e = starrocks::TExpr();
        e.nodes.emplace_back(TExprNode());
        e.nodes.insert(e.nodes.end(), child.nodes.begin(), child.nodes.end());
        auto& to_expr = e.nodes[0];
        to_expr.__set_type(type_desc.to_thrift());
        to_expr.__set_child_type(child.nodes[0].type.types[0].scalar_type.type);
        to_expr.__set_node_type(TExprNodeType::CAST_EXPR);
        to_expr.__set_is_nullable(true);
        to_expr.__set_num_children(1);
        to_expr.use_vectorized = true;
        return e;
    }

    std::unique_ptr<ParquetScanner> create_parquet_scanner(
            const std::string& timezone, DescriptorTbl* desc_tbl,
            const std::unordered_map<size_t, ::starrocks::TExpr>& dst_slot_exprs,
            const std::vector<TBrokerRangeDesc>& ranges) {
        /// Init RuntimeState
        auto query_globals = TQueryGlobals();
        query_globals.time_zone = timezone;
        RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), query_globals, nullptr));
        state->set_desc_tbl(desc_tbl);
        state->init_instance_mem_tracker();

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
        params->strict_mode = true;
        std::vector<TupleDescriptor*> tuples;
        desc_tbl->get_tuple_descs(&tuples);
        const auto num_tuples = tuples.size();
        params->src_tuple_id = 0;
        params->dest_tuple_id = num_tuples - 1;
        const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
        const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
        for (int i = 0; i < src_tuple->slots().size(); i++) {
            auto& src_slot = src_tuple->slots()[i];
            auto& dst_slot = dst_tuple->slots()[i];
            if (dst_slot_exprs.count(i)) {
                params->expr_of_dest_slot[dst_slot->id()] = dst_slot_exprs.at(i);
            } else {
                params->expr_of_dest_slot[dst_slot->id()] =
                        create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
            }
        }

        for (int i = 0; i < src_tuple->slots().size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
        ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

        TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;

        return std::make_unique<ParquetScanner>(state, profile, *broker_scan_range, counter);
    }

    void validate(std::unique_ptr<ParquetScanner>& scanner, const size_t expect_num_rows,
                  std::function<void(const ChunkPtr&)> check_func) {
        ASSERT_STATUS_OK(scanner->open());
        size_t num_rows = 0;
        while (true) {
            auto res = scanner->get_next();
            if (!res.ok() && res.status().is_end_of_file()) {
                ASSERT_EQ(expect_num_rows, num_rows);
                break;
            }
            if (!res.ok()) {
                std::cout << "Unexpected status:" << res.status().to_string() << std::endl;
            }
            ChunkPtr chunk = res.value();
            if (chunk == nullptr) {
                ASSERT_EQ(expect_num_rows, num_rows);
                break;
            }

            ASSERT_TRUE(chunk->num_rows() > 0);
            num_rows += chunk->num_rows();
            check_func(chunk);
            break;
        }
        scanner->close();
    }

    SlotInfoArray select_columns(const std::vector<std::string>& column_names, bool is_nullable) {
        auto slot_map = std::unordered_map<std::string, TypeDescriptor>{
                {"col_date", TypeDescriptor::from_primtive_type(TYPE_DATE)},
                {"col_datetime", TypeDescriptor::from_primtive_type(TYPE_DATETIME)},
                {"col_char", TypeDescriptor::from_primtive_type(TYPE_CHAR)},
                {"col_varchar", TypeDescriptor::from_primtive_type(TYPE_VARCHAR)},
                {"col_boolean", TypeDescriptor::from_primtive_type(TYPE_BOOLEAN)},
                {"col_tinyint", TypeDescriptor::from_primtive_type(TYPE_TINYINT)},
                {"col_smallint", TypeDescriptor::from_primtive_type(TYPE_SMALLINT)},
                {"col_int", TypeDescriptor::from_primtive_type(TYPE_INT)},
                {"col_bigint", TypeDescriptor::from_primtive_type(TYPE_BIGINT)},
                {"col_decimal_p6s2", TypeDescriptor::from_primtive_type(TYPE_DECIMAL32, -1, 6, 2)},
                {"col_decimal_p14s5", TypeDescriptor::from_primtive_type(TYPE_DECIMAL64, -1, 14, 5)},
                {"col_decimal_p27s9", TypeDescriptor::from_primtive_type(TYPE_DECIMALV2, -1, 27, 9)},
        };
        SlotInfoArray slot_infos;
        slot_infos.reserve(column_names.size());
        for (auto& name : column_names) {
            slot_infos.emplace_back(name, slot_map[name], is_nullable);
        }
        return slot_infos;
    }

    template <bool is_nullable>
    void test_column_from_path(const std::vector<std::string>& columns_from_file,
                               const std::vector<std::string>& columns_from_path,
                               const std::vector<std::string>& column_values,
                               const std::unordered_map<size_t, ::starrocks::TExpr>& dst_slot_exprs) {
        std::vector<std::string>* file_names = nullptr;
        if constexpr (is_nullable) {
            file_names = &_nullable_file_names;
        } else {
            file_names = &_file_names;
        }
        std::vector<std::string> column_names;
        column_names.reserve(columns_from_file.size() + columns_from_path.size());
        column_names.template insert(column_names.end(), columns_from_file.begin(), columns_from_file.end());
        column_names.template insert(column_names.end(), columns_from_path.begin(), columns_from_path.end());

        auto src_slot_infos = select_columns(columns_from_file, is_nullable);
        for (auto i = 0; i < columns_from_path.size(); ++i) {
            src_slot_infos.template emplace_back(columns_from_path[i], TypeDescriptor::from_primtive_type(TYPE_VARCHAR),
                                                 is_nullable);
        }

        auto dst_slot_infos = select_columns(column_names, is_nullable);

        auto ranges = generate_ranges(*file_names, columns_from_file.size(), column_values);
        auto* desc_tbl = generate_desc_tbl(src_slot_infos, dst_slot_infos);
        auto scanner = create_parquet_scanner("UTC", desc_tbl, dst_slot_exprs, ranges);
        auto check = [](const ChunkPtr& chunk) {
            auto& columns = chunk->columns();
            for (auto& col : columns) {
                if constexpr (is_nullable) {
                    ASSERT_TRUE(!col->only_null() || !col->is_constant());
                } else {
                    ASSERT_TRUE(!col->is_nullable() || !col->is_constant());
                }
            }
        };
        validate(scanner, 36865, check);
    }

    void SetUp() {
        std::string starrocks_home = getenv("STARROCKS_HOME");
        std::string test_exec_dir = starrocks_home + "/be/test/exec";
        _nullable_file_names =
                std::vector<std::string>{test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_0.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_1.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_4095.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_4096.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_4097.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_8191.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_8192.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_8193.parquet"};
        _file_names = std::vector<std::string>{test_exec_dir + "/test_data/parquet_data/data_0.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_1.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_4095.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_4096.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_4097.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_8191.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_8192.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_8193.parquet"};
    }

private:
    ObjectPool _obj_pool;
    std::vector<std::string> _file_names;
    std::vector<std::string> _nullable_file_names;
};

TEST_F(ParquetScannerTest, test_nullable_parquet_data) {
    auto column_names = std::vector<std::string>{
            "col_date",     "col_datetime", "col_char",   "col_varchar",      "col_boolean",       "col_tinyint",
            "col_smallint", "col_int",      "col_bigint", "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9",
    };
    auto slot_infos = select_columns(column_names, true);
    auto ranges = generate_ranges(_nullable_file_names, slot_infos.size(), {});
    auto* desc_tbl = generate_desc_tbl(slot_infos, {});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {
        auto& columns = chunk->columns();
        for (auto& col : columns) {
            ASSERT_TRUE(!col->only_null() && col->is_nullable());
        }
    };
    validate(scanner, 36865, check);
}

TEST_F(ParquetScannerTest, test_parquet_data) {
    auto column_names = std::vector<std::string>{
            "col_date",     "col_datetime", "col_char",   "col_varchar",      "col_boolean",       "col_tinyint",
            "col_smallint", "col_int",      "col_bigint", "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9",
    };
    auto slot_infos = select_columns(column_names, false);
    auto ranges = generate_ranges(_file_names, slot_infos.size(), {});
    auto* desc_tbl = generate_desc_tbl(slot_infos, {});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {
        auto& columns = chunk->columns();
        for (auto& col : columns) {
            ASSERT_TRUE(!col->is_nullable() && !col->is_constant());
        }
    };
    validate(scanner, 36865, check);
}

TEST_F(ParquetScannerTest, test_parquet_data_with_1_column_from_path) {
    std::vector<std::string> columns_from_file = {
            "col_datetime", "col_char",   "col_varchar",      "col_boolean",       "col_tinyint",      "col_smallint",
            "col_int",      "col_bigint", "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9"};
    auto varchar_type = TypeDescriptor::from_primtive_type(TYPE_VARCHAR);
    auto date_type = TypeDescriptor::from_primtive_type(TYPE_DATE);
    std::vector<std::string> columns_from_path = {"col_date"};
    std::vector<std::string> column_values = {"2021-03-22"};
    auto column_ref_expr = create_column_ref(11, varchar_type, true);
    auto cast_expr = create_cast_expr(column_ref_expr, date_type);

    auto dst_slot_exprs = std::unordered_map<size_t, starrocks::TExpr>{{11, cast_expr}};
    test_column_from_path<true>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
    test_column_from_path<false>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
}

TEST_F(ParquetScannerTest, test_parquet_data_with_2_column_from_path) {
    std::vector<std::string> columns_from_file = {
            "col_char", "col_varchar", "col_boolean",      "col_tinyint",       "col_smallint",
            "col_int",  "col_bigint",  "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9"};
    std::vector<std::string> columns_from_path = {"col_date", "col_datetime"};
    std::vector<std::string> column_values = {"2021-02-22", "2020-12-20 22:56:04"};

    auto varchar_type = TypeDescriptor::from_primtive_type(TYPE_VARCHAR);
    auto date_type = TypeDescriptor::from_primtive_type(TYPE_DATE);
    auto datetime_type = TypeDescriptor::from_primtive_type(TYPE_DATETIME);

    auto column_ref_expr10 = create_column_ref(10, varchar_type, true);
    auto column_ref_expr11 = create_column_ref(11, varchar_type, true);

    auto cast_expr10 = create_cast_expr(column_ref_expr10, date_type);
    auto cast_expr11 = create_cast_expr(column_ref_expr11, datetime_type);

    auto dst_slot_exprs = std::unordered_map<size_t, starrocks::TExpr>{
            {10, cast_expr10},
            {11, cast_expr11},
    };

    test_column_from_path<true>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
    test_column_from_path<false>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
}

TEST_F(ParquetScannerTest, test_parquet_data_with_3_column_from_path) {
    std::vector<std::string> columns_from_file = {"col_varchar",      "col_boolean",       "col_tinyint",
                                                  "col_smallint",     "col_int",           "col_bigint",
                                                  "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9"};
    std::vector<std::string> columns_from_path = {"col_date", "col_datetime", "col_char"};

    auto varchar_type = TypeDescriptor::from_primtive_type(TYPE_VARCHAR);
    auto date_type = TypeDescriptor::from_primtive_type(TYPE_DATE);
    auto datetime_type = TypeDescriptor::from_primtive_type(TYPE_DATETIME);

    auto column_ref_expr9 = create_column_ref(9, varchar_type, true);
    auto column_ref_expr10 = create_column_ref(10, varchar_type, true);
    auto column_ref_expr11 = create_column_ref(11, varchar_type, true);

    auto cast_expr9 = create_cast_expr(column_ref_expr9, date_type);
    auto cast_expr10 = create_cast_expr(column_ref_expr10, datetime_type);

    auto dst_slot_exprs = std::unordered_map<size_t, starrocks::TExpr>{
            {9, cast_expr9},
            {10, cast_expr10},
            {11, column_ref_expr11},
    };

    std::vector<std::string> column_values = {"2021-02-22", "2020-12-20 22:56:04", "beijing"};
    test_column_from_path<true>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
    test_column_from_path<false>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
}

} // namespace starrocks::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
