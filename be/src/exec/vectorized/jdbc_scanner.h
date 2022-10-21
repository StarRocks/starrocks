// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/olap_common.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "jni.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "udf/java/java_udf.h"

namespace starrocks::vectorized {

struct JDBCScanContext {
    std::string driver_path;
    std::string driver_class_name;
    std::string jdbc_url;
    std::string user;
    std::string passwd;
    std::string sql;
    std::map<std::string, std::string> properties;
};

struct JDBCScannerProfile {
    RuntimeProfile::Counter* rows_read_counter = nullptr;
    RuntimeProfile::Counter* io_timer = nullptr;
    RuntimeProfile::Counter* io_counter = nullptr;
    RuntimeProfile::Counter* fill_chunk_timer = nullptr;
};

class JDBCScanner {
public:
    JDBCScanner(const JDBCScanContext& context, const TupleDescriptor* tuple_desc, RuntimeProfile* runtime_profile)
            : _scan_ctx(context),
              _tuple_desc(tuple_desc),
              _slot_descs(tuple_desc->slots()),
              _runtime_profile(runtime_profile) {}

    ~JDBCScanner();

    Status open(RuntimeState* state);

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos);

    Status close(RuntimeState* state);

    // if execution threads has been changed, we have to reset jni env
    // because jni env can not be used across threads.
    Status reset_jni_env();

private:
    void _init_profile();

    Status _precheck_data_type(const std::string& java_class, SlotDescriptor* slot_desc);

    Status _init_jdbc_bridge();

    Status _init_jdbc_scan_context(RuntimeState* state);

    Status _init_jdbc_scanner();

    Status _init_column_class_name();

    Status _has_next(bool* result);

    Status _get_next_chunk(jobject* chunk);

    Status _fill_chunk(jobject jchunk, ChunkPtr* chunk);

    Status _close_jdbc_scanner();

    template <PrimitiveType type, typename CppType>
    void _append_data(Column* column, CppType& value);

    template <typename CppType>
    Status _append_value_from_result(jobject jval, std::function<CppType(jobject)> get_value_func,
                                     SlotDescriptor* slot_desc, Column* column);

    Status _append_datetime_val(jobject jval, SlotDescriptor* slot_desc, Column* column);

    Status _append_localdatetime_val(jobject jval, SlotDescriptor* slot_desc, Column* column);

    Status _append_date_val(jobject jval, SlotDescriptor* slot_desc, Column* column);

    Status _append_decimal_val(jobject jval, SlotDescriptor* slot_desc, Column* column);

    std::string _get_date_string(jobject jval);

    std::string _get_localdatetime_string(jobject jval);

    JDBCScanContext _scan_ctx;
    // result tuple desc
    const TupleDescriptor* _tuple_desc;
    // result column slot desc
    std::vector<SlotDescriptor*> _slot_descs;
    // java class name for each result column
    std::vector<std::string> _column_class_name;

    JNIEnv* _jni_env = nullptr;

    jclass _jdbc_bridge_cls;
    jclass _jdbc_scanner_cls;
    jclass _jdbc_util_cls;

    jmethodID _scanner_has_next;
    jmethodID _scanner_get_next_chunk;
    jmethodID _scanner_close;
    // JDBCUtil method
    jmethodID _util_format_date;
    jmethodID _util_format_localdatetime;
    // _jdbc_bridge and _jdbc_scan_context are only used for cross-function passing,
    // they will be invalid after invoking _init_jdbc_scanner
    jobject _jdbc_bridge;
    jobject _jdbc_scan_context;
    jobject _jdbc_scanner;

    RuntimeProfile* _runtime_profile = nullptr;
    JDBCScannerProfile _profile;

    static constexpr const char* JDBC_BRIDGE_CLASS_NAME = "com/starrocks/jdbcbridge/JDBCBridge";
    static constexpr const char* JDBC_SCAN_CONTEXT_CLASS_NAME = "com/starrocks/jdbcbridge/JDBCScanContext";
    static constexpr const char* JDBC_SCANNER_CLASS_NAME = "com/starrocks/jdbcbridge/JDBCScanner";
    static constexpr const char* JDBC_UTIL_CLASS_NAME = "com/starrocks/jdbcbridge/JDBCUtil";

    static const int32_t DEFAULT_JDBC_CONNECTION_POOL_SIZE = 8;
    static const int32_t MINIMUM_ALLOWED_JDBC_CONNECTION_IDLE_TIMEOUT_MS = 10000;
};
} // namespace starrocks::vectorized