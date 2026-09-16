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

#include "exprs/udf/java/jni_arrow_func_call_stub.h"

#include <arrow/array.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <fmt/format.h>

#include <memory>
#include <string>
#include <utility>

#include "base/utility/arrow_utils.h"
#include "exprs/udf/java/arrow_udf_jni.h"
#include "exprs/udf/java/java_udf_reflection.h"
#include "jni.h"
#include "runtime/java/java_env.h"
#include "runtime/java/java_global_ref.h"
#include "runtime/java/jvm_class.h"
#include "runtime/java/jvm_helper.h"

namespace starrocks {

// Per-driver JNI state for one arrow scalar UDF: the loaded UDF class + instance, the resolved
// `evaluate` reflect Method, and the com.starrocks.udf.ArrowUDFHelper entry points plus the
// per-stub Arrow BufferAllocator handle. Global refs self-clean on a JNI thread (JavaGlobalRef);
// the allocator is closed on a JNI pthread in the destructor (its leak check doubles as a guard).
struct JniArrowUDFContext {
    std::unique_ptr<JavaUdfClassLoader> classloader;
    std::unique_ptr<JavaUdfClassAnalyzer> analyzer;
    JVMClass udf_class{nullptr};
    JavaGlobalRef udf_handle{nullptr};
    JavaGlobalRef evaluate_method{nullptr};
    jclass arrow_helper_class = nullptr; // borrowed from the process-global cache (not owned)
    jmethodID evaluate_arrow_mid = nullptr;
    jmethodID close_allocator_mid = nullptr;
    jlong allocator_handle = 0;

    ~JniArrowUDFContext() {
        if (allocator_handle == 0 || close_allocator_mid == nullptr) {
            return;
        }
        jclass klass = arrow_helper_class;
        jmethodID mid = close_allocator_mid;
        jlong handle = allocator_handle;
        (void)JavaEnv::GetInstance()->call_function_in_pthread([klass, mid, handle]() -> Status {
            JNIEnv* env = JVMHelper::getInstance().getEnv();
            env->CallStaticVoidMethod(klass, mid, handle);
            RETURN_ERROR_IF_JNI_EXCEPTION(env);
            return Status::OK();
        });
    }
};

namespace {

class JniArrowFuncCallStub final : public AbstractArrowFuncCallStub {
public:
    JniArrowFuncCallStub(FunctionContext* ctx, RuntimeState* state, std::shared_ptr<JniArrowUDFContext> jni_ctx)
            : AbstractArrowFuncCallStub(ctx), _state(state), _ctx(std::move(jni_ctx)) {}

protected:
    StatusOr<std::shared_ptr<RecordBatch>> do_evaluate(RecordBatch&& batch) override;

private:
    RuntimeState* _state;
    std::shared_ptr<JniArrowUDFContext> _ctx;
};

StatusOr<std::shared_ptr<arrow::RecordBatch>> JniArrowFuncCallStub::do_evaluate(RecordBatch&& batch) {
    const int64_t num_rows = batch.num_rows();
    std::shared_ptr<arrow::Array> result_array;

    auto call = [&]() -> Status {
        JNIEnv* env = JVMHelper::getInstance().getEnv();

        // Export the input batch to the Arrow C Data Interface. ExportedArrowBatch releases the
        // exported buffers on scope exit unless Java consumed (moved) them.
        ExportedArrowBatch input;
        RETURN_IF_ERROR(input.export_record_batch(batch));

        // BE-owned output structs; Java exports the result FieldVector into them and transfers
        // buffer ownership back to us (freed when the imported arrow::Array is destroyed).
        ArrowSchema out_schema{};
        ArrowArray out_array{};

        env->CallStaticVoidMethod(_ctx->arrow_helper_class, _ctx->evaluate_arrow_mid, _ctx->udf_handle.handle(),
                                  _ctx->evaluate_method.handle(), _ctx->allocator_handle, input.schema_addr(),
                                  input.array_addr(), reinterpret_cast<jlong>(&out_schema),
                                  reinterpret_cast<jlong>(&out_array));
        RETURN_ERROR_IF_JNI_EXCEPTION(env);

        auto imported = arrow::ImportArray(&out_array, &out_schema);
        if (!imported.ok()) {
            if (out_array.release != nullptr) {
                out_array.release(&out_array);
            }
            if (out_schema.release != nullptr) {
                out_schema.release(&out_schema);
            }
            return to_status(imported.status());
        }
        result_array = *imported;
        return Status::OK();
    };

    RETURN_IF_ERROR(JavaEnv::GetInstance()->submit_java_udf_call(_state, call)->get_future().get());

    if (result_array->length() != num_rows) {
        return Status::InternalError(fmt::format("arrow UDF result length {} does not match input row count {}",
                                                 result_array->length(), num_rows));
    }
    auto result_schema = arrow::schema({arrow::field("result", result_array->type(), /*nullable=*/true)});
    return arrow::RecordBatch::Make(result_schema, result_array->length(), {result_array});
}

} // namespace

std::unique_ptr<UDFCallStub> create_jni_arrow_call_stub(FunctionContext* ctx, RuntimeState* state,
                                                        const std::string& libpath, const std::string& symbol) {
    auto jni_ctx = std::make_shared<JniArrowUDFContext>();

    auto build = [&]() -> Status {
        JNIEnv* env = JVMHelper::getInstance().getEnv();

        jni_ctx->classloader = std::make_unique<JavaUdfClassLoader>(libpath);
        RETURN_IF_ERROR(jni_ctx->classloader->init());
        jni_ctx->analyzer = std::make_unique<JavaUdfClassAnalyzer>();
        ASSIGN_OR_RETURN(jni_ctx->udf_class, jni_ctx->classloader->getClass(symbol));
        ASSIGN_OR_RETURN(jni_ctx->udf_handle, jni_ctx->udf_class.newInstance());
        ASSIGN_OR_RETURN(auto evaluate_obj,
                         jni_ctx->analyzer->get_method_object(jni_ctx->udf_class.clazz(), "evaluate"));
        jni_ctx->evaluate_method = JavaGlobalRef(evaluate_obj);

        // Resolve com.starrocks.udf.ArrowUDFHelper (bundled in udf-extensions.jar, same as UDFHelper).
        ASSIGN_OR_RETURN(jni_ctx->arrow_helper_class, arrow_udf_helper_class());
        jclass helper = jni_ctx->arrow_helper_class;

        jmethodID create_mid = env->GetStaticMethodID(helper, "createAllocator", "()J");
        RETURN_ERROR_IF_JNI_EXCEPTION(env);
        jni_ctx->close_allocator_mid = env->GetStaticMethodID(helper, "closeAllocator", "(J)V");
        RETURN_ERROR_IF_JNI_EXCEPTION(env);
        jni_ctx->evaluate_arrow_mid =
                env->GetStaticMethodID(helper, "evaluateArrow", "(Ljava/lang/Object;Ljava/lang/reflect/Method;JJJJJ)V");
        RETURN_ERROR_IF_JNI_EXCEPTION(env);

        jni_ctx->allocator_handle = env->CallStaticLongMethod(helper, create_mid);
        RETURN_ERROR_IF_JNI_EXCEPTION(env);
        return Status::OK();
    };

    Status build_status = JavaEnv::GetInstance()->submit_java_udf_call(state, build)->get_future().get();
    if (!build_status.ok()) {
        return create_error_call_stub(build_status);
    }
    return std::make_unique<JniArrowFuncCallStub>(ctx, state, std::move(jni_ctx));
}

} // namespace starrocks
