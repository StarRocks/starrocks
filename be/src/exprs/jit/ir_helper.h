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

#include <cstdint>

#include "column/type_traits.h"
#include "common/statusor.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Value.h"
#include "runtime/types.h"

namespace starrocks {

/**
 * @brief The LLVMDatum struct is utilized to store the datum's value and nullity flag within LLVM IR.
 */
struct LLVMDatum {
    llvm::Value* value = nullptr;     ///< Represents the actual value of the datum.
    llvm::Value* null_flag = nullptr; ///< Represents the nullity status of the datum.

    LLVMDatum(llvm::IRBuilder<>& b) { null_flag = llvm::ConstantInt::get(b.getInt8Ty(), 0); }
};

/**
 * @brief The LLVMDatum struct is utilized to store the column's values and nullity flags within LLVM IR.
 */
struct LLVMColumn {
    llvm::Value* is_constant = nullptr; ///< Indicates whether the column is constant.
    llvm::Value* values = nullptr;      ///< Represents the actual values of the column.
    llvm::Type* value_type = nullptr;   ///< Represents the type of the column's values.
    llvm::Value* nullable =
            nullptr; ///< Indicates whether the column can be null. If the column is non-nullable, calculating the null flag becomes unnecessary.
                     // The null flags is a bitset, so the type is i8*.
    llvm::Value* null_flags = nullptr; ///< Represents the nullity status of the column.
};

class IRHelper {
public:
    /**
     * @brief Check if the logical type is supported by JIT.
     */
    static bool support_jit(const LogicalType& type);

    /**
     * @brief Convert a logical type to its corresponding LLVM IR type.
     * Since the kinds of LLVM IR types can change depending on the hardware we use, we need a flexible method that can adapt to these differences.
     */
    static StatusOr<llvm::Type*> logical_to_ir_type(llvm::IRBuilder<>& b, const LogicalType& type);

    /**
     * @brief Create a LLVM IR value from a C++ value.
     */
    template <LogicalType type, typename T = RunTimeCppType<type>>
    static StatusOr<llvm::Value*> create_ir_number(llvm::IRBuilder<>& b, T value) {
        switch (type) {
        case TYPE_BOOLEAN:
            return b.getInt8(value);
        case TYPE_TINYINT:
            return b.getInt8(value);
        case TYPE_SMALLINT:
            return b.getInt16(value);
        case TYPE_INT:
        case TYPE_DECIMAL32:
            return b.getInt32(value);
        case TYPE_BIGINT:
        case TYPE_DECIMAL64:
            return b.getInt64(value);
        case TYPE_LARGEINT:
        case TYPE_DECIMAL128: {
            // TODO(Yueyang): test this.
            llvm::APInt value_128(128, value, true);
            return llvm::ConstantInt::get(b.getContext(), value_128);
        }
        case TYPE_FLOAT:
            return llvm::ConstantFP::get(b.getFloatTy(), value);
        case TYPE_DOUBLE:
            return llvm::ConstantFP::get(b.getDoubleTy(), value);
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_TIME:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_DECIMALV2:
        case TYPE_VARBINARY:
        default:
            // Not supported.
            return Status::NotSupported("JIT type not supported.");
        }
    }

    // cast bool of int8 to llvm bool int1
    static llvm::Value* bool_to_cond(llvm::IRBuilder<>& b, llvm::Value* int8) {
        return b.CreateICmpNE(int8, llvm::ConstantInt::get(int8->getType(), 0));
    }

    static StatusOr<llvm::Value*> create_ir_number(llvm::IRBuilder<>& b, const LogicalType& type, const uint8_t* value);

    /** 
     * @brief Convert a LLVM IR value from one type to another.
     */
    static StatusOr<llvm::Value*> cast_to_type(llvm::IRBuilder<>& b, llvm::Value* value, const LogicalType& from_type,
                                               const LogicalType& to_type);
};

} // namespace starrocks