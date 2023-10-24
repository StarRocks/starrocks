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

    LLVMDatum(llvm::IRBuilder<>& b) { null_flag = b.getInt8(0); }
};

/**
 * @brief The LLVMDatum struct is utilized to store the column's values and nullity flags within LLVM IR.
 */
struct LLVMColumn {
    llvm::Value* values = nullptr;    ///< Represents the actual values of the column.
    llvm::Type* value_type = nullptr; ///< Represents the type of the column's values.
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
    template <typename Type>
    static StatusOr<llvm::Value*> create_ir_number(llvm::IRBuilder<>& b, const LogicalType& type, Type value);

    static StatusOr<llvm::Value*> create_ir_number(llvm::IRBuilder<>& b, const LogicalType& type, const uint8_t* value);

    /** 
     * @brief Convert a LLVM IR value from one type to another.
     */
    static StatusOr<llvm::Value*> cast_to_type(llvm::IRBuilder<>& b, llvm::Value* value, const LogicalType& from_type,
                                               const LogicalType& to_type);
};

} // namespace starrocks