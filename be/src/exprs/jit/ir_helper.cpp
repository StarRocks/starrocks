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

#include "exprs/jit/ir_helper.h"

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "types/logical_type.h"

namespace starrocks {

bool IRHelper::support_jit(const LogicalType& type) {
    return type == TYPE_BOOLEAN || type == TYPE_TINYINT || type == TYPE_SMALLINT || type == TYPE_INT ||
           type == TYPE_BIGINT || type == TYPE_LARGEINT // Integer types;
           || type == TYPE_FLOAT || type == TYPE_DOUBLE // Floating point types;
            ;
}

// This code should be synchronized with corresponding section in type_traits.h .
StatusOr<llvm::Type*> IRHelper::logical_to_ir_type(llvm::IRBuilder<>& b, const LogicalType& type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return b.getInt8Ty();
    case TYPE_TINYINT:
        return b.getInt8Ty();
    case TYPE_SMALLINT:
        return b.getInt16Ty();
    case TYPE_INT:
    case TYPE_DECIMAL32:
        return b.getInt32Ty();
    case TYPE_BIGINT:
    case TYPE_DECIMAL64:
        return b.getInt64Ty();
    case TYPE_LARGEINT:
    case TYPE_DECIMAL128:
        return b.getInt128Ty();
    case TYPE_FLOAT:
        return b.getFloatTy();
    case TYPE_DOUBLE:
        return b.getDoubleTy();
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

StatusOr<llvm::Value*> IRHelper::create_ir_number(llvm::IRBuilder<>& b, const LogicalType& type, int64_t value) {
    switch (type) {
    case TYPE_BOOLEAN:
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

StatusOr<llvm::Value*> IRHelper::load_ir_number(llvm::IRBuilder<>& b, const LogicalType& type, const uint8_t* value) {
    switch (type) {
    case TYPE_BOOLEAN:
        return b.getInt8(reinterpret_cast<const uint8_t*>(value)[0]);
    case TYPE_TINYINT:
        return b.getInt8(reinterpret_cast<const int8_t*>(value)[0]);
    case TYPE_SMALLINT:
        return b.getInt16(reinterpret_cast<const int16_t*>(value)[0]);
    case TYPE_INT:
    case TYPE_DECIMAL32:
        return b.getInt32(reinterpret_cast<const int32_t*>(value)[0]);
    case TYPE_BIGINT:
    case TYPE_DECIMAL64:
        return b.getInt64(reinterpret_cast<const int64_t*>(value)[0]);
    case TYPE_LARGEINT:
    case TYPE_DECIMAL128: {
        llvm::APInt value_128(128, reinterpret_cast<const int128_t*>(value)[0], true);
        return llvm::ConstantInt::get(b.getContext(), value_128);
    }
    case TYPE_FLOAT:
        return llvm::ConstantFP::get(b.getFloatTy(), reinterpret_cast<const float*>(value)[0]);
    case TYPE_DOUBLE:
        return llvm::ConstantFP::get(b.getDoubleTy(), reinterpret_cast<const double*>(value)[0]);
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

StatusOr<llvm::Value*> IRHelper::cast_to_type(llvm::IRBuilder<>& b, llvm::Value* value, const LogicalType& from_type,
                                              const LogicalType& to_type) {
    ASSIGN_OR_RETURN(auto logical_to_type, IRHelper::logical_to_ir_type(b, to_type));

    if (from_type == to_type) {
        return value;
    } else if (from_type == TYPE_BOOLEAN && is_integer_type(to_type)) {
        // TODO(Yueyang): check this.
        return b.CreateIntCast(value, logical_to_type, false);
    } else if (from_type == TYPE_BOOLEAN && is_float_type(to_type)) {
        auto* integer = b.CreateIntCast(value, b.getInt32Ty(), false);
        return b.CreateCast(llvm::Instruction::SIToFP, integer, logical_to_type);
    } else if (is_integer_type(from_type) && to_type == TYPE_BOOLEAN) {
        auto result = b.CreateICmpNE(value, llvm::ConstantInt::get(value->getType(), 0, true));
        return b.CreateCast(llvm::Instruction::ZExt, result, logical_to_type);
    } else if (is_float_type(from_type) && to_type == TYPE_BOOLEAN) {
        auto result = b.CreateFCmpUNE(value, llvm::ConstantFP::get(value->getType(), 0));
        return b.CreateCast(llvm::Instruction::ZExt, result, logical_to_type);
    } else if (is_integer_type(from_type) && is_integer_type(to_type)) {
        return b.CreateIntCast(value, logical_to_type, true);
    } else if (is_integer_type(from_type) && is_float_type(to_type)) {
        return b.CreateCast(llvm::Instruction::SIToFP, value, logical_to_type);
    } else if (is_float_type(from_type) && is_integer_type(to_type)) {
        return b.CreateCast(llvm::Instruction::FPToSI, value, logical_to_type);
    } else if (is_float_type(from_type) && is_float_type(to_type)) {
        return b.CreateFPCast(value, logical_to_type);
    }

    // Not supported.
    return Status::NotSupported("JIT cast type not supported.");
}

} // namespace starrocks