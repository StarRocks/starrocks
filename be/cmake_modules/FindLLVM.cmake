# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if (ENABLE_MULTI_DYNAMIC_LIBS)
    add_library(LLVM SHARED IMPORTED)
    set_target_properties(LLVM PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/llvm/lib/libLLVM-16.so)
    install(FILES ${THIRDPARTY_DIR}/llvm/lib/libLLVM-16.so DESTINATION ${OUTPUT_DIR}/lib)
    set (LLVM_LIBRARIES LLVM)
else()
    set (LLVM_LIBRARIES
        LLVMRuntimeDyld
        LLVMBitstreamReader
        LLVMOption
        LLVMAsmPrinter
        LLVMProfileData
        LLVMAsmParser
        LLVMOrcTargetProcess
        LLVMExecutionEngine
        LLVMBinaryFormat
        LLVMDebugInfoDWARF
        LLVMObjCARCOpts
        LLVMPasses
        LLVMCodeGen
        LLVMFrontendOpenMP
        LLVMMCDisassembler
        LLVMJITLink
        LLVMCFGuard
        LLVMInstrumentation
        LLVMInstCombine
        LLVMipo
        LLVMVectorize
        LLVMIRReader
        LLVMCore
        LLVMTarget
        LLVMMC
        LLVMAnalysis
        LLVMGlobalISel
        LLVMScalarOpts
        LLVMLinker
        LLVMCoroutines
        LLVMTargetParser
        LLVMDemangle
        LLVMRemarks
        LLVMDebugInfoCodeView
        LLVMAggressiveInstCombine
        LLVMIRPrinter
        LLVMOrcShared
        LLVMOrcJIT
        LLVMTextAPI
        LLVMBitWriter
        LLVMBitReader
        LLVMObject
        LLVMTransformUtils
        LLVMSelectionDAG
        LLVMMCParser
        LLVMSupport
    )

    if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
        list(APPEND LLVM_LIBRARIES LLVMX86CodeGen LLVMX86Desc LLVMX86Info LLVMX86AsmParser LLVMX86Disassembler)
    elseif ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "aarch64")
        list(APPEND LLVM_LIBRARIES LLVMAArch64CodeGen LLVMAArch64Desc LLVMAArch64Info LLVMAArch64Utils LLVMAArch64AsmParser LLVMAArch64Disassembler)
    endif()

    foreach(lib IN ITEMS ${LLVM_LIBRARIES})
        add_library(${lib} STATIC IMPORTED GLOBAL)
        set_target_properties(${lib} PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/llvm/lib/lib${lib}.a)
    endforeach()

endif()


