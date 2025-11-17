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
// See the License for the specific la

package com.starrocks.type;

public class HLLType extends ScalarType {

    // HLL DEFAULT LENGTH  2^14(registers) + 1(type)
    public static final int MAX_HLL_LENGTH = 16385;

    public static final ScalarType HLL = new HLLType(MAX_HLL_LENGTH);

    public HLLType(int len) {
        super(PrimitiveType.HLL);
        setLength(len);
    }
}
