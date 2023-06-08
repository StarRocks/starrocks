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

package com.starrocks.common.util;

import java.util.Arrays;
import java.util.stream.Collectors;

public class LogUtil {
    public static String getCurrentStackTrace() {
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .map(stack -> "        " + stack.toString())
                .collect(Collectors.joining(System.lineSeparator(), System.lineSeparator(), ""));
    }
}
