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


package com.staros.util;

import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LogUtils {
    public static void fatal(Logger logger, String format, Object... args) {
        List<Object> newArgs = Arrays.stream(args).collect(Collectors.toList());
        // Append a throwable object in order to log current stacktrace
        newArgs.add(new Throwable("LogFatalStackTrace"));
        logger.fatal(format, newArgs.toArray());
        System.exit(-1);
    }
}
