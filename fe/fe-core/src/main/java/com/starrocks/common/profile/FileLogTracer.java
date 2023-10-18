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

package com.starrocks.common.profile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class FileLogTracer extends LogTracer {
    private static final Logger LOG = LogManager.getLogger(FileLogTracer.class);

    public void log(long time, String content) {
        LOG.info(content);
    }

    public void log(long time, String format, Object... objects) {
        FormattingTuple ft = MessageFormatter.arrayFormat(format, objects);
        LOG.info(ft.getMessage());
    }

    @Override
    public void log(long time, Function<Object[], String> logFunc, Object... objects) {
        LOG.info(logFunc.apply(objects));
    }

    public List<LogEvent> getLogs() {
        return Collections.emptyList();
    }
}
