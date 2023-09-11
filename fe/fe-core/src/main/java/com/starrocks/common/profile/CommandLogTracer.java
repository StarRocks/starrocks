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

import com.google.common.collect.Lists;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.util.List;
import java.util.function.Function;

public class CommandLogTracer extends LogTracer {
    private final List<LogEvent> logs = Lists.newLinkedList();

    public void log(long time, String content) {
        this.logs.add(new LogEvent(time, content));
    }

    public void log(long time, String format, Object... objects) {
        FormattingTuple ft = MessageFormatter.arrayFormat(format, objects);
        this.logs.add(new LogEvent(time, ft.getMessage()));
    }

    @Override
    public void log(long time, Function<Object[], String> logFunc, Object... objects) {
        String log = logFunc.apply(objects);
        this.logs.add(new LogEvent(time, log));
    }

    public List<LogEvent> getLogs() {
        return this.logs;
    }
}
