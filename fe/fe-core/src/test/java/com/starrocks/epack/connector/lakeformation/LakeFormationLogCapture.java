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

package com.starrocks.epack.connector.lakeformation;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Collects what one class logged while a block ran. */
final class LakeFormationLogCapture implements AutoCloseable {

    private final List<LogEvent> events = new CopyOnWriteArrayList<>();
    private final Logger logger;
    private final AbstractAppender appender;
    private final Level levelToRestore;

    private LakeFormationLogCapture(Class<?> loggingClass) {
        this.logger = (Logger) LogManager.getLogger(loggingClass);
        this.appender = new AbstractAppender("lf-log-capture-" + UUID.randomUUID(), null, null) {
            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        appender.start();
        logger.addAppender(appender);
        // The test configuration keeps the root logger at WARN, so an INFO line would never reach an
        // appender at all - and a test asserting one would fail for the wrong reason.
        this.levelToRestore = logger.getLevel();
        logger.setLevel(Level.TRACE);
    }

    /** Starts capturing what the given class logs. Use in try-with-resources; close detaches. */
    static LakeFormationLogCapture on(Class<?> loggingClass) {
        return new LakeFormationLogCapture(loggingClass);
    }

    List<String> messagesAt(Level level) {
        List<String> messages = new ArrayList<>();
        for (LogEvent event : events) {
            if (event.getLevel() == level) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        }
        return messages;
    }

    /** The one message expected at this level; fails if there were none or several. */
    String onlyMessageAt(Level level) {
        List<String> messages = messagesAt(level);
        assertEquals(1, messages.size(), "expected exactly one " + level + " line, got " + messages);
        return messages.get(0);
    }

    @Override
    public void close() {
        logger.setLevel(levelToRestore);
        logger.removeAppender(appender);
        appender.stop();
    }
}
