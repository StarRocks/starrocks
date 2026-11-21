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

package com.starrocks.persist.image;

import com.google.protobuf.Message;
import com.starrocks.common.Pair;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Mock;
import mockit.MockUp;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Shared file and global-state fixtures for the colocate/load image tests. */
final class LoadImageTestSupport {

    private LoadImageTestSupport() {
    }

    @FunctionalInterface
    interface SaveImage {
        void save(ImageWriterV3 writer) throws IOException;
    }

    static Path save(Path dir, SaveImage save) throws Exception {
        Path file = Files.createTempFile(dir, "load-image-", ".v3");
        try (OutputStream output = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(output, 123)) {
            save.save(writer);
            writer.finish();
        }
        return file;
    }

    static Path entries(Path dir, SectionId id, Message manager, List<? extends Message> entries) throws Exception {
        return save(dir, writer -> {
            try (SectionWriter section = writer.beginSection(id)) {
                section.writeManager(manager);
                for (Message entry : entries) {
                    section.writeEntry(entry);
                }
            }
        });
    }

    static List<IndexEntryPB> load(Path file, List<Pair<SectionId, SectionLoader>> loaders) throws Exception {
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            reader.load(loaders);
            return reader.sections();
        }
    }

    static GlobalStateMgr global() {
        GlobalStateMgr global = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(global.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return global;
            }

            @Mock
            public boolean isCheckpointThread() {
                return true;
            }
        };
        Mockito.when(global.getSqlParser()).thenReturn(new SqlParser(AstBuilder.getInstance()));
        transactions(global);
        return global;
    }

    static GlobalTransactionMgr transactions(GlobalStateMgr global) {
        GlobalTransactionMgr transactions = new GlobalTransactionMgr(global);
        Mockito.when(global.getGlobalTransactionMgr()).thenReturn(transactions);
        return transactions;
    }
}
