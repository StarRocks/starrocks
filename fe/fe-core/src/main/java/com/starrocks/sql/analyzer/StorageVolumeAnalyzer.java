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

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.DescStorageVolumeStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowStorageVolumesStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.List;

public class StorageVolumeAnalyzer {
    public static void analyze(StatementBase stmt, ConnectContext session) {
        new StorageVolumeAnalyzer.StorageVolumeAnalyzerVisitor().visit(stmt, session);
    }

    static class StorageVolumeAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateStorageVolumeStatement(CreateStorageVolumeStmt statement, ConnectContext context) {
            String svName = statement.getName();
            if (Strings.isNullOrEmpty(svName)) {
                throw new SemanticException("'storage volume name' can not be null or empty");
            }
            if (svName.equals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME)) {
                throw new SemanticException(String.format("%s can not be created by SQL",
                        StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));
            }

            List<String> locations = statement.getStorageLocations();
            if (locations.isEmpty()) {
                throw new SemanticException("'storage volume locations' can not be empty");
            }
            for (String location : locations) {
                if (location.isEmpty()) {
                    throw new SemanticException("'location in storage volume' can not be empty");
                }
            }

            String svType = statement.getStorageVolumeType();
            if (Strings.isNullOrEmpty((svType))) {
                throw new SemanticException("'storage volume type' can not be null or empty");
            }

            FeNameFormat.checkStorageVolumeName(svName);
            return null;
        }

        @Override
        public Void visitAlterStorageVolumeStatement(AlterStorageVolumeStmt statement, ConnectContext context) {
            String svName = statement.getName();
            if (Strings.isNullOrEmpty(svName)) {
                throw new SemanticException("'storage volume name' can not be null or empty");
            }
            return null;
        }

        @Override
        public Void visitDropStorageVolumeStatement(DropStorageVolumeStmt statement, ConnectContext context) {
            String svName = statement.getName();
            if (Strings.isNullOrEmpty(svName)) {
                throw new SemanticException("'storage volume name' can not be null or empty");
            }
            return null;
        }

        @Override
        public Void visitShowStorageVolumesStatement(ShowStorageVolumesStmt statement, ConnectContext context) {
            return null;
        }

        @Override
        public Void visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, ConnectContext context) {
            String svName = statement.getName();
            if (Strings.isNullOrEmpty(svName)) {
                throw new SemanticException("'storage volume name' can not be null or empty");
            }

            StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
            try {
                if (!storageVolumeMgr.exists(svName)) {
                    throw new SemanticException("Unknown storage volume: %s", svName);
                }
            } catch (DdlException e) {
                throw new SemanticException("Failed to get storage volume", e);
            }

            return null;
        }

        @Override
        public Void visitSetDefaultStorageVolumeStatement(SetDefaultStorageVolumeStmt statement,
                                                          ConnectContext context) {
            String svName = statement.getName();
            if (Strings.isNullOrEmpty(svName)) {
                throw new SemanticException("'storage volume name' can not be null or empty");
            }
            return null;
        }
    }
}
