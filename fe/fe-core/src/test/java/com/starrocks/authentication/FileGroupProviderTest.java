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

package com.starrocks.authentication;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

public class FileGroupProviderTest {
    @Test
    public void testFileGroupProvider() throws DdlException {
        new MockUp<FileGroupProvider>() {
            @Mock
            public InputStream getPath(String groupFileUrl) throws IOException {
                String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath() + "/" + "file_group";
                return new FileInputStream(path);
            }
        };

        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String groupName = "file_group_provider";
        Map<String, String> properties = Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "file",
                FileGroupProvider.GROUP_FILE_URL, "file_group");

        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        FileGroupProvider fileGroupProvider = (FileGroupProvider) authenticationMgr.getGroupProvider(groupName);

        Set<String> groups = fileGroupProvider.getGroup(new UserIdentity("harbor", "%"), "harbor");
        Assertions.assertTrue(groups.contains("group1"));
        Assertions.assertTrue(groups.contains("group2"));
    }
}
