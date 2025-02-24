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

import com.starrocks.sql.ast.UserIdentity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileGroupProvider extends GroupProvider {
    public static final String TYPE = "file";
    private Map<String, Set<String>> userGroups;

    public FileGroupProvider(String name, Map<String, String> properties) {
        super(name, properties);
    }

    @Override
    public void init() {
        String groupFile = getProperties().get("group_file");
        userGroups = parseGroupFile(groupFile);
    }

    @Override
    public List<String> getGroup(UserIdentity userIdentity) {
        return new ArrayList<>(userGroups.get(userIdentity.getUser()));
    }

    public Map<String, Set<String>> parseGroupFile(String fileName) {
        Map<String, Set<String>> userGroups = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] parts = line.split(":");
                String groupName = parts[0];
                String[] users = parts[1].split(",");

                for (String user : users) {
                    user = user.trim();
                    userGroups.putIfAbsent(user, new HashSet<>());
                    userGroups.get(user).add(groupName);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return userGroups;
    }
}
