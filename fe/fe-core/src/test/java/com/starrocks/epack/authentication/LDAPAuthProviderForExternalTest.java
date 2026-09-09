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

package com.starrocks.epack.authentication;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

/**
 * The enterprise {@code type='ldap'} path takes the session identity from the entry its search
 * found. Getting that read wrong means {@code current_user()} and the audit log report a principal
 * the directory does not hold, which no other test on this path would notice.
 */
public class LDAPAuthProviderForExternalTest {

    private static SearchResult entryWith(BasicAttributes attributes) {
        return new SearchResult("uid=Allen,ou=people,dc=example,dc=com", null, attributes);
    }

    /**
     * Test case: the entry carries the attribute the search filtered on.
     * Test point: its own spelling is returned, not the one the client typed.
     */
    @Test
    public void testReadsTheDirectorySpelling() throws Exception {
        BasicAttributes attributes = new BasicAttributes(true);
        attributes.put("uid", "Allen");
        Assertions.assertEquals("Allen",
                LDAPAuthProviderForExternal.readSearchAttrValue(entryWith(attributes), "uid"));
    }

    /**
     * Test case: the directory returned no attributes, a different attribute, or a non-textual value.
     * Test point: each reports null, so the caller falls back instead of handing a wrong or
     *             non-string principal to the session.
     */
    @Test
    public void testReportsNullWhenTheNameIsNotThere() throws Exception {
        Assertions.assertNull(LDAPAuthProviderForExternal.readSearchAttrValue(entryWith(null), "uid"));

        BasicAttributes other = new BasicAttributes(true);
        other.put("mail", "allen@example.com");
        Assertions.assertNull(LDAPAuthProviderForExternal.readSearchAttrValue(entryWith(other), "uid"));

        BasicAttributes binary = new BasicAttributes(true);
        binary.put("uid", new byte[] {1, 2, 3});
        Assertions.assertNull(LDAPAuthProviderForExternal.readSearchAttrValue(entryWith(binary), "uid"));

        BasicAttributes empty = new BasicAttributes(true);
        empty.put(new javax.naming.directory.BasicAttribute("uid"));
        Assertions.assertNull(LDAPAuthProviderForExternal.readSearchAttrValue(entryWith(empty), "uid"));
    }
}
