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

import org.junit.Assert;
import org.junit.Test;

import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LDAPGroupCacheMgrTest {

    @Test
    public void testGetGroupType() throws Exception {
        InitialDirContext initialDirContext = mock(InitialDirContext.class);
        mockGroupType(initialDirContext, "cn=test1,ou=Group,dc=example,dc=com", "groupofUniqueNames");
        Assert.assertEquals(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES,
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test1,ou=Group,dc=example,dc=com"));
        Assert.assertTrue(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPES.contains(
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test1,ou=Group,dc=example,dc=com")));
        mockGroupType(initialDirContext, "cn=test11,ou=Group,dc=example,dc=com", "groupOfUniqueNames");
        Assert.assertEquals(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES,
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test11,ou=Group,dc=example,dc=com"));
        Assert.assertTrue(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPES.contains(
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test11,ou=Group,dc=example,dc=com")));

        mockGroupType(initialDirContext, "cn=test2,ou=Group,dc=example,dc=com", "groupOfNames");
        Assert.assertEquals(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES,
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test2,ou=Group,dc=example,dc=com"));
        Assert.assertTrue(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPES.contains(
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test2,ou=Group,dc=example,dc=com")));
        mockGroupType(initialDirContext, "cn=test22,ou=Group,dc=example,dc=com", "groupofNames");
        Assert.assertEquals(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES,
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test22,ou=Group,dc=example,dc=com"));
        Assert.assertTrue(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPES.contains(
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test22,ou=Group,dc=example,dc=com")));

        mockGroupType(initialDirContext, "cn=test3,ou=Group,dc=example,dc=com", "posixGroup");
        Assert.assertEquals(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP,
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test3,ou=Group,dc=example,dc=com"));
        Assert.assertTrue(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPES.contains(
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test3,ou=Group,dc=example,dc=com")));
        mockGroupType(initialDirContext, "cn=test33,ou=Group,dc=example,dc=com", "posixgroup");
        Assert.assertEquals(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP,
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test33,ou=Group,dc=example,dc=com"));
        Assert.assertTrue(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPES.contains(
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test33,ou=Group,dc=example,dc=com")));

        mockGroupType(initialDirContext, "cn=test4,ou=Group,dc=example,dc=com", "group");
        Assert.assertEquals(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPE_AD_GROUP,
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test4,ou=Group,dc=example,dc=com"));
        Assert.assertTrue(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPES.contains(
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test4,ou=Group,dc=example,dc=com")));
        mockGroupType(initialDirContext, "cn=test44,ou=Group,dc=example,dc=com", "Group");
        Assert.assertEquals(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPE_AD_GROUP,
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test44,ou=Group,dc=example,dc=com"));
        Assert.assertTrue(LDAPGroupCacheMgr.SUPPORTED_LDAP_GROUP_TYPES.contains(
                LDAPGroupCacheMgr.getGroupType(initialDirContext, "cn=test44,ou=Group,dc=example,dc=com")));
    }

    private void mockGroupType(DirContext context, String groupDN, String groupType) throws Exception {
        BasicAttributes basicAttributes = new BasicAttributes();
        basicAttributes.put("objectClass", groupType);
        when(context.getAttributes(groupDN)).thenReturn(basicAttributes);
    }
}
