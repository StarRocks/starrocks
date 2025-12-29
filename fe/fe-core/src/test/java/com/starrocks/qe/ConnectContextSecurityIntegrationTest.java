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

package com.starrocks.qe;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases for ConnectContext security integration functionality
 */
public class ConnectContextSecurityIntegrationTest {

    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        connectContext = new ConnectContext();
    }

    /**
     * Test default security integration value
     */
    @Test
    public void testDefaultSecurityIntegration() {
        // Default security integration should be "native"
        String defaultSecurityIntegration = connectContext.getSecurityIntegration();
        Assertions.assertEquals("native", defaultSecurityIntegration);
    }

    /**
     * Test setting security integration to LDAP
     */
    @Test
    public void testSetSecurityIntegrationToLDAP() {
        String ldapIntegration = "ldap_provider";
        connectContext.setSecurityIntegration(ldapIntegration);
        
        String retrievedIntegration = connectContext.getSecurityIntegration();
        Assertions.assertEquals(ldapIntegration, retrievedIntegration);
    }

    /**
     * Test setting security integration to SAML
     */
    @Test
    public void testSetSecurityIntegrationToSAML() {
        String samlIntegration = "saml_provider";
        connectContext.setSecurityIntegration(samlIntegration);
        
        String retrievedIntegration = connectContext.getSecurityIntegration();
        Assertions.assertEquals(samlIntegration, retrievedIntegration);
    }

    /**
     * Test setting security integration to OpenID Connect
     */
    @Test
    public void testSetSecurityIntegrationToOIDC() {
        String oidcIntegration = "oidc_provider";
        connectContext.setSecurityIntegration(oidcIntegration);
        
        String retrievedIntegration = connectContext.getSecurityIntegration();
        Assertions.assertEquals(oidcIntegration, retrievedIntegration);
    }

    /**
     * Test setting security integration to null
     */
    @Test
    public void testSetSecurityIntegrationToNull() {
        // First set to a non-null value
        connectContext.setSecurityIntegration("test_provider");
        Assertions.assertEquals("test_provider", connectContext.getSecurityIntegration());
        
        // Then set to null
        connectContext.setSecurityIntegration(null);
        String retrievedIntegration = connectContext.getSecurityIntegration();
        Assertions.assertNull(retrievedIntegration);
    }

    /**
     * Test setting security integration to empty string
     */
    @Test
    public void testSetSecurityIntegrationToEmptyString() {
        connectContext.setSecurityIntegration("");
        
        String retrievedIntegration = connectContext.getSecurityIntegration();
        Assertions.assertEquals("", retrievedIntegration);
    }

    /**
     * Test multiple calls to setSecurityIntegration
     */
    @Test
    public void testMultipleSetSecurityIntegrationCalls() {
        // Set to LDAP
        connectContext.setSecurityIntegration("ldap");
        Assertions.assertEquals("ldap", connectContext.getSecurityIntegration());
        
        // Change to SAML
        connectContext.setSecurityIntegration("saml");
        Assertions.assertEquals("saml", connectContext.getSecurityIntegration());
        
        // Change back to native
        connectContext.setSecurityIntegration("native");
        Assertions.assertEquals("native", connectContext.getSecurityIntegration());
    }

    /**
     * Test setting security integration with special characters
     */
    @Test
    public void testSetSecurityIntegrationWithSpecialCharacters() {
        String integrationWithSpecialChars = "ldap_provider_2024@company.com";
        connectContext.setSecurityIntegration(integrationWithSpecialChars);
        
        String retrievedIntegration = connectContext.getSecurityIntegration();
        Assertions.assertEquals(integrationWithSpecialChars, retrievedIntegration);
    }

    /**
     * Test security integration persistence across context operations
     */
    @Test
    public void testSecurityIntegrationPersistenceAcrossOperations() {
        String testIntegration = "persistent_provider";
        connectContext.setSecurityIntegration(testIntegration);
        
        // Perform other operations on the context that shouldn't affect security integration
        connectContext.setDatabase("test_db");
        connectContext.setQualifiedUser("test_user");
        
        // Security integration should remain unchanged
        String retrievedIntegration = connectContext.getSecurityIntegration();
        Assertions.assertEquals(testIntegration, retrievedIntegration);
    }

    /**
     * Test security integration getter returns the same reference for same value
     */
    @Test
    public void testSecurityIntegrationReferenceConsistency() {
        String testIntegration = "test_provider";
        connectContext.setSecurityIntegration(testIntegration);
        
        String retrieved1 = connectContext.getSecurityIntegration();
        String retrieved2 = connectContext.getSecurityIntegration();
        
        // Should return the same value (content equality)
        Assertions.assertEquals(retrieved1, retrieved2);
        Assertions.assertEquals(testIntegration, retrieved1);
        Assertions.assertEquals(testIntegration, retrieved2);
    }

    /**
     * Test security integration with very long string
     */
    @Test
    public void testSecurityIntegrationWithLongString() {
        StringBuilder longIntegration = new StringBuilder("very_long_integration_name_");
        for (int i = 0; i < 100; i++) {
            longIntegration.append("provider_").append(i).append("_");
        }
        String longIntegrationString = longIntegration.toString();
        
        connectContext.setSecurityIntegration(longIntegrationString);
        
        String retrievedIntegration = connectContext.getSecurityIntegration();
        Assertions.assertEquals(longIntegrationString, retrievedIntegration);
    }

    /**
     * Test security integration field is independent of other context fields
     */
    @Test
    public void testSecurityIntegrationIndependence() {
        // Set security integration
        connectContext.setSecurityIntegration("ldap_provider");
        
        // Set other context fields
        connectContext.setDatabase("test_database");
        connectContext.setQualifiedUser("test_user@company.com");
        
        // Verify security integration is not affected
        Assertions.assertEquals("ldap_provider", connectContext.getSecurityIntegration());
        
        // Change other fields
        connectContext.setDatabase("another_database");
        connectContext.setQualifiedUser("another_user");
        
        // Security integration should still be unchanged
        Assertions.assertEquals("ldap_provider", connectContext.getSecurityIntegration());
    }

    /**
     * Test initialization of new ConnectContext instances
     */
    @Test
    public void testNewConnectContextInstances() {
        ConnectContext context1 = new ConnectContext();
        ConnectContext context2 = new ConnectContext();
        
        // Both should have the default security integration
        Assertions.assertEquals("native", context1.getSecurityIntegration());
        Assertions.assertEquals("native", context2.getSecurityIntegration());
        
        // Setting one shouldn't affect the other
        context1.setSecurityIntegration("ldap");
        Assertions.assertEquals("ldap", context1.getSecurityIntegration());
        Assertions.assertEquals("native", context2.getSecurityIntegration());
        
        // Setting the second shouldn't affect the first
        context2.setSecurityIntegration("saml");
        Assertions.assertEquals("ldap", context1.getSecurityIntegration());
        Assertions.assertEquals("saml", context2.getSecurityIntegration());
    }
}