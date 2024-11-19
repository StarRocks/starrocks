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
package com.starrocks.epack.authorization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.starrocks.authentication.AuthenticationException;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PasswordPolicy {
    public static String PASSWORD_MIN_LENGTH = "PASSWORD_MIN_LENGTH";
    public static String PASSWORD_MIN_UPPER_CASE_CHARS = "PASSWORD_MIN_UPPER_CASE_CHARS";
    public static String PASSWORD_MIN_LOWER_CASE_CHARS = "PASSWORD_MIN_LOWER_CASE_CHARS";
    public static String PASSWORD_MIN_NUMERIC_CHARS = "PASSWORD_MIN_NUMERIC_CHARS";
    public static String PASSWORD_MIN_SPECIAL_CHARS = "PASSWORD_MIN_SPECIAL_CHARS";

    public static String PASSWORD_MAX_AGE_DAYS = "PASSWORD_MAX_AGE_DAYS";
    public static String PASSWORD_MAX_RETRIES = "PASSWORD_MAX_RETRIES";
    public static String PASSWORD_LOCKOUT_TIME_MINS = "PASSWORD_LOCKOUT_TIME_MINS";

    public static final Set<Character> SPECIAL_CHARACTERS =
            Sets.newHashSet('-', '~', '!', '@', '#', '$', '%', '^', '&', '<', '>', '=', '+');

    public static PasswordPolicy defaultPasswordPolicy = new PasswordPolicy(0L, "default", "default",
            ImmutableMap.of(
                    PASSWORD_MIN_LENGTH, "8",
                    PASSWORD_MIN_UPPER_CASE_CHARS, "1",
                    PASSWORD_MIN_LOWER_CASE_CHARS, "1",
                    PASSWORD_MIN_NUMERIC_CHARS, "1"));

    public static List<String> validPasswordProperties = ImmutableList.of(
            PASSWORD_MIN_LENGTH,
            PASSWORD_MIN_UPPER_CASE_CHARS,
            PASSWORD_MIN_LOWER_CASE_CHARS,
            PASSWORD_MIN_NUMERIC_CHARS,
            PASSWORD_MIN_SPECIAL_CHARS,
            PASSWORD_MAX_AGE_DAYS,
            PASSWORD_MAX_RETRIES,
            PASSWORD_LOCKOUT_TIME_MINS);

    private final Long policyId;
    private final String policyName;
    private final String comment;
    private final Map<String, String> properties;

    public PasswordPolicy(Long policyId, String policyName, String comment, Map<String, String> properties) {
        this.policyId = policyId;
        this.policyName = policyName;
        this.comment = comment;
        this.properties = new CaseInsensitiveMap<>(properties);
    }

    public Long getPolicyId() {
        return policyId;
    }

    public String getPolicyName() {
        return policyName;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Integer getPasswordMaxAgeDays() {
        String maxAgeDays = properties.get(PASSWORD_MAX_AGE_DAYS);
        if (maxAgeDays != null) {
            return Integer.parseInt(maxAgeDays);
        } else {
            return null;
        }
    }

    public Integer getPasswordMaxRetries() {
        String passwordMaxRetries = properties.get(PASSWORD_MAX_RETRIES);
        if (passwordMaxRetries != null) {
            return Integer.parseInt(passwordMaxRetries);
        } else {
            return null;
        }
    }

    public Integer getPasswordLockoutTimeMins() {
        String passwordLockTimeMins = properties.get(PASSWORD_LOCKOUT_TIME_MINS);
        if (passwordLockTimeMins != null) {
            return Integer.parseInt(passwordLockTimeMins);
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public void checkPasswordValid(String password) throws AuthenticationException {
        if (properties.containsKey(PASSWORD_MIN_LENGTH)) {
            int passwordMinLength = Integer.parseInt(properties.get(PASSWORD_MIN_LENGTH));
            if (password.length() < passwordMinLength) {
                throw new AuthenticationException("Does not meet the Password Policy restrictions. " +
                        "Password length cannot be less than " + passwordMinLength);
            }
        }

        if (properties.containsKey(PASSWORD_MIN_UPPER_CASE_CHARS)) {
            long upperCaseCount = password.chars().filter(Character::isUpperCase).count();
            int passwordMinUpperCaseChars = Integer.parseInt(properties.get(PASSWORD_MIN_UPPER_CASE_CHARS));
            if (upperCaseCount < passwordMinUpperCaseChars) {
                throw new AuthenticationException("Does not meet the Password Policy restrictions. " +
                        "The password contains at least " + passwordMinUpperCaseChars + " uppercase character");
            }
        }

        if (properties.containsKey(PASSWORD_MIN_LOWER_CASE_CHARS)) {
            long lowerCaseCount = password.chars().filter(Character::isLowerCase).count();
            int passwordMinLowerCaseChars = Integer.parseInt(properties.get(PASSWORD_MIN_LOWER_CASE_CHARS));
            if (lowerCaseCount < passwordMinLowerCaseChars) {
                throw new AuthenticationException("Does not meet the Password Policy restrictions. " +
                        "The password contains at least " + passwordMinLowerCaseChars + " lowercase character");
            }
        }

        if (properties.containsKey(PASSWORD_MIN_NUMERIC_CHARS)) {
            long digitCount = password.chars().filter(Character::isDigit).count();
            int passwordMinNumericChars = Integer.parseInt(properties.get(PASSWORD_MIN_NUMERIC_CHARS));
            if (digitCount < passwordMinNumericChars) {
                throw new AuthenticationException("Does not meet the Password Policy restrictions. " +
                        "The password contains at least " + passwordMinNumericChars + " numeric character");
            }
        }

        if (properties.containsKey(PASSWORD_MIN_SPECIAL_CHARS)) {
            long specialCaseCount = password.chars().filter(c -> SPECIAL_CHARACTERS.contains((char) c)).count();
            int passwordMinSpecialCaseChars = Integer.parseInt(properties.get(PASSWORD_MIN_SPECIAL_CHARS));
            if (specialCaseCount < passwordMinSpecialCaseChars) {
                throw new AuthenticationException("Does not meet the Password Policy restrictions. " +
                        "The password contains at least " + passwordMinSpecialCaseChars +
                        " special character('-', '~', '!', '@', '#', '$', '%', '^', '&', '<', '>', '=', '+')");
            }
        }
    }
}
