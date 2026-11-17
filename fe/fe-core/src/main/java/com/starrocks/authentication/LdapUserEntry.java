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

import javax.naming.directory.Attributes;

/**
 * A user entry as seen during authentication: its DN plus whatever attributes were requested.
 * <p>
 * Both authentication paths need the two together - search-and-bind searches the DN out of the
 * directory, direct bind computes it from the DN pattern - so they hand back the same shape and the
 * code after them is written once.
 *
 * @param dn         the distinguished name of the user, never null
 * @param attributes the requested attributes, null when none were requested or the read failed
 */
public record LdapUserEntry(String dn, Attributes attributes) {
}
