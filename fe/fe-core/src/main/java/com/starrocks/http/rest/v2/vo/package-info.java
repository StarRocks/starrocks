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

/**
 * VO is the abbreviation of View Object, which is used to organize and encapsulate <br/>
 * the persistence layer data in the desired form and return it to the client, <br/>
 * while shielding the details of the persistence layer data (such as entity object). <br/>
 * The <a href="https://docs.oracle.com/cd/A97688_16/generic.903/bc_guide/bc_awhatisavo.htm">Oracle Docs</a> give more introduction.
 */
package com.starrocks.http.rest.v2.vo;