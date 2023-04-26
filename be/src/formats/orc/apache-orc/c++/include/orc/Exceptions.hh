/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <stdexcept>
#include <string>

#include "orc/orc-config.hh"

namespace orc {

class NotImplementedYet : public std::logic_error {
public:
    explicit NotImplementedYet(const std::string& what_arg);
    explicit NotImplementedYet(const char* what_arg);
    ~NotImplementedYet() ORC_NOEXCEPT override;
    NotImplementedYet(const NotImplementedYet&);

private:
    NotImplementedYet& operator=(const NotImplementedYet&) = delete;
};

class ParseError : public std::runtime_error {
public:
    explicit ParseError(const std::string& what_arg);
    explicit ParseError(const char* what_arg);
    ~ParseError() ORC_NOEXCEPT override;
    ParseError(const ParseError&);

private:
    ParseError& operator=(const ParseError&) = delete;
};

class InvalidArgument : public std::runtime_error {
public:
    explicit InvalidArgument(const std::string& what_arg);
    explicit InvalidArgument(const char* what_arg);
    ~InvalidArgument() ORC_NOEXCEPT override;
    InvalidArgument(const InvalidArgument&);

private:
    InvalidArgument& operator=(const InvalidArgument&) = delete;
};
} // namespace orc
