// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <exception>
#include <glog/logging.h>
#include "util/stack_util.h"

namespace starrocks {

class Exception : public std::exception
{
    public:
        Exception() { _stack = get_stack_trace(); }

        void log(const char* msg) {
            try {
                LOG(WARNING) << msg;
                LOG(WARNING) << _stack;
            } catch (...) {
                // do nothing
            }
        }
    private:
        std::string _stack;
};

} // namespace starrocks
