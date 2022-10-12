// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <hs/hs.h>
#include <string.h>

#include "common/status.h"

// find time zone with hyper scan.
namespace starrocks {
class TimezoneHsScan {
public:
    // match example: +08:00, -08:00 etc.
    const std::string hs_reg_pattern = "^[+|-]{1}\\d{2}\\:\\d{2}$";

    // compile pattern in hyper scan, we just need compile 1 times, then hs_scan multi times.
    Status compile() {
        if (hs_compile(hs_reg_pattern.c_str(), HS_FLAG_ALLOWEMPTY | HS_FLAG_DOTALL | HS_FLAG_UTF8 | HS_FLAG_SINGLEMATCH,
                       HS_MODE_BLOCK, NULL, &database, &compile_err) != HS_SUCCESS) {
            std::stringstream error;
            error << "Invalid regex expression: " << hs_reg_pattern << ": " << compile_err->message;
            hs_free_compile_error(compile_err);
            return Status::InvalidArgument(error.str());
        }

        if (hs_alloc_scratch(database, &scratch) != HS_SUCCESS) {
            std::stringstream error;
            error << "ERROR: Unable to allocate scratch space.";
            hs_free_database(database);
            return Status::InvalidArgument(error.str());
        }

        return Status::OK();
    }

    TimezoneHsScan() {}
    ~TimezoneHsScan() {
        if (scratch != nullptr) {
            hs_free_scratch(scratch);
        }

        if (database != nullptr) {
            hs_free_database(database);
        }
    }

    // store hyper scan compile result.
    hs_database_t* database = nullptr;

    // scratch space needs to be allocated.
    hs_scratch_t* scratch = nullptr;

private:
    // hyper scan compile error.
    hs_compile_error_t* compile_err = nullptr;
};
} // namespace starrocks