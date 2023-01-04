//
// Copyright 1999-2006 and onwards Google, Inc.
//
// Useful string functions and so forth.  This is a grab-bag file.
//
// You might also want to look at memutil.h, which holds mem*()
// equivalents of a lot of the str*() functions in string.h,
// eg memstr, mempbrk, etc.
//
// These functions work fine for UTF-8 strings as long as you can
// consider them to be just byte strings.  For example, due to the
// design of UTF-8 you do not need to worry about accidental matches,
// as long as all your inputs are valid UTF-8 (use \uHHHH, not \xHH or \oOOO).
//
// Caveats:
// * all the lengths in these routines refer to byte counts,
//   not character counts.
// * case-insensitivity in these routines assumes that all the letters
//   in question are in the range A-Z or a-z.
//
// If you need Unicode specific processing (for example being aware of
// Unicode character boundaries, or knowledge of Unicode casing rules,
// or various forms of equivalence and normalization), take a look at
// files in i18n/utf8.

#pragma once

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#ifndef _MSC_VER
#include <strings.h> // for strcasecmp, but msvc does not have this header
#endif

#include <functional>
using std::binary_function;
using std::less;
#include <string>
using std::string;
#include <vector>
using std::vector;

#include "gutil/integral_types.h"
#include "gutil/port.h"
#include "gutil/strings/stringpiece.h"

// Returns whether str begins with prefix.
inline bool HasPrefixString(const StringPiece& str, const StringPiece& prefix) {
    return str.starts_with(prefix);
}

// Returns whether str ends with suffix.
inline bool HasSuffixString(const StringPiece& str, const StringPiece& suffix) {
    return str.ends_with(suffix);
}