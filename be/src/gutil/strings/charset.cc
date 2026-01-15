// Copyright 2008 Google Inc. All Rights Reserved.

#include "gutil/strings/charset.h"

#include <cstring>

namespace strings {

CharSet::CharSet() {
    memset(static_cast<void*>(this), 0, sizeof(*this));
}

CharSet::CharSet(const char* characters) {
    memset(static_cast<void*>(this), 0, sizeof(*this));
    for (; *characters != '\0'; ++characters) {
        Add(*characters);
    }
}

CharSet::CharSet(const CharSet& other) {
    memcpy(static_cast<void*>(this), static_cast<const void*>(&other), sizeof(*this));
}

} // namespace strings
