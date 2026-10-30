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

// Keep starrocks_be loadable on hosts whose glibc is older than the build image's.
//
// A symbol's `@GLIBC_x.y` suffix is stamped by the linker, not the compiler: an
// object file references the bare name and ld copies whatever *default* version
// libc.so.6/libm.so.6 exports for it. Building in the Ubuntu 24.04 dev-env image
// (glibc 2.39) therefore yields references such as `strlcpy@GLIBC_2.38`, and the
// loader on a glibc 2.35 host (Ubuntu 22.04) refuses them outright:
//
//     starrocks_be: /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.38' not found
//
// Defining these entry points here removes the references altogether, because a
// definition present in the link always wins over a shared library's. That also
// covers references coming out of the prebuilt thirdparty archives, which a
// per-TU `.symver` pin could not - those objects are not recompiled.
//
// Two constraints this file must keep:
//
//  1. Every forwarding target is reached through an explicit `.symver` alias, and
//     no feature-test macro is defined. Calling the plain name would not work:
//     on glibc >= 2.38, _GNU_SOURCE implies _ISOC2X_SOURCE, so even in a C11 TU
//     <stdlib.h> rewrites strtol() into __isoc23_strtol() - i.e. the forwarding
//     body becomes an endless self-call.
//  2. Version nodes are architecture-specific. The x86-64 ABI starts at
//     GLIBC_2.2.5, while the aarch64 port arrived in glibc 2.17 and exports
//     nothing older, so a hard-coded 2.2.5 would leave undefined versioned
//     references there. Pick them per architecture below.
//  3. The definitions stay out of .dynsym (hidden visibility). An unversioned
//     definition satisfies a versioned request, so an exported fmod() would let
//     ld.so resolve this binary's own `fmod@GLIBC_2.2.5` reference back to the
//     wrapper below. Under -O2 that is a tail jump: 100% CPU, no stack growth.
//
// Only compiled when build.sh is given --with-glibc-compat, and deliberately as
// C11 (see be/src/service/CMakeLists.txt): the variadic forwarding wrappers are
// plain va_list plumbing, and the file must stay out of reach of the -std=gnu++23
// header behaviour the rest of BE is compiled with.
//
// Not covered, because nothing references them today: mbrtoc8/c8rtomb (C++23
// char8_t), the C23 stdc_* bit utilities and the newer syscall wrappers
// (pidfd_*, fsopen, mount_setattr, process_madvise). build-support/check_glibc_abi.sh
// fails the build if any of them - or anything else too new - ever shows up.

// Only ISO C headers, and only ISO C declarations are used from them: BE injects
// -D_XOPEN_SOURCE=500 globally, which switches _DEFAULT_SOURCE off, so glibc's
// locale_t, strnlen(), wcsnlen() and getrandom() are all invisible here.
#include <errno.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>

// ABI-identical to glibc's own locale type (a pointer to struct __locale_struct);
// used only to pass the argument straight through to the real *_l entry point.
typedef void* sr_locale_t;

// ---------------------------------------------------------------------------
// Aliases for the old, still-exported implementations. Versions verified against
// the glibc shipped in both the 24.04 build image and the 22.04 target host.
// ---------------------------------------------------------------------------
// The oldest node each architecture actually exports (verified with `nm -D` against
// the x86-64 and aarch64 glibc of both the build image and the target host).
#if defined(__x86_64__)
#define SR_V_BASE 2.2.5  // earliest x86-64 node
#define SR_V_L 2.3       // strtol_l / strtoul_l / wcsto*_l
#define SR_V_LL 2.3.3    // strtoll_l / strtoull_l
#define SR_V_C99 2.7     // __isoc99_v*scanf
#elif defined(__aarch64__)
#define SR_V_BASE 2.17   // the aarch64 port starts here; nothing older exists
#define SR_V_L 2.17
#define SR_V_LL 2.17
#define SR_V_C99 2.17
#else
#error "glibc_compat.c: add this architecture's glibc symbol version nodes"
#endif

// Two levels, so a version macro is expanded before it is stringified.
#define SR_STR_(x) #x
#define SR_STR(x) SR_STR_(x)
#define SR_ALIAS(alias, sym, ver) __asm__(".symver " SR_STR(alias) ", " SR_STR(sym) "@GLIBC_" SR_STR(ver))

// libm entry points that merely gained a new version node in 2.38/2.39.
SR_ALIAS(__sr_fmod, fmod, SR_V_BASE);
SR_ALIAS(__sr_fmodf, fmodf, SR_V_BASE);
SR_ALIAS(__sr_exp10, exp10, SR_V_BASE);
extern double __sr_fmod(double, double);
extern float __sr_fmodf(float, float);
extern double __sr_exp10(double);

// The classic strtol family.
SR_ALIAS(__sr_strtol, strtol, SR_V_BASE);
SR_ALIAS(__sr_strtoll, strtoll, SR_V_BASE);
SR_ALIAS(__sr_strtoul, strtoul, SR_V_BASE);
SR_ALIAS(__sr_strtoull, strtoull, SR_V_BASE);
SR_ALIAS(__sr_strtoimax, strtoimax, SR_V_BASE);
SR_ALIAS(__sr_strtoumax, strtoumax, SR_V_BASE);
SR_ALIAS(__sr_strtol_l, strtol_l, SR_V_L);
SR_ALIAS(__sr_strtoul_l, strtoul_l, SR_V_L);
SR_ALIAS(__sr_strtoll_l, strtoll_l, SR_V_LL);
SR_ALIAS(__sr_strtoull_l, strtoull_l, SR_V_LL);
extern long __sr_strtol(const char*, char**, int);
extern long long __sr_strtoll(const char*, char**, int);
extern unsigned long __sr_strtoul(const char*, char**, int);
extern unsigned long long __sr_strtoull(const char*, char**, int);
extern intmax_t __sr_strtoimax(const char*, char**, int);
extern uintmax_t __sr_strtoumax(const char*, char**, int);
extern long __sr_strtol_l(const char*, char**, int, sr_locale_t);
extern unsigned long __sr_strtoul_l(const char*, char**, int, sr_locale_t);
extern long long __sr_strtoll_l(const char*, char**, int, sr_locale_t);
extern unsigned long long __sr_strtoull_l(const char*, char**, int, sr_locale_t);

// ... and its wide-character twin.
SR_ALIAS(__sr_wcstol, wcstol, SR_V_BASE);
SR_ALIAS(__sr_wcstoll, wcstoll, SR_V_BASE);
SR_ALIAS(__sr_wcstoul, wcstoul, SR_V_BASE);
SR_ALIAS(__sr_wcstoull, wcstoull, SR_V_BASE);
SR_ALIAS(__sr_wcstoimax, wcstoimax, SR_V_BASE);
SR_ALIAS(__sr_wcstoumax, wcstoumax, SR_V_BASE);
SR_ALIAS(__sr_wcstol_l, wcstol_l, SR_V_L);
SR_ALIAS(__sr_wcstoul_l, wcstoul_l, SR_V_L);
SR_ALIAS(__sr_wcstoll_l, wcstoll_l, SR_V_L);
SR_ALIAS(__sr_wcstoull_l, wcstoull_l, SR_V_L);
extern long __sr_wcstol(const wchar_t*, wchar_t**, int);
extern long long __sr_wcstoll(const wchar_t*, wchar_t**, int);
extern unsigned long __sr_wcstoul(const wchar_t*, wchar_t**, int);
extern unsigned long long __sr_wcstoull(const wchar_t*, wchar_t**, int);
extern intmax_t __sr_wcstoimax(const wchar_t*, wchar_t**, int);
extern uintmax_t __sr_wcstoumax(const wchar_t*, wchar_t**, int);
extern long __sr_wcstol_l(const wchar_t*, wchar_t**, int, sr_locale_t);
extern unsigned long __sr_wcstoul_l(const wchar_t*, wchar_t**, int, sr_locale_t);
extern long long __sr_wcstoll_l(const wchar_t*, wchar_t**, int, sr_locale_t);
extern unsigned long long __sr_wcstoull_l(const wchar_t*, wchar_t**, int, sr_locale_t);

// The C99 scanf family - what a modern compiler binds to anyway.
SR_ALIAS(__sr_vscanf, __isoc99_vscanf, SR_V_C99);
SR_ALIAS(__sr_vfscanf, __isoc99_vfscanf, SR_V_C99);
SR_ALIAS(__sr_vsscanf, __isoc99_vsscanf, SR_V_C99);
SR_ALIAS(__sr_vwscanf, __isoc99_vwscanf, SR_V_C99);
SR_ALIAS(__sr_vfwscanf, __isoc99_vfwscanf, SR_V_C99);
SR_ALIAS(__sr_vswscanf, __isoc99_vswscanf, SR_V_C99);
extern int __sr_vscanf(const char*, va_list);
extern int __sr_vfscanf(FILE*, const char*, va_list);
extern int __sr_vsscanf(const char*, const char*, va_list);
extern int __sr_vwscanf(const wchar_t*, va_list);
extern int __sr_vfwscanf(FILE*, const wchar_t*, va_list);
extern int __sr_vswscanf(const wchar_t*, const wchar_t*, va_list);

// getrandom() is glibc 2.25 and declared under __USE_GNU only; declare it here
// rather than turning on a feature-test macro (see rule 1). `long` is the ABI of
// ssize_t on every platform BE builds for.
extern long getrandom(void* buffer, size_t length, unsigned int flags);

// Everything below is hidden: needed by this link only, never exported (rule 3).
#pragma GCC visibility push(hidden)

// ---------------------------------------------------------------------------
// libm: same implementation as before 2.38, only a lower version node. fmod() is
// exact per IEEE 754, so results are bit-identical; the old code is just slower.
// ---------------------------------------------------------------------------
double fmod(double x, double y) {
    return __sr_fmod(x, y);
}
float fmodf(float x, float y) {
    return __sr_fmodf(x, y);
}
double exp10(double x) {
    return __sr_exp10(x);
}

// ---------------------------------------------------------------------------
// The C23 strtol family (glibc >= 2.38). Only difference from the classic entry
// points: with base 0 or 2, C23 also accepts a 0b/0B prefix.
// ---------------------------------------------------------------------------
long __isoc23_strtol(const char* s, char** end, int base) {
    return __sr_strtol(s, end, base);
}
long long __isoc23_strtoll(const char* s, char** end, int base) {
    return __sr_strtoll(s, end, base);
}
unsigned long __isoc23_strtoul(const char* s, char** end, int base) {
    return __sr_strtoul(s, end, base);
}
unsigned long long __isoc23_strtoull(const char* s, char** end, int base) {
    return __sr_strtoull(s, end, base);
}
intmax_t __isoc23_strtoimax(const char* s, char** end, int base) {
    return __sr_strtoimax(s, end, base);
}
uintmax_t __isoc23_strtoumax(const char* s, char** end, int base) {
    return __sr_strtoumax(s, end, base);
}
long __isoc23_strtol_l(const char* s, char** end, int base, sr_locale_t loc) {
    return __sr_strtol_l(s, end, base, loc);
}
long long __isoc23_strtoll_l(const char* s, char** end, int base, sr_locale_t loc) {
    return __sr_strtoll_l(s, end, base, loc);
}
unsigned long __isoc23_strtoul_l(const char* s, char** end, int base, sr_locale_t loc) {
    return __sr_strtoul_l(s, end, base, loc);
}
unsigned long long __isoc23_strtoull_l(const char* s, char** end, int base, sr_locale_t loc) {
    return __sr_strtoull_l(s, end, base, loc);
}

long __isoc23_wcstol(const wchar_t* s, wchar_t** end, int base) {
    return __sr_wcstol(s, end, base);
}
long long __isoc23_wcstoll(const wchar_t* s, wchar_t** end, int base) {
    return __sr_wcstoll(s, end, base);
}
unsigned long __isoc23_wcstoul(const wchar_t* s, wchar_t** end, int base) {
    return __sr_wcstoul(s, end, base);
}
unsigned long long __isoc23_wcstoull(const wchar_t* s, wchar_t** end, int base) {
    return __sr_wcstoull(s, end, base);
}
intmax_t __isoc23_wcstoimax(const wchar_t* s, wchar_t** end, int base) {
    return __sr_wcstoimax(s, end, base);
}
uintmax_t __isoc23_wcstoumax(const wchar_t* s, wchar_t** end, int base) {
    return __sr_wcstoumax(s, end, base);
}
long __isoc23_wcstol_l(const wchar_t* s, wchar_t** end, int base, sr_locale_t loc) {
    return __sr_wcstol_l(s, end, base, loc);
}
long long __isoc23_wcstoll_l(const wchar_t* s, wchar_t** end, int base, sr_locale_t loc) {
    return __sr_wcstoll_l(s, end, base, loc);
}
unsigned long __isoc23_wcstoul_l(const wchar_t* s, wchar_t** end, int base, sr_locale_t loc) {
    return __sr_wcstoul_l(s, end, base, loc);
}
unsigned long long __isoc23_wcstoull_l(const wchar_t* s, wchar_t** end, int base, sr_locale_t loc) {
    return __sr_wcstoull_l(s, end, base, loc);
}

// ---------------------------------------------------------------------------
// The C23 scanf family (glibc >= 2.38). Forwarded to the C99 entry points; the
// only feature lost is the C23 %b conversion.
// ---------------------------------------------------------------------------
int __isoc23_scanf(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int r = __sr_vscanf(fmt, ap);
    va_end(ap);
    return r;
}
int __isoc23_fscanf(FILE* f, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int r = __sr_vfscanf(f, fmt, ap);
    va_end(ap);
    return r;
}
int __isoc23_sscanf(const char* s, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int r = __sr_vsscanf(s, fmt, ap);
    va_end(ap);
    return r;
}
int __isoc23_vscanf(const char* fmt, va_list ap) {
    return __sr_vscanf(fmt, ap);
}
int __isoc23_vfscanf(FILE* f, const char* fmt, va_list ap) {
    return __sr_vfscanf(f, fmt, ap);
}
int __isoc23_vsscanf(const char* s, const char* fmt, va_list ap) {
    return __sr_vsscanf(s, fmt, ap);
}

int __isoc23_wscanf(const wchar_t* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int r = __sr_vwscanf(fmt, ap);
    va_end(ap);
    return r;
}
int __isoc23_fwscanf(FILE* f, const wchar_t* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int r = __sr_vfwscanf(f, fmt, ap);
    va_end(ap);
    return r;
}
int __isoc23_swscanf(const wchar_t* s, const wchar_t* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int r = __sr_vswscanf(s, fmt, ap);
    va_end(ap);
    return r;
}
int __isoc23_vwscanf(const wchar_t* fmt, va_list ap) {
    return __sr_vwscanf(fmt, ap);
}
int __isoc23_vfwscanf(FILE* f, const wchar_t* fmt, va_list ap) {
    return __sr_vfwscanf(f, fmt, ap);
}
int __isoc23_vswscanf(const wchar_t* s, const wchar_t* fmt, va_list ap) {
    return __sr_vswscanf(s, fmt, ap);
}

// ---------------------------------------------------------------------------
// strlcpy/strlcat/wcslcpy/wcslcat: genuinely new in glibc 2.38, plain BSD
// semantics. The __*_chk forms are what _FORTIFY_SOURCE rewrites calls into.
// ---------------------------------------------------------------------------
static size_t sr_strnlen(const char* s, size_t maxlen) {
    size_t i = 0;
    while (i < maxlen && s[i] != '\0') ++i;
    return i;
}

static size_t sr_wcsnlen(const wchar_t* s, size_t maxlen) {
    size_t i = 0;
    while (i < maxlen && s[i] != L'\0') ++i;
    return i;
}

size_t strlcpy(char* dst, const char* src, size_t size) {
    size_t srclen = strlen(src);
    if (size != 0) {
        size_t n = srclen < size - 1 ? srclen : size - 1;
        memcpy(dst, src, n);
        dst[n] = '\0';
    }
    return srclen;
}

size_t strlcat(char* dst, const char* src, size_t size) {
    size_t dstlen = sr_strnlen(dst, size);
    size_t srclen = strlen(src);
    if (dstlen == size) {
        return size + srclen;
    }
    size_t room = size - dstlen - 1;
    size_t n = srclen < room ? srclen : room;
    memcpy(dst + dstlen, src, n);
    dst[dstlen + n] = '\0';
    return dstlen + srclen;
}

size_t wcslcpy(wchar_t* dst, const wchar_t* src, size_t size) {
    size_t srclen = wcslen(src);
    if (size != 0) {
        size_t n = srclen < size - 1 ? srclen : size - 1;
        wmemcpy(dst, src, n);
        dst[n] = L'\0';
    }
    return srclen;
}

size_t wcslcat(wchar_t* dst, const wchar_t* src, size_t size) {
    size_t dstlen = sr_wcsnlen(dst, size);
    size_t srclen = wcslen(src);
    if (dstlen == size) {
        return size + srclen;
    }
    size_t room = size - dstlen - 1;
    size_t n = srclen < room ? srclen : room;
    wmemcpy(dst + dstlen, src, n);
    dst[dstlen + n] = L'\0';
    return dstlen + srclen;
}

size_t __strlcpy_chk(char* dst, const char* src, size_t size, size_t dstlen) {
    if (size > dstlen) abort();
    return strlcpy(dst, src, size);
}
size_t __strlcat_chk(char* dst, const char* src, size_t size, size_t dstlen) {
    if (size > dstlen) abort();
    return strlcat(dst, src, size);
}
// glibc's <bits/wchar2.h> already passes __glibc_objsize(dst) / sizeof(wchar_t),
// i.e. dstlen counts wchar_t elements, exactly like size - do not scale it again.
size_t __wcslcpy_chk(wchar_t* dst, const wchar_t* src, size_t size, size_t dstlen) {
    if (size > dstlen) abort();
    return wcslcpy(dst, src, size);
}
size_t __wcslcat_chk(wchar_t* dst, const wchar_t* src, size_t size, size_t dstlen) {
    if (size > dstlen) abort();
    return wcslcat(dst, src, size);
}

// ---------------------------------------------------------------------------
// arc4random: new in glibc 2.36. getrandom(2) is the same kernel CSPRNG glibc's
// own implementation seeds itself from.
// ---------------------------------------------------------------------------
void arc4random_buf(void* buf, size_t n) {
    unsigned char* p = (unsigned char*)buf;
    while (n > 0) {
        long r = getrandom(p, n, 0);
        if (r <= 0) {
            if (r < 0 && errno == EINTR) continue;
            abort(); // arc4random_buf() is specified as cannot-fail
        }
        p += (size_t)r;
        n -= (size_t)r;
    }
}

uint32_t arc4random(void) {
    uint32_t v;
    arc4random_buf(&v, sizeof(v));
    return v;
}

uint32_t arc4random_uniform(uint32_t upper_bound) {
    if (upper_bound < 2) {
        return 0;
    }
    // Reject the biased tail, i.e. everything below 2^32 % upper_bound.
    const uint32_t min = (uint32_t)(-upper_bound) % upper_bound;
    uint32_t r;
    do {
        r = arc4random();
    } while (r < min);
    return r % upper_bound;
}

#pragma GCC visibility pop
