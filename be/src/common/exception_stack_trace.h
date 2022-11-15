// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

// refer to https://stackoverflow.com/a/45057441/3315536
#pragma once
// Our stack unwinding is a GNU C extension:
#if defined(__GNUC__)
// include elfutils to parse debugger information:
#include <elfutils/libdwfl.h>

// include libunwind to gather the stack trace:
#define UNW_LOCAL_ONLY

#include <cxxabi.h>
#include <dlfcn.h>
#include <libunwind.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <typeinfo>

#include "runtime/current_thread.h"

#define LIBUNWIND_MAX_PROCNAME_LENGTH 4096

static bool mExceptionStackTrace = true;

// We would like to print a stacktrace for every throw (even in
// sub-libraries and independent of the object thrown). This works
// only for gcc and only with a bit of trickery
extern "C" {
void print_exception_info(const std::type_info* aExceptionInfo) {
    int vDemangleStatus;
    char* vDemangledExceptionName;

    if (aExceptionInfo != NULL) {
        // Demangle the name of the exception using the GNU C++ ABI:
        vDemangledExceptionName = abi::__cxa_demangle(aExceptionInfo->name(), NULL, NULL, &vDemangleStatus);
        if (vDemangledExceptionName != NULL) {
            fprintf(stderr, "\n");
            fprintf(stderr, "Caught exception %s:\n", vDemangledExceptionName);

            // Free the memory from __cxa_demangle():
            free(vDemangledExceptionName);
        } else {
            // NOTE: if the demangle fails, we do nothing, so the
            // non-demangled name will be printed. Thats ok.
            fprintf(stderr, "\n");
            fprintf(stderr, "Caught exception %s:\n", aExceptionInfo->name());
        }
    } else {
        fprintf(stderr, "\n");
        fprintf(stderr, "Caught exception:\n");
    }
}

// TODO(fzh): print msg into a string
void libunwind_print_backtrace(const int aFramesToIgnore = 0) {
    unw_cursor_t vUnwindCursor;
    unw_context_t vUnwindContext;
    unw_word_t ip, sp, off;
    unw_proc_info_t pip;
    int vUnwindStatus, vDemangleStatus, i, n = 0;
    char vProcedureName[LIBUNWIND_MAX_PROCNAME_LENGTH];
    char* vDemangledProcedureName;
    [[maybe_unused]] const char* vDynObjectFileName;
    const char* vSourceFileName;
    int vSourceFileLineNumber;

    // This is from libDL used for identification of the object file names:
    Dl_info dlinfo;

    // This is from DWARF for accessing the debugger information:
    Dwarf_Addr addr;
    char* debuginfo_path = NULL;
    Dwfl_Callbacks callbacks = {};
    Dwfl_Line* vDWARFObjLine;
    // initialize the DWARF handling:
    callbacks.find_elf = dwfl_linux_proc_find_elf;
    callbacks.find_debuginfo = dwfl_standard_find_debuginfo;
    callbacks.debuginfo_path = &debuginfo_path;
    Dwfl* dwfl = dwfl_begin(&callbacks);
    if (dwfl == NULL) {
        fprintf(stderr, "libunwind_print_backtrace(): Error initializing DWARF.\n");
    }
    if ((dwfl != NULL) && (dwfl_linux_proc_report(dwfl, getpid()) != 0)) {
        fprintf(stderr, "libunwind_print_backtrace(): Error initializing DWARF.\n");
        dwfl = NULL;
    }
    if ((dwfl != NULL) && (dwfl_report_end(dwfl, NULL, NULL) != 0)) {
        fprintf(stderr, "libunwind_print_backtrace(): Error initializing DWARF.\n");
        dwfl = NULL;
    }

    // Begin stack unwinding with libunwnd:
    vUnwindStatus = unw_getcontext(&vUnwindContext);
    if (vUnwindStatus) {
        fprintf(stderr, "libunwind_print_backtrace(): Error in unw_getcontext: %d\n", vUnwindStatus);
        return;
    }

    vUnwindStatus = unw_init_local(&vUnwindCursor, &vUnwindContext);
    if (vUnwindStatus) {
        fprintf(stderr, "libunwind_print_backtrace(): Error in unw_init_local: %d\n", vUnwindStatus);
        return;
    }

    vUnwindStatus = unw_step(&vUnwindCursor);
    for (i = 0; ((i < aFramesToIgnore) && (vUnwindStatus > 0)); ++i) {
        // We ignore the first aFramesToIgnore stack frames:
        vUnwindStatus = unw_step(&vUnwindCursor);
    }

    while (vUnwindStatus > 0) {
        pip.unwind_info = NULL;
        vUnwindStatus = unw_get_proc_info(&vUnwindCursor, &pip);
        if (vUnwindStatus) {
            fprintf(stderr, "libunwind_print_backtrace(): Error in unw_get_proc_info: %d\n", vUnwindStatus);
            break;
        }

        // Resolve the address of the stack frame using libunwind:
        unw_get_reg(&vUnwindCursor, UNW_REG_IP, &ip);
        unw_get_reg(&vUnwindCursor, UNW_REG_SP, &sp);

        // Resolve the name of the procedure using libunwind:
        // unw_get_proc_name() returns 0 on success, and returns UNW_ENOMEM
        // if the procedure name is too long to fit in the buffer provided and
        // a truncated version of the name has been returned:
        vUnwindStatus = unw_get_proc_name(&vUnwindCursor, vProcedureName, LIBUNWIND_MAX_PROCNAME_LENGTH, &off);
        if (vUnwindStatus == 0) {
            // Demangle the name of the procedure using the GNU C++ ABI:
            vDemangledProcedureName = abi::__cxa_demangle(vProcedureName, NULL, NULL, &vDemangleStatus);
            if (vDemangledProcedureName != NULL) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-truncation"
                strncpy(vProcedureName, vDemangledProcedureName, LIBUNWIND_MAX_PROCNAME_LENGTH);
#pragma GCC diagnostic pop
                // Free the memory from __cxa_demangle():
                free(vDemangledProcedureName);
            } else {
                // NOTE: if the demangle fails, we do nothing, so the
                // non-demangled name will be printed. Thats ok.
            }
        } else if (vUnwindStatus == UNW_ENOMEM) {
            // NOTE: libunwind could resolve the name, but could not store
            // it in a buffer of only LIBUNWIND_MAX_PROCNAME_LENGTH characters.
            // So we have a truncated procedure name that can not be demangled.
            // We ignore the problem and the truncated non-demangled name will
            // be printed.
        } else {
            vProcedureName[0] = '?';
            vProcedureName[1] = '?';
            vProcedureName[2] = '?';
            vProcedureName[3] = 0;
        }

        // Resolve the object file name using dladdr:
        if (dladdr((void*)(pip.start_ip + off), &dlinfo) && dlinfo.dli_fname && *dlinfo.dli_fname) {
            vDynObjectFileName = dlinfo.dli_fname;
        } else {
            vDynObjectFileName = "???";
        }

        // Resolve the source file name using DWARF:
        if (dwfl != NULL) {
            addr = (uintptr_t)(ip - 4);
            [[maybe_unused]] Dwfl_Module* module = dwfl_addrmodule(dwfl, addr);
            // Here we could also ask for the procedure name:
            //const char* vProcedureName = dwfl_module_addrname(module, addr);
            // Here we could also ask for the object file name:
            //vDynObjectFileName = dwfl_module_info(module, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
            vDWARFObjLine = dwfl_getsrc(dwfl, addr);
            if (vDWARFObjLine != NULL) {
                vSourceFileName = dwfl_lineinfo(vDWARFObjLine, &addr, &vSourceFileLineNumber, NULL, NULL, NULL);
                //fprintf(stderr, " %s:%d", strrchr(vSourceFileName, '/')+1, vSourceFileLineNumber);
            }
        }
        if (dwfl == NULL || vDWARFObjLine == NULL || vSourceFileName == NULL) {
            vSourceFileName = "???";
            vSourceFileLineNumber = 0;
        }

        // Print the stack frame number:
        fprintf(stderr, "#%2d:", ++n);

        // Print the stack addresses:
        fprintf(stderr, " 0x%016" PRIxPTR " sp=0x%016" PRIxPTR, static_cast<uintptr_t>(ip), static_cast<uintptr_t>(sp));

        // Print the source file name:
        fprintf(stderr, " %s:%d", vSourceFileName, vSourceFileLineNumber);

        // Print the dynamic object file name (that is the library name).
        // This is typically not interesting if we have the source file name.
        //fprintf(stderr, " %s", vDynObjectFileName);

        // Print the procedure name:
        fprintf(stderr, " %s", vProcedureName);

        // Print the procedure offset:
        //fprintf(stderr, " + 0x%" PRIxPTR, static_cast<uintptr_t>(off));

        // Print a newline to terminate the output:
        fprintf(stderr, "\n");

        // Stop the stack trace at the main method (there are some
        // uninteresting higher level functions on the stack):
        if (strcmp(vProcedureName, "main") == 0) {
            break;
        }

        vUnwindStatus = unw_step(&vUnwindCursor);
        if (vUnwindStatus < 0) {
            fprintf(stderr, "libunwind_print_backtrace(): Error in unw_step: %d\n", vUnwindStatus);
        }
    }
}

void __wrap___cxa_throw(void* thrown_exception, void* infov, void (*dest)(void*)) {
    std::type_info* info = (std::type_info*)infov;
    auto query_id = starrocks::CurrentThread::current().query_id();
    auto fragment_instance_id = starrocks::CurrentThread::current().fragment_instance_id();
    fprintf(stderr, "query_id=%s, fragment_instance_id=%s throws exceptions:", starrocks::print_id(query_id).c_str(),
            starrocks::print_id(fragment_instance_id).c_str());
    // print the stack trace to stderr:
    if (mExceptionStackTrace) {
        print_exception_info(info);
        libunwind_print_backtrace(1);
    }

    // call the real __cxa_throw():
    static void (*const rethrow)(void*, void*, void (*)(void*)) __attribute__((noreturn)) =
            (void (*)(void*, void*, void (*)(void*)))dlsym(RTLD_NEXT, "__cxa_throw");
    rethrow(thrown_exception, info, dest);
}
}
#endif
