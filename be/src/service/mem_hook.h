// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#ifndef BE_TEST
inline __thread bool g_tls_enable_mem_tracker = true;
#define MEM_TRACKER_ENABLED() g_tls_enable_mem_tracker
#define ENABLE_MEMTRACKER()  g_tls_enable_mem_tracker = true
#define DISABLE_MEMTRACKER() g_tls_enable_mem_tracker = false
#else
#define MEM_TRACKER_ENABLED() false
#define ENABLE_MEMTRACKER()
#define DISABLE_MEMTRACKER()
#endif
