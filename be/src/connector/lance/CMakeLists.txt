# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0.

# Build this optional dependency with thirdparty/build-lance.sh before configuring BE.
# BE consumes the installed library and matching header without invoking Rust tools.
find_package(StarRocksLance 0.1.0 EXACT REQUIRED CONFIG
             PATHS "${THIRDPARTY_DIR}/lance" NO_DEFAULT_PATH)

# build.sh packages this into be/lib even when Java extensions are disabled.
install(FILES "${StarRocksLance_LIBRARY}" DESTINATION "${OUTPUT_DIR}/lib")
