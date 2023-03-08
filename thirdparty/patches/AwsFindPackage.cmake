# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

option(IN_SOURCE_BUILD "If the CRT libs are being built from your source tree (add_subdirectory), set this to ON" OFF)

# This function handles dependency list building based on if traditional CMAKE modules via. find_package should be
# used, vs if this is an in source build via. something like git submodules and add_subdirectory.
# This is largely because CMake was not well planned out, and as a result, in-source and modules don't play well
# together. Only use this on CRT libraries (including S2N), libcrypto will stay as an assumed external dependency.
#
# package_name: is the name of the package to find
# DEP_AWS_LIBS: output variable will be appended after each call to this function. You don't have to use it,
#    but it can be passed directly target_link_libraries and it will be the properly qualified library
#    name and namespace based on configuration.
function(aws_use_package package_name)
    if (IN_SOURCE_BUILD)
        set(DEP_AWS_LIBS ${DEP_AWS_LIBS} ${package_name} PARENT_SCOPE)
    else()
        find_package(${package_name} REQUIRED)
        set(DEP_AWS_LIBS ${DEP_AWS_LIBS} AWS::${package_name} PARENT_SCOPE)
    endif()
endfunction()
