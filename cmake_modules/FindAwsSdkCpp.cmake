##############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
##############################################################################

# - Find aws-sdk-cpp (S3 client) built as static libraries.
# AWS_SDK_CPP_ROOT is the install prefix of aws-sdk-cpp (contains include/ and lib/).
#
# This module defines
#  AWS_SDK_CPP_INCLUDE_DIR, directory containing the aws/ headers
#  AWS_SDK_CPP_STATIC_LIBS, the static libraries of the SDK and its aws-c-* / s2n deps
#  AWS_SDK_CPP_FOUND, whether the SDK has been found
#  aws_sdk_cpp, an interface library that links all of the above as one group

find_path(AWS_SDK_CPP_INCLUDE_DIR NAMES aws/s3/S3Client.h
  PATHS ${AWS_SDK_CPP_ROOT}/include
  NO_DEFAULT_PATH)

find_library(AWS_SDK_CPP_S3_LIB NAMES libaws-cpp-sdk-s3.a
  PATHS ${AWS_SDK_CPP_ROOT}/lib ${AWS_SDK_CPP_ROOT}/lib64
  NO_DEFAULT_PATH)

if (NOT AWS_SDK_CPP_INCLUDE_DIR OR NOT AWS_SDK_CPP_S3_LIB)
  message(FATAL_ERROR "aws-sdk-cpp includes and libraries NOT found. "
    "Looked in ${AWS_SDK_CPP_ROOT}/include and ${AWS_SDK_CPP_ROOT}/lib")
  set(AWS_SDK_CPP_FOUND FALSE)
else()
  # The SDK installs about 15 static libraries (aws-cpp-sdk-*, aws-crt-cpp, aws-c-*,
  # aws-checksums, s2n). Static libraries are linked in dependency order: each library
  # comes before the libraries it uses. (A --start-group/--end-group wrapper is not used:
  # CMake reorders flags of interface libraries on the final link line.)
  get_filename_component(_AWS_SDK_CPP_LIB_DIR ${AWS_SDK_CPP_S3_LIB} DIRECTORY)
  set(AWS_SDK_CPP_STATIC_LIBS)
  foreach(_lib aws-cpp-sdk-s3 aws-cpp-sdk-core aws-crt-cpp aws-c-s3 aws-c-auth
      aws-c-mqtt aws-c-event-stream aws-c-http aws-c-compression aws-c-sdkutils
      aws-c-io s2n aws-c-cal aws-checksums aws-c-common)
    if (NOT EXISTS ${_AWS_SDK_CPP_LIB_DIR}/lib${_lib}.a)
      message(FATAL_ERROR "aws-sdk-cpp library lib${_lib}.a NOT found in "
        "${_AWS_SDK_CPP_LIB_DIR}")
    endif()
    list(APPEND AWS_SDK_CPP_STATIC_LIBS ${_AWS_SDK_CPP_LIB_DIR}/lib${_lib}.a)
  endforeach()
  set(AWS_SDK_CPP_FOUND TRUE)
  message(STATUS "aws-sdk-cpp include dir: ${AWS_SDK_CPP_INCLUDE_DIR}")
  message(STATUS "aws-sdk-cpp static libraries: ${AWS_SDK_CPP_STATIC_LIBS}")

  # The SDK libraries depend on curl, OpenSSL and zlib, which impalad already links.
  # curl is also used by other libraries, so CMake keeps only its first occurrence on the
  # link line, before the SDK, and SDK-only curl symbols (e.g. curl_version_info) stay
  # unresolved. The static libcurl is therefore repeated here by path, right after the
  # SDK libraries. CURL_STATIC_LIB comes from FindCurl, which runs before this module.
  add_library(aws_sdk_cpp INTERFACE)
  target_link_libraries(aws_sdk_cpp INTERFACE ${AWS_SDK_CPP_STATIC_LIBS} ${CURL_STATIC_LIB})
endif()

mark_as_advanced(
  AWS_SDK_CPP_INCLUDE_DIR
  AWS_SDK_CPP_S3_LIB
  AWS_SDK_CPP_STATIC_LIBS
  AWS_SDK_CPP_FOUND
)
