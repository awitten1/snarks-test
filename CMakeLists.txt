
cmake_minimum_required(VERSION 3.22)
cmake_policy(SET CMP0077 NEW)

project(snarks-test LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  # BN128 is only supported on x86
  set(CURVE "ALT_BN128" CACHE STRING "Default curve" FORCE)
  set(WITH_SUPERCOP OFF CACHE BOOL "Disable WITH_SUPERCOP" FORCE)
endif()

add_subdirectory(libsnark)
include_directories(src/include)

find_package(GTest REQUIRED)
add_executable(main main.cpp)

