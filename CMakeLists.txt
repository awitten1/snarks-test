
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

find_package(GTest REQUIRED)
find_package(TBB REQUIRED)

include_directories(src/include)
include_directories(third_party/parallel-hashmap)
link_libraries(TBB::tbb)

option(ENABLE_TSAN "Enable TSAN" OFF)

if (ENABLE_TSAN)
  add_compile_options("-fsanitize=thread")
  add_link_options("-fsanitize=thread")
endif()


if (ENABLE_ASAN)
  add_compile_options("-fsanitize=address")
  add_link_options("-fsanitize=address")
endif()

option(BUILD_TESTS "Build unit tests" ON)

if (BUILD_TESTS)
  add_executable(tests src/tests/tests.cpp)
  target_link_libraries(tests PUBLIC GTest::gtest_main)
endif()

add_executable(example1 src/examples/example1.cpp)

