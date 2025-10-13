
cmake_minimum_required(VERSION 3.22)

project(snarks-test LANGUAGES CXX)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  # these are needed for aarch64 apparently.
  # ALT_BN128 wasn't not scientifically chosen.
  set(CURVE ALT_BN128)
  set(WITH_SUPERCOP OFF)
endif()

add_subdirectory(libsnark)

add_executable(main main.cpp)

