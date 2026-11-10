cmake_minimum_required(VERSION 3.24)
project(ppm VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)  # lets clang-tidy and editors see real compile flags

option(PPM_BUILD_TESTS  "Build GoogleTest unit tests"       ON)
option(PPM_BUILD_BENCH  "Build Google Benchmark benchmarks" ON)
option(PPM_BUILD_PYTHON "Build the ppm_core pybind11 module" OFF)

# Shared warning flags, attached to our own targets only (not third-party code).
add_library(ppm_warnings INTERFACE)
target_compile_options(ppm_warnings INTERFACE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)

# The pure C++ library. Tests, benchmarks and the Python module all link this,
# so the numerical code is compiled once and tested exactly as Python uses it.
add_library(ppm_lib STATIC
    cpp/src/version.cpp
)
target_include_directories(ppm_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/cpp/include)
target_link_libraries(ppm_lib PRIVATE ppm_warnings)
set_target_properties(ppm_lib PROPERTIES POSITION_INDEPENDENT_CODE ON)  # needed to link into a shared Python module

include(FetchContent)

if(PPM_BUILD_TESTS)
    enable_testing()
    FetchContent_Declare(googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    add_subdirectory(cpp/tests)
endif()

if(PPM_BUILD_BENCH)
    FetchContent_Declare(benchmark
        URL https://github.com/google/benchmark/archive/refs/tags/v1.9.1.tar.gz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(benchmark)
    add_subdirectory(cpp/bench)
endif()

if(PPM_BUILD_PYTHON)
    # pybind11 comes from the Python build environment (pyproject build-system.requires).
    find_package(Python 3.11 REQUIRED COMPONENTS Interpreter Development.Module)
    find_package(pybind11 CONFIG REQUIRED)
    pybind11_add_module(ppm_core cpp/bindings/module.cpp)
    target_link_libraries(ppm_core PRIVATE ppm_lib ppm_warnings)
    install(TARGETS ppm_core DESTINATION .)  # top-level module: `import ppm_core`
endif()
