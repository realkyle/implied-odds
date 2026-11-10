add_executable(ppm_tests
    test_version.cpp
)
target_link_libraries(ppm_tests PRIVATE ppm_lib ppm_warnings GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(ppm_tests)  # registers each TEST() with ctest, so `ctest -R` can select one
