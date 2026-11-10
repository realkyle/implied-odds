add_executable(ppm_bench
    bench_version.cpp
)
target_link_libraries(ppm_bench PRIVATE ppm_lib benchmark::benchmark_main)
