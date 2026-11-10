#include <benchmark/benchmark.h>

#include "ppm/version.hpp"

// Placeholder so the benchmark target builds; replaced by IV-solver benchmarks in Phase 1.
static void BM_Version(benchmark::State& state) {
    for (auto _ : state) {
        benchmark::DoNotOptimize(ppm::version());
    }
}
BENCHMARK(BM_Version);
