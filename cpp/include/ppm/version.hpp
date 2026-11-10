#pragma once

#include <string_view>

namespace ppm {

/// Library version string, e.g. "0.1.0". A placeholder so the build, tests,
/// benchmarks and Python bindings all have something real to exercise.
[[nodiscard]] std::string_view version() noexcept;

}  // namespace ppm
