#include <pybind11/pybind11.h>

#include <string>

#include "ppm/version.hpp"

namespace py = pybind11;

PYBIND11_MODULE(ppm_core, m) {
    m.doc() = "ppm C++ core: option pricing, implied vol and volatility surfaces";
    // Copy into std::string: Python needs to own the text it receives.
    m.def("version", [] { return std::string(ppm::version()); }, "Library version string.");
}
