"""Smoke tests: the package imports and the C++ extension is built and callable."""

import ppm
import ppm_core


def test_cpp_and_python_versions_agree():
    # Proves the pybind11 module was built, installed and links the C++ library.
    assert ppm_core.version() == ppm.__version__
