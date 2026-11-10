# implied-odds

Do prediction markets price crypto events the same way the options market does?

A Kalshi or Polymarket contract such as "BTC above $95,000 at 5pm ET" pays $1 or $0, which makes it a
digital option. This project fits an arbitrage-free volatility surface to Deribit BTC options, computes
the options-implied price of each such contract, and compares the two.

**Status:** early scaffolding. Nothing below is built yet beyond the build system.

## Stack

- C++20 core (`cpp/`) exposed to Python as `ppm_core` via pybind11
- Python research layer (`python/ppm/`): venue recorders, contract matching, analysis
- CMake + GoogleTest + Google Benchmark; scikit-build-core + uv; GitHub Actions CI

## Build and test

```bash
# C++
cmake --preset dev && cmake --build --preset dev && ctest --preset dev

# Python (builds the C++ extension too)
uv sync && uv run pytest
```

Paper trading only: this project never places real orders.
