#include <gtest/gtest.h>

#include "ppm/version.hpp"

TEST(Version, MatchesProjectVersion) { EXPECT_EQ(ppm::version(), "0.1.0"); }
