#pragma once

#include <filesystem>

namespace test_data
{
inline std::filesystem::path directory()
{
    return std::filesystem::path(__FILE__).parent_path().parent_path() / "data";
}
} // namespace test_data
