#pragma once
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace dbs {
namespace utils {

void int2str(int num, char** result_str);

void concatPath(const char* path1, const char* path2, char** result_path);

void concatStr(const char* str1, const char* str2, char** result_str);

}  // namespace utils
}  // namespace dbs