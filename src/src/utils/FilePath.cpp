#include "utils/FilePath.hpp"

namespace dbs {
namespace utils {

void int2str(int num, char** result_str) {
    int len = snprintf(nullptr, 0, "%d", num);
    *result_str = new char[len + 1];
    sprintf(*result_str, "%d", num);
}

void concatPath(const char* path1, const char* path2, char** result_path) {
    int len1 = strlen(path1);
    int len2 = strlen(path2);
    *result_path = new char[len1 + len2 + 2];
    strcpy(*result_path, path1);
    strcat(*result_path, "/");
    strcat(*result_path, path2);
}

void concatStr(const char* str1, const char* str2, char** result_str) {
    int len1 = strlen(str1);
    int len2 = strlen(str2);
    *result_str = new char[len1 + len2 + 1];
    strcpy(*result_str, str1);
    strcat(*result_str, str2);
}

}  // namespace utils
}  // namespace dbs