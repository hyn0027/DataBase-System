#pragma once

#include <iostream>
#include <cstdio>
#include <map>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

#include "common/Config.hpp"

namespace dbs {
namespace fs{

class FileManager {
public:
    /**
     * @brief write to page, 将buf+off开始的2048个四字节整数(8kb信息)
     * 写入fileID和pageID指定的文件页中
     * @param fileID: file id
     * @param pageID: page id
     * @param buf: buffer, 存储信息的缓存(4字节无符号整数数组)
     * @param off: offset
     * @return true if success, false otherwise
     */
    bool writePage(int fileID, int pageID, BufType buf, int off);

    /**
     * @brief read from page, 将fileID和pageID指定的文件页中
     * 2048个四字节整数(8kb)读入到buf+off开始的内存中
     * @param fileID: file id
     * @param pageID: page id
     * @param buf: buffer, 存储信息的缓存(4字节无符号整数数组)
     * @param off: offset
     * @return true if success, false otherwise
     */
    bool readPage(int fileID, int pageID, BufType buf, int off);

    /**
     * @brief close file, 关闭文件，关闭前务必保证调用过bpm->close()
     */
    void closeFile(int fileID);

    /**
     * @brief create file, 新建name指定的文件名
     * @param name: file name
     * @return true if success, false otherwise
     */
    bool createFile(const char* name);

    /**
     * @brief open file, 打开name指定的文件名
     * @param name: file name
     * @return file id if success, -1 otherwise
     */
    int openFile(const char* name);

    /**
     * @brief assert if a file exist
     * 
     * @param name 
     * @return true exist, false not exist
     */
    bool existFile(const char* name);

    /**
     * @brief delete file, 删除name指定的文件名
     * @param name: file name
     * @return true if success, false otherwise
     */
    bool deleteFile(const char* name);

    /**
     * @brief exist folder
     * @param path: folder path
     * @return true if exist, false otherwise
     */
    bool existFolder(const char* path);

    /**
     * @brief create folder
     * @param path: folder path
     * @return true if success, false otherwise
     */
    bool createFolder(const char* path);

    /**
     * @brief delete folder
     * @param path: folder path
     * @return true if success, false otherwise
     */
    bool deleteFolder(const char* path);
private:
    std::map<int, int> fd;
    uint file_id = 0;
};

}  // namespace fs
}  // namespace dbs