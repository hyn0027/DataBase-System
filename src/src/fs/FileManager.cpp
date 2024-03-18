#include "fs/FileManager.hpp"

namespace dbs {
namespace fs {

bool FileManager::writePage(int fileID, int pageID, BufType buf, int off) {
    int f = fd[fileID];
    off_t offset = pageID;
    offset = (offset << PAGE_SIZE_IDX);
    off_t error = lseek(f, offset, SEEK_SET);
    if (error != offset) return false;
    BufType b = buf + off;
    error = write(f, (void*)b, PAGE_SIZE);
    return true;
}

bool FileManager::readPage(int fileID, int pageID, BufType buf, int off) {
    int f = fd[fileID];
    off_t offset = pageID;
    offset = (offset << PAGE_SIZE_IDX);
    off_t error = lseek(f, offset, SEEK_SET);
    if (error != offset) return false;
    BufType b = buf + off;
    error = read(f, (void*)b, PAGE_SIZE);
    return true;
}

void FileManager::closeFile(int fileID) {
    close(fd[fileID]);
    fd.erase(fileID);
}

bool FileManager::createFile(const char* name) {
    FILE* f = fopen(name, "a+");
    if (f == NULL) {
        std::cerr << "Failed creating file " << name << std::endl;
        return false;
    }
    fclose(f);
    return true;
}

int FileManager::openFile(const char* name) {
    int fileID = file_id++;
    int f = open(name, O_RDWR);
    if (f == -1) {
        std::cerr << "Failed opening file " << name << std::endl;
        return -1;
    }
    fd[fileID] = f;
    return fileID;
}

bool FileManager::existFile(const char* name) {
    int f = open(name, O_RDWR);
    if (f == -1) return false;
    close(f);
    return true;
}

bool FileManager::deleteFile(const char* name) {
    if (remove(name) != 0) {
        std::cerr << "Failed deleting file " << name << std::endl;
        return false;
    }
    return true;
}

bool FileManager::existFolder(const char* path) {
    struct stat info;
    return stat(path, &info) == 0 && S_ISDIR(info.st_mode);
}

bool FileManager::createFolder(const char* path) {
    if (mkdir(path, 0777) == 0) {
        return true;
    }
    std::cerr << "Failed creating folder " << path << std::endl;
    return false;
}

bool FileManager::deleteFolder(const char* path) {
    DIR* dir = opendir(path);
    struct dirent* entry;

    if (dir == NULL) {
        std::cerr << "Failed deleting folder " << path << std::endl;
        return false;
    }

    while ((entry = readdir(dir)) != NULL) {
        std::string entryName = entry->d_name;
        if (entryName != "." && entryName != "..") {
            std::string fullPath = std::string(path) + "/" + entryName;
            struct stat statBuf;
            if (lstat(fullPath.c_str(), &statBuf) == -1) {
                closedir(dir);
                std::cerr << "Failed deleting folder " << path << std::endl;
                return false;
            }

            if (S_ISDIR(statBuf.st_mode)) {
                // Recursive call for directories
                if (!deleteFolder(fullPath.c_str())) {
                    closedir(dir);
                    std::cerr << "Failed deleting folder " << path << std::endl;
                    return false;
                }
            } else {
                // Delete files
                if (unlink(fullPath.c_str()) == -1) {
                    // Error deleting file
                    closedir(dir);
                    std::cerr << "Failed deleting folder " << path << std::endl;
                    return false;
                }
            }
        }
    }
    closedir(dir);

    // Delete the directory itself
    if (rmdir(path) == -1) {
        std::cerr << "Failed deleting folder " << path << std::endl;
        return false;
    }

    return true;
}

}  // namespace fs
}  // namespace dbs