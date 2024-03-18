#pragma once

#include "fs/FileManager.hpp"
#include "fs/FindReplace.hpp"
#include "utils/BitMap.hpp"
#include "utils/HashMap.hpp"

namespace dbs {
namespace fs {

struct PageLocation {
    int fileID, pageID;
    PageLocation() : fileID(-1), pageID(-1) {} 
    PageLocation(int fileID_, int pageID_) : fileID(fileID_), pageID(pageID_) {}
    utils::HashItemTwoInt toHashItem() {
        return utils::HashItemTwoInt(fileID, pageID);
    }
};

class BufPageManager {
public:
    /**
     * @brief Construct a new Buf Page Manager object
     * @param fm file manager
     */
    BufPageManager(FileManager* fm);
    /**
     * @brief Destroy the Buf Page Manager object
     */
    ~BufPageManager();
    /**
     * @brief Get the Page object
     * 
     * @param fileID file id 
     * @param pageID page num
     * @param index the page index (used to mark access or markdirty)
     * @return BufType (unsigned int[(PAGE_SIZE >> 2)])
     */
    BufType getPage(int fileID, int pageID, int& index);
    /**
     * @brief must be called after visit the page (identified by the index)
     * 
     * @param index 
     */
    void access(int index);
    /**
     * @brief must be called after modify the page (identified by the index)
     * if called markDirty, no need to call access
     * 
     * @param index 
     */
    void markDirty(int index);
    /**
     * @brief release every opened page, must be called when exisiting the program 
     */
    void close();
private:
    BufType fetchPage(int fileID, int pageID, int& index);
    BufType allocMem();
    void release(int index);
    void writeBack(int index);
    FindReplace* replace;
    utils::BitMap* dirty;
    BufType* addr;
    FileManager* fileManager;
    utils::HashMap<utils::HashItemTwoInt>* hash;
    PageLocation* pageLocation;
    int last_visit_index;
};

}  // namespace fs
}  // namespace dbs