#include "fs/BufPageManager.hpp"

namespace dbs {
namespace fs {

BufPageManager::BufPageManager(FileManager* fm) {
    fileManager = fm;
    replace = new FindReplace(CACHE_CAPACITY);
    dirty = new utils::BitMap(CACHE_CAPACITY, false);
    addr = new BufType[CACHE_CAPACITY];
    for (int i = 0; i < CACHE_CAPACITY; i++) addr[i] = nullptr;
    hash = new utils::HashMap<utils::HashItemTwoInt>();
    pageLocation = new PageLocation[CACHE_CAPACITY];
    last_visit_index = -1;
}

BufPageManager::~BufPageManager() {
    fileManager = nullptr;
    delete replace;
    delete dirty;
    delete[] addr;
    delete hash;
    delete[] pageLocation;
}

BufType BufPageManager::allocMem() {
    return new unsigned int[(PAGE_SIZE >> 2)];
}

BufType BufPageManager::fetchPage(int fileID, int pageID, int& index) {
    BufType b;
    index = replace->find();
    b = addr[index];
    if (b == nullptr) {
        b = allocMem();
        addr[index] = b;
    } else {
        if (dirty->getBit(index)) {
            fileManager->writePage(pageLocation[index].fileID,
                                   pageLocation[index].pageID, b, 0);
            dirty->setBit(index, false);
        }
        assert(hash->del(pageLocation[index].toHashItem()) == true);
    }
    hash->insert(utils::HashItemTwoInt(fileID, pageID, index));
    pageLocation[index] = PageLocation(fileID, pageID);
    return b;
}

void BufPageManager::access(int index) {
    if (index != last_visit_index) {
        replace->access(index);
        last_visit_index = index;
    }
}

BufType BufPageManager::getPage(int fileID, int pageID, int& index) {
    utils::HashItemTwoInt find_item(fileID, pageID);
    find_item = hash->find(find_item);
    index = find_item.val;
    if (index != -1) {
        access(index);
        return addr[index];
    } else {
        BufType b = fetchPage(fileID, pageID, index);
        fileManager->readPage(fileID, pageID, b, 0);
        return b;
    }
}

void BufPageManager::markDirty(int index) {
    dirty->setBit(index, true);
    access(index);
}

void BufPageManager::writeBack(int index) {
    if (dirty->getBit(index)) {
        fileManager->writePage(pageLocation[index].fileID,
                               pageLocation[index].pageID, addr[index], 0);
        dirty->setBit(index, false);
    }
}

void BufPageManager::release(int index) {
    writeBack(index);
    replace->free(index);
    hash->del(pageLocation[index].toHashItem());
}

void BufPageManager::close() {
    for (int i = 0; i < CACHE_CAPACITY; ++i) {
        if (addr[i] != nullptr) {
            release(i);
            delete[] addr[i];
            addr[i] = nullptr;
        }
    }
}

}  // namespace fs
}  // namespace dbs