#include "index/IndexManager.hpp"

namespace dbs {
namespace index {

IndexManager::IndexManager(fs::FileManager* fm_, fs::BufPageManager* bpm_) {
    fm = fm_;
    bpm = bpm_;
    current_opening_file_paths.clear();
    current_opening_file_ids.clear();
}

IndexManager::~IndexManager() {
    closeAllCurrentFile();
    fm = nullptr;
    bpm = nullptr;
}

void IndexManager::closeAllCurrentFile() {
    bpm->close();
    for (auto& file_path : current_opening_file_paths) {
        delete[] file_path;
        file_path = nullptr;
    }
    current_opening_file_paths.clear();
    for (auto& file_id : current_opening_file_ids) {
        fm->closeFile(file_id);
    }
    current_opening_file_ids.clear();
}

void IndexManager::closeFileIfExist(const char* file_path) {
    int current_opening_file_num = current_opening_file_paths.size();
    for (int i = 0; i < current_opening_file_num; i++) {
        if (strcmp(current_opening_file_paths[i], file_path) == 0) {
            int file_id = current_opening_file_ids[i];
            bpm->close();
            fm->closeFile(file_id);
            delete[] current_opening_file_paths[i];
            current_opening_file_paths[i] = nullptr;
            current_opening_file_paths.erase(
                current_opening_file_paths.begin() + i);
            current_opening_file_ids.erase(current_opening_file_ids.begin() +
                                           i);
            return;
        }
    }
}

void IndexManager::closeFirstFile() {
    if (current_opening_file_ids.size() == 0) return;
    int file_id = current_opening_file_ids.front();
    current_opening_file_ids.erase(current_opening_file_ids.begin());
    bpm->close();
    fm->closeFile(file_id);
    delete[] current_opening_file_paths.front();
    current_opening_file_paths.front() = nullptr;
    current_opening_file_paths.erase(current_opening_file_paths.begin());
}

int IndexManager::openFile(const char* file_path) {
    int current_opening_file_num = current_opening_file_paths.size();
    for (int i = 0; i < current_opening_file_num; i++) {
        if (strcmp(current_opening_file_paths[i], file_path) == 0) {
            return current_opening_file_ids[i];
        }
    }
    if (current_opening_file_num == opening_cache_capacity) {
        closeFirstFile();
    }
    current_opening_file_paths.push_back(new char[strlen(file_path) + 1]);
    strcpy(current_opening_file_paths.back(), file_path);
    int file_id = fm->openFile(file_path);
    current_opening_file_ids.push_back(file_id);
    return file_id;
}

void IndexManager::createEmptyBitMapPage(int file_id, int page_id) {
    BufType b;
    int index;

    b = bpm->getPage(file_id, page_id, index);
    memset(b, 0, PAGE_SIZE);
    b[BUF_PER_PAGE - 1] = -1;  // 目前没有下一页
    bpm->markDirty(index);
}

bool IndexManager::setBitMapPage(int file_id, int base_page_id, int insert_pos,
                                 bool insert_val) {
    if (base_page_id == -1) {
        return false;
    }
    BufType b;
    int index;
    b = bpm->getPage(file_id, base_page_id, index);
    if (insert_pos < INDEX_BITMAP_PAGE_BYTE_LEN * BIT_PER_BYTE) {
        // 在本页
        utils::setBitFromBuf(b, insert_pos, insert_val);
        bpm->markDirty(index);
        return true;
    }
    // 递归向后查找
    bpm->access(index);
    return setBitMapPage(file_id, b[BUF_PER_PAGE - 1],
                         insert_pos - INDEX_BITMAP_PAGE_BYTE_LEN * BIT_PER_BYTE,
                         insert_val);
}

int IndexManager::getFirstEmptyPageId(int file_id, bool set) {
    BufType b;
    int index;
    int offset = 0;
    int base_page_id = 1;
    while (base_page_id != -1) {
        b = bpm->getPage(file_id, base_page_id, index);
        bpm->access(index);
        for (int i = 0; i < BUF_PER_PAGE - 1; i++) {
            if (b[i] != 0xffffffff) {
                int j = utils::getFirstZeroBit(b[i]);
                if (set) {
                    utils::setBitFromNum(b[i], j, true);
                    bpm->markDirty(index);
                }
                return (i << LOG_BIT_PER_BUF) + j + offset;
            }
        }
        offset += (INDEX_BITMAP_PAGE_BYTE_LEN << LOG_BIT_PER_BYTE);
        base_page_id = b[BUF_PER_PAGE - 1];
    }
    b[BUF_PER_PAGE - 1] = offset;
    bpm->markDirty(index);
    createEmptyBitMapPage(file_id, offset);
    setBitMapPage(file_id, offset, 0, true);
    if (set) setBitMapPage(file_id, offset, 1, true);  // 第一个bitmap页
    return offset + 1;
}

int IndexManager::getBPlusTreeM(int index_key_num) {
    return (PAGE_SIZE - INDEX_HEADER_BYTE_LEN) /
               getBPlusTreeItemLength(index_key_num) -
           1;
}

int IndexManager::getBPlusTreeItemLength(int index_key_num) {
    return (index_key_num + 2) << LOG_BYTE_PER_BUF;
}

void IndexManager::writeBPlusTreeInternalNode2Page(
    BufType b, const BPlusTreeInternalNode& node, int index_key_num) {
    b[0] = node.prev_page_id;     // 上一个同级节点页号
    b[1] = node.next_page_id;     // 下一个同级节点页号
    b[2] = node.children.size();  // 子节点数量
    b[3] = 0;                     // 是否是叶节点
    int buf_offset = INDEX_HEADER_BYTE_LEN >> LOG_BYTE_PER_BUF;
    for (auto& child : node.children) {
        b[buf_offset++] = child.page_id;
        for (int i = 0; i < index_key_num; i++) {
            b[buf_offset++] = child.max_key[i];
        }
    }
}

void IndexManager::readBPlusTreeInternalNodeFromPage(
    BufType b, BPlusTreeInternalNode& node, int index_key_num) {
    node.prev_page_id = b[0];  // 上一个同级节点页号
    node.next_page_id = b[1];  // 下一个同级节点页号
    int children_num = b[2];   // 子节点数量
    assert(b[3] == 0);         // 是否是叶节点
    node.children.clear();
    int buf_offset = INDEX_HEADER_BYTE_LEN >> LOG_BYTE_PER_BUF;
    for (int i = 0; i < children_num; i++) {
        BPlusTreeInternalChild child(b[buf_offset++], index_key_num);
        for (int j = 0; j < index_key_num; j++) {
            child.max_key[j] = b[buf_offset++];
        }
        node.children.push_back(child);
    }
}

void IndexManager::writeBPlusTreeLeafNode2Page(BufType b,
                                               const BPlusTreeLeafNode& node,
                                               int index_key_num) {
    b[0] = node.prev_page_id;     // 上一个同级节点页号
    b[1] = node.next_page_id;     // 下一个同级节点页号
    b[2] = node.children.size();  // 子节点数量
    b[3] = 1;                     // 是否是叶节点
    int buf_offset = INDEX_HEADER_BYTE_LEN >> LOG_BYTE_PER_BUF;
    for (auto& child : node.children) {
        b[buf_offset++] = child.page_id;
        b[buf_offset++] = child.slot_id;
        for (int i = 0; i < index_key_num; i++) {
            b[buf_offset++] = child.key[i];
        }
    }
}

void IndexManager::readBPlusTreeLeafNodeFromPage(BufType b,
                                                 BPlusTreeLeafNode& node,
                                                 int index_key_num) {
    node.prev_page_id = b[0];  // 上一个同级节点页号
    node.next_page_id = b[1];  // 下一个同级节点页号
    int children_num = b[2];   // 子节点数量
    assert(b[3] == 1);         // 是否是叶节点
    node.children.clear();
    int buf_offset = INDEX_HEADER_BYTE_LEN >> LOG_BYTE_PER_BUF;
    for (int i = 0; i < children_num; i++) {
        BPlusTreeLeafChild child(b[buf_offset], b[buf_offset + 1],
                                 index_key_num);
        buf_offset += 2;
        for (int j = 0; j < index_key_num; j++) {
            child.key[j] = b[buf_offset++];
        }
        node.children.push_back(child);
    }
}

void IndexManager::setPrevPageID(int file_id, int page_id, int prev_page_id) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, page_id, index);
    b[0] = prev_page_id;
    bpm->markDirty(index);
}

void IndexManager::setNextPageID(int file_id, int page_id, int next_page_id) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, page_id, index);
    b[1] = next_page_id;
    bpm->markDirty(index);
}

void IndexManager::setLeafChildSlotIDAndPageID(int file_id, int page_id,
                                               int child_id, int item_page_id,
                                               int item_slot_id,
                                               int index_key_num) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, page_id, index);
    int buf_offset = INDEX_HEADER_BYTE_LEN >> LOG_BYTE_PER_BUF;
    buf_offset += child_id * (2 + index_key_num);
    b[buf_offset++] = item_page_id;
    b[buf_offset++] = item_slot_id;
    bpm->markDirty(index);
}

void IndexManager::initializeIndexFile(const char* file_path,
                                       int index_key_num) {
    assert(index_key_num > 0);
    closeFileIfExist(file_path);
    if (fm->existFile(file_path)) {
        assert(fm->deleteFile(file_path));
    }
    assert(fm->createFile(file_path));
    int file_id = openFile(file_path);
    assert(file_id != -1);

    BufType b;
    int index;

    b = bpm->getPage(file_id, 0, index);
    b[0] = index_key_num;  // index key num
    bpm->markDirty(index);

    // 第1页开始是使用页面的bitmap
    createEmptyBitMapPage(file_id, 1);
    setBitMapPage(file_id, 1, 0, true);  // 首页
    setBitMapPage(file_id, 1, 1, true);  // 第一个bitmap页

    // 创建根节点和叶节点
    int root_page_id = getFirstEmptyPageId(file_id, true);
    int leaf_page_id = getFirstEmptyPageId(file_id, true);

    b = bpm->getPage(file_id, 0, index);
    b[1] = root_page_id;
    bpm->markDirty(index);

    // 初始化叶节点
    BPlusTreeLeafNode leaf_node(-1, -1);
    BPlusTreeInternalNode root_node(-1, -1);
    root_node.children.push_back(
        BPlusTreeInternalChild(leaf_page_id, index_key_num));

    b = bpm->getPage(file_id, leaf_page_id, index);
    writeBPlusTreeLeafNode2Page(b, leaf_node, index_key_num);
    bpm->markDirty(index);

    b = bpm->getPage(file_id, root_page_id, index);
    writeBPlusTreeInternalNode2Page(b, root_node, index_key_num);
    bpm->markDirty(index);
}

bool IndexManager::insertIndex(const char* file_path,
                               const IndexValue& index_value) {
    // open file
    int file_id = openFile(file_path);
    assert(file_id != -1);

    // meta info
    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int index_key_num = b[0];
    int root_page_id = b[1];
    bpm->access(index);
    int m = getBPlusTreeM(index_key_num);

    // check validity
    if (index_value.key.size() != (size_t)index_key_num) return false;
    if (root_page_id == -1) return false;

    // insert
    BPlusTreeLeafChild leaf_child(index_value);
    insertNode(file_id, root_page_id, leaf_child, index_key_num, m);

    // 处理根节点上溢
    if (tmp_tree_internal_child[0].page_id != -1) {
        int new_root_page_id = getFirstEmptyPageId(file_id, true);
        BPlusTreeInternalNode new_root(-1, -1);
        new_root.children.push_back(tmp_tree_internal_child[0]);
        new_root.children.push_back(tmp_tree_internal_child[1]);

        b = bpm->getPage(file_id, new_root_page_id, index);
        writeBPlusTreeInternalNode2Page(b, new_root, index_key_num);
        bpm->markDirty(index);

        b = bpm->getPage(file_id, 0, index);
        b[1] = new_root_page_id;
        bpm->markDirty(index);
    }

    // 返回
    return true;
}

bool IndexManager::handleInternalNodeOverflow(
    int file_id, int page_id, BPlusTreeInternalNode& node,
    const std::list<dbs::index::BPlusTreeInternalChild>::iterator& insert_pos,
    int index_key_num, int b_plus_tree_m) {
    // 子节点是否上溢
    if (tmp_tree_internal_child[0].page_id == -1) {
        return false;  // 当前节点不需要更新
    }

    // 因为子节点上溢了，更新当前节点
    *insert_pos = tmp_tree_internal_child[0];
    node.children.insert(std::next(insert_pos), tmp_tree_internal_child[1]);

    // 当前节点是否上溢
    if (node.children.size() <= (size_t)b_plus_tree_m) {
        // 当前节点没有上溢
        tmp_tree_internal_child[0].page_id = -1;
        tmp_tree_internal_child[1].page_id = -1;
    } else {
        // 当前节点上溢

        // 一分为二，插入新节点
        int new_node_page_id = getFirstEmptyPageId(file_id, true);
        BPlusTreeInternalNode new_node;
        splitInternalNode(node, new_node, page_id, new_node_page_id,
                          index_key_num, b_plus_tree_m);

        // 当前节点的下一个同级页面，修改指针
        if (new_node.next_page_id != -1) {
            setPrevPageID(file_id, new_node.next_page_id, new_node_page_id);
        }

        // 新节点写入文件
        BufType b;
        int index;
        b = bpm->getPage(file_id, new_node_page_id, index);
        writeBPlusTreeInternalNode2Page(b, new_node, index_key_num);
        bpm->markDirty(index);
    }
    return true;  // 当前节点需要更新
}

void IndexManager::splitInternalNode(BPlusTreeInternalNode& origin_node,
                                     BPlusTreeInternalNode& new_node,
                                     int origin_node_page_id,
                                     int new_node_page_id, int index_key_num,
                                     int b_plus_tree_m) {
    // 链表连接
    new_node.prev_page_id = origin_node_page_id;
    new_node.next_page_id = origin_node.next_page_id;
    origin_node.next_page_id = new_node_page_id;

    // 切分children
    auto middle =
        std::next(origin_node.children.begin(), (b_plus_tree_m + 1) / 2);
    new_node.children.splice(new_node.children.begin(), origin_node.children,
                             middle, origin_node.children.end());

    // 设置tmp_tree_internal_child
    tmp_tree_internal_child[0] = BPlusTreeInternalChild(
        origin_node_page_id, origin_node.children.back().max_key);
    tmp_tree_internal_child[1] = BPlusTreeInternalChild(
        new_node_page_id, new_node.children.back().max_key);

    return;
}

void IndexManager::handleLeafNodeOverflow(int file_id, int page_id,
                                          BPlusTreeLeafNode& node,
                                          int index_key_num,
                                          int b_plus_tree_m) {
    if (node.children.size() <= (size_t)b_plus_tree_m) {
        // 当前节点没有上溢
        tmp_tree_internal_child[0].page_id = -1;
        tmp_tree_internal_child[1].page_id = -1;
        return;
    }
    // 当前节点上溢

    // 一分为二，插入新节点
    int new_node_page_id = getFirstEmptyPageId(file_id, true);
    BPlusTreeLeafNode new_node;
    splitLeafNode(node, new_node, page_id, new_node_page_id, index_key_num,
                  b_plus_tree_m);

    // 当前页面的下一个同级页面，修改指针
    if (new_node.next_page_id != -1) {
        setPrevPageID(file_id, new_node.next_page_id, new_node_page_id);
    }

    // 新节点写入文件
    BufType b;
    int index;
    b = bpm->getPage(file_id, new_node_page_id, index);
    writeBPlusTreeLeafNode2Page(b, new_node, index_key_num);
    bpm->markDirty(index);
}

void IndexManager::splitLeafNode(BPlusTreeLeafNode& origin_node,
                                 BPlusTreeLeafNode& new_node,
                                 int origin_node_page_id, int new_node_page_id,
                                 int index_key_num, int b_plus_tree_m) {
    // 链表连接
    new_node.prev_page_id = origin_node_page_id;
    new_node.next_page_id = origin_node.next_page_id;
    origin_node.next_page_id = new_node_page_id;

    // 切分children
    auto middle =
        std::next(origin_node.children.begin(), (b_plus_tree_m + 1) / 2);
    new_node.children.splice(new_node.children.begin(), origin_node.children,
                             middle, origin_node.children.end());

    // 设置tmp_tree_internal_child
    tmp_tree_internal_child[0] = BPlusTreeInternalChild(
        origin_node_page_id, origin_node.children.back().key);
    tmp_tree_internal_child[1] =
        BPlusTreeInternalChild(new_node_page_id, new_node.children.back().key);

    return;
}

void IndexManager::insertNode(int file_id, int page_id,
                              const BPlusTreeLeafChild& insert_item,
                              int index_key_num, int b_plus_tree_m) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, page_id, index);
    bool is_leaf = b[3];
    if (is_leaf) {
        // 如果是叶节点

        // 读取节点
        BPlusTreeLeafNode node;
        readBPlusTreeLeafNodeFromPage(b, node, index_key_num);
        bpm->access(index);

        // 插入
        auto child_itr = node.children.begin();
        for (; child_itr != node.children.end(); child_itr++) {
            if (insert_item <= *child_itr) {
                node.children.insert(child_itr, insert_item);
                break;
            }
        }
        if (child_itr == node.children.end()) {
            node.children.push_back(insert_item);
        }

        // 检查overflow
        handleLeafNodeOverflow(file_id, page_id, node, index_key_num,
                               b_plus_tree_m);

        // 写回
        b = bpm->getPage(file_id, page_id, index);
        writeBPlusTreeLeafNode2Page(b, node, index_key_num);
        bpm->markDirty(index);
        return;
    } else {
        // 不是叶节点

        // 读取节点
        BPlusTreeInternalNode node;
        readBPlusTreeInternalNodeFromPage(b, node, index_key_num);
        bpm->access(index);

        // 插入
        for (auto child_itr = node.children.begin();
             child_itr != node.children.end(); child_itr++) {
            if (insert_item <= *child_itr) {
                // 被插入节点小于max，递归插入
                insertNode(file_id, child_itr->page_id, insert_item,
                           index_key_num, b_plus_tree_m);

                // 处理overflow
                if (handleInternalNodeOverflow(file_id, page_id, node,
                                               child_itr, index_key_num,
                                               b_plus_tree_m)) {
                    // 如有必要，写回
                    b = bpm->getPage(file_id, page_id, index);
                    writeBPlusTreeInternalNode2Page(b, node, index_key_num);
                    bpm->markDirty(index);
                }
                return;
            }
        }
        // 被插入节点大于max，更新max
        node.children.back().max_key = insert_item.key;

        // 插入
        insertNode(file_id, node.children.back().page_id, insert_item,
                   index_key_num, b_plus_tree_m);

        // 处理overflow
        handleInternalNodeOverflow(file_id, page_id, node,
                                   std::prev(node.children.end()),
                                   index_key_num, b_plus_tree_m);

        // 写回
        b = bpm->getPage(file_id, page_id, index);
        writeBPlusTreeInternalNode2Page(b, node, index_key_num);
        bpm->markDirty(index);
    }
}

bool IndexManager::searchIndex(const char* file_path,
                               const IndexValue& search_value,
                               std::vector<IndexValue>& search_results) {
    return searchIndexInRanges(file_path, search_value, search_value,
                               search_results);
}

bool IndexManager::searchIndexInRanges(
    const char* file_path, const IndexValue& search_value_low,
    const IndexValue& search_value_high,
    std::vector<IndexValue>& search_results) {
    search_results.clear();

    // open file
    int file_id = openFile(file_path);
    assert(file_id != -1);

    // meta info
    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int index_key_num = b[0];
    int root_page_id = b[1];
    bpm->access(index);

    // check validity
    if (search_value_low.key.size() != (size_t)index_key_num) return false;
    if (search_value_high.key.size() != (size_t)index_key_num) return false;

    BPlusTreeLeafNode leaf_result;
    if (!searchNode(file_id, root_page_id, BPlusTreeLeafChild(search_value_low),
                    index_key_num, leaf_result)) {
        return true;
    }

    searchForward(file_id, leaf_result, index_key_num,
                  BPlusTreeLeafChild(search_value_low),
                  BPlusTreeLeafChild(search_value_high), search_results);

    return true;
}

bool IndexManager::searchNode(int file_id, int page_id,
                              const BPlusTreeLeafChild& search_value,
                              int index_key_num,
                              BPlusTreeLeafNode& leaf_result) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, page_id, index);
    bool is_leaf = b[3];
    if (is_leaf) {
        // 如果是叶节点

        // 读取节点
        BPlusTreeLeafNode node;
        readBPlusTreeLeafNodeFromPage(b, node, index_key_num);
        bpm->access(index);

        if (node.children.size() > 0 && search_value <= node.children.back()) {
            leaf_result = node;
            return true;
        }

        // 没找到
        return false;
    } else {
        // 不是叶节点

        // 读取节点
        BPlusTreeInternalNode node;
        readBPlusTreeInternalNodeFromPage(b, node, index_key_num);
        bpm->access(index);

        // 搜索
        for (auto& child : node.children) {
            if (search_value <= child) {
                return searchNode(file_id, child.page_id, search_value,
                                  index_key_num, leaf_result);
            }
        }

        // 没找到
        return false;
    }
}

void IndexManager::searchForward(int file_id,
                                 const BPlusTreeLeafNode& base_node,
                                 int index_key_num,
                                 const BPlusTreeLeafChild& search_value_low,
                                 const BPlusTreeLeafChild& search_value_high,
                                 std::vector<IndexValue>& search_results) {
    assert(base_node.children.size() > 0);
    // 从base_node的base_slot_itr开始向后搜索，直到search_value_high
    for (auto itr = base_node.children.begin(); itr != base_node.children.end();
         itr++) {
        if (search_value_low <= *itr && *itr <= search_value_high) {
            search_results.push_back(
                IndexValue(itr->page_id, itr->slot_id, itr->key));
        } else if (search_value_high < *itr) {
            return;
        }
    }

    // Get next page
    if (base_node.next_page_id == -1) return;
    BufType b;
    int index;
    b = bpm->getPage(file_id, base_node.next_page_id, index);
    BPlusTreeLeafNode next_node;
    readBPlusTreeLeafNodeFromPage(b, next_node, index_key_num);
    bpm->access(index);

    // 递归
    searchForward(file_id, next_node, index_key_num, search_value_low,
                  search_value_high, search_results);
}

bool IndexManager::deleteIndex(const char* file_path,
                               const IndexValue& index_value,
                               bool exact_match) {
    // open file
    int file_id = openFile(file_path);
    assert(file_id != -1);

    // meta info
    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int index_key_num = b[0];
    int root_page_id = b[1];
    bpm->access(index);
    int m = getBPlusTreeM(index_key_num);

    // check validity
    if (index_value.key.size() != (size_t)index_key_num) return false;

    return deleteNode(file_id, root_page_id, BPlusTreeLeafChild(index_value),
                      exact_match, index_key_num, m);
}

bool IndexManager::deleteNode(int file_id, int page_id,
                              const BPlusTreeLeafChild& delete_value,
                              bool exact_match, int index_key_num,
                              int b_plus_tree_m) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, page_id, index);
    bool is_leaf = b[3];

    if (is_leaf) {
        // 如果是叶节点

        // 读取节点
        BPlusTreeLeafNode node;
        readBPlusTreeLeafNodeFromPage(b, node, index_key_num);
        bpm->access(index);

        bool found = false;
        bool update_max_val = false;
        for (auto itr = node.children.begin(); itr != node.children.end();
             itr++) {
            if (delete_value == *itr) {
                if (exact_match && !delete_value.exact_match(*itr)) {
                    bool found_exact = false;
                    for (auto exact_itr = itr; exact_itr != node.children.end();
                         exact_itr++) {
                        if (delete_value.exact_match(*exact_itr)) {
                            (*exact_itr).slot_id = (*itr).slot_id;
                            (*exact_itr).page_id = (*itr).page_id;
                            found_exact = true;
                            break;
                        }
                    }
                    if (!found_exact) {
                        BPlusTreeLeafNode exact_node = node;
                        int exact_page_id = page_id, child_item_slot_id = 0;
                        std::list<BPlusTreeLeafChild>::iterator exact_itr;
                        while (!found_exact) {
                            if (exact_node.next_page_id == -1) {
                                break;
                            }
                            exact_page_id = exact_node.next_page_id;
                            b = bpm->getPage(file_id, exact_node.next_page_id,
                                             index);
                            readBPlusTreeLeafNodeFromPage(b, exact_node,
                                                          index_key_num);
                            bpm->access(index);
                            bool failed = false;
                            child_item_slot_id = 0;
                            for (exact_itr = exact_node.children.begin();
                                 exact_itr != exact_node.children.end();
                                 exact_itr++, child_item_slot_id++) {
                                if (delete_value.exact_match(*exact_itr)) {
                                    found_exact = true;
                                    break;
                                } else if (delete_value < *exact_itr) {
                                    failed = true;
                                    break;
                                }
                            }
                            if (found_exact) break;
                            if (failed) break;
                        }
                        if (!found_exact) {
                            // 没找到
                            return false;
                        }

                        // 写回
                        setLeafChildSlotIDAndPageID(
                            file_id, exact_page_id, child_item_slot_id,
                            itr->page_id, itr->slot_id, index_key_num);
                    }
                }
                // 删掉找到的位置
                found = true;
                update_max_val = (std::next(itr) == node.children.end());
                node.children.erase(itr);
                break;
            } else if (delete_value < *itr) {
                break;
            }
        }
        if (!found) {
            // 没找到
            return false;
        }
        tmp_tree_underflow = false;
        if (node.next_page_id != -1 &&
            (int)node.children.size() < (b_plus_tree_m + 1) / 2) {
            // 出现下溢
            b = bpm->getPage(file_id, node.next_page_id, index);
            BPlusTreeLeafNode next_node;
            readBPlusTreeLeafNodeFromPage(b, next_node, index_key_num);
            bpm->access(index);

            if ((int)next_node.children.size() + (int)node.children.size() <=
                b_plus_tree_m) {
                // 合并节点
                tmp_tree_underflow = true;
                update_max_val = false;
                next_node.children.splice(next_node.children.begin(),
                                          node.children);

                // 修改链表
                next_node.prev_page_id = node.prev_page_id;
                if (next_node.prev_page_id != -1) {
                    setNextPageID(file_id, next_node.prev_page_id,
                                  node.next_page_id);
                }

                // next node 写回
                b = bpm->getPage(file_id, node.next_page_id, index);
                writeBPlusTreeLeafNode2Page(b, next_node, index_key_num);
                bpm->markDirty(index);

                // 删除节点
                setBitMapPage(file_id, 1, page_id, false);
            } else {
                // 借一个节点
                node.children.push_back(next_node.children.front());
                next_node.children.pop_front();
                update_max_val = true;

                // next node 写回
                b = bpm->getPage(file_id, node.next_page_id, index);
                writeBPlusTreeLeafNode2Page(b, next_node, index_key_num);
                bpm->markDirty(index);
            }
        }

        if (node.next_page_id == -1 && node.children.size() == 0 &&
            node.prev_page_id != -1) {
            tmp_tree_underflow = true;
            update_max_val = false;

            setNextPageID(file_id, node.prev_page_id, -1);

            setBitMapPage(file_id, 1, page_id, false);
        }
        if (update_max_val) {
            // 更新max值
            if (node.children.size() == 0) {
                tmp_tree_internal_child[0].max_key.clear();
                for (int i = 0; i < index_key_num; i++)
                    tmp_tree_internal_child[0].max_key.push_back(INT_MIN);
            } else {
                tmp_tree_internal_child[0].max_key = node.children.back().key;
            }
            tmp_tree_internal_child[0].page_id = page_id;
        } else {
            tmp_tree_internal_child[0].page_id = -1;
        }

        // 写回
        if (!tmp_tree_underflow) {
            // underflow的情况不用写回
            b = bpm->getPage(file_id, page_id, index);
            writeBPlusTreeLeafNode2Page(b, node, index_key_num);
            bpm->markDirty(index);
        }

        return true;
    } else {
        // 不是叶节点

        // 读取节点
        BPlusTreeInternalNode node;
        readBPlusTreeInternalNodeFromPage(b, node, index_key_num);
        bpm->access(index);

        // 搜索
        for (auto itr = node.children.begin(); itr != node.children.end();
             itr++) {
            if (delete_value <= *itr) {
                if (!deleteNode(file_id, (*itr).page_id, delete_value,
                                exact_match, index_key_num, b_plus_tree_m)) {
                    // 没找到
                    return false;
                }
                if (tmp_tree_underflow) {
                    // 子节点踢出去
                    tmp_tree_underflow = false;
                    bool update_max_val =
                        (std::next(itr) == node.children.end());
                    node.children.erase(itr);
                    if ((int)node.children.size() < (b_plus_tree_m + 1) / 2 &&
                        node.next_page_id != -1) {
                        // 出现下溢
                        b = bpm->getPage(file_id, node.next_page_id, index);
                        BPlusTreeInternalNode next_node;
                        readBPlusTreeInternalNodeFromPage(b, next_node,
                                                          index_key_num);
                        bpm->access(index);

                        if ((int)next_node.children.size() +
                                (int)node.children.size() <=
                            b_plus_tree_m) {
                            // 合并节点
                            tmp_tree_underflow = true;
                            update_max_val = false;
                            next_node.children.splice(
                                next_node.children.begin(), node.children);

                            // 修改链表
                            next_node.prev_page_id = node.prev_page_id;
                            if (next_node.prev_page_id != -1) {
                                setNextPageID(file_id, next_node.prev_page_id,
                                              node.next_page_id);
                            }

                            // next node 写回
                            b = bpm->getPage(file_id, node.next_page_id, index);
                            writeBPlusTreeInternalNode2Page(b, next_node,
                                                            index_key_num);
                            bpm->markDirty(index);

                            // 删除节点
                            setBitMapPage(file_id, 1, page_id, false);
                        } else {
                            node.children.push_back(next_node.children.front());
                            next_node.children.pop_front();
                            update_max_val = true;

                            // next node 写回
                            b = bpm->getPage(file_id, node.next_page_id, index);
                            writeBPlusTreeInternalNode2Page(b, next_node,
                                                            index_key_num);
                            bpm->markDirty(index);
                        }
                    }
                    if (node.next_page_id == -1 && node.children.size() == 0 &&
                        node.prev_page_id != -1) {
                        tmp_tree_underflow = true;
                        update_max_val = false;

                        setNextPageID(file_id, node.prev_page_id, -1);

                        setBitMapPage(file_id, 1, page_id, false);
                    }
                    if (update_max_val) {
                        // 更新max值
                        tmp_tree_internal_child[0].max_key =
                            node.children.back().max_key;
                        tmp_tree_internal_child[0].page_id = page_id;
                    } else {
                        tmp_tree_internal_child[0].page_id = -1;
                    }

                    if (!tmp_tree_underflow) {
                        // 写回
                        b = bpm->getPage(file_id, page_id, index);
                        writeBPlusTreeInternalNode2Page(b, node, index_key_num);
                        bpm->markDirty(index);
                    }
                } else if (tmp_tree_internal_child[0].page_id != -1) {
                    // 更新max值
                    (*itr).max_key = tmp_tree_internal_child[0].max_key;
                    if (std::next(itr) == node.children.end()) {
                        // 更新max值
                        tmp_tree_internal_child[0].max_key =
                            node.children.back().max_key;
                        tmp_tree_internal_child[0].page_id = page_id;
                    } else {
                        tmp_tree_internal_child[0].page_id = -1;
                    }

                    // 写回
                    b = bpm->getPage(file_id, page_id, index);
                    writeBPlusTreeInternalNode2Page(b, node, index_key_num);
                    bpm->markDirty(index);
                }
                return true;
            }
        }

        // 没找到
        return false;
    }
}

bool IndexManager::deleteIndexFile(const char* file_path) {
    // 检查文件是否存在
    closeFileIfExist(file_path);
    return fm->deleteFile(file_path);
}

}  // namespace index
}  // namespace dbs