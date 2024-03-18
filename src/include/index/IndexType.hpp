#pragma once

#include <climits>
#include <iostream>
#include <list>
#include <vector>

#include "common/Config.hpp"
#include "record/DataType.hpp"

namespace dbs {
namespace index {

struct IndexValue {
    int page_id;
    int slot_id;
    std::vector<int> key;
    IndexValue();
    IndexValue(int page_id_, int slot_id_, int index_key_num_);
    IndexValue(int page_id_, int slot_id_, std::vector<int> key_);
    bool operator<(const IndexValue& other) const {
        for (size_t i = 0; i < key.size(); i++) {
            if (key[i] < other.key[i]) return true;
            if (key[i] > other.key[i]) return false;
        }
        return false;
    }
    void print() const {
        for (size_t i = 0; i < key.size(); i++) {
            std::cout << key[i];
            if (i != key.size() - 1) std::cout << ", ";
        }
        std::cout << std::endl;
        ;
    }
};

void indexValueToRecordLocation(const std::vector<IndexValue>& index_values,
                                std::vector<record::RecordLocation>& locations);

struct BPlusTreeInternalChild {
    std::vector<int> max_key;
    int page_id;
    BPlusTreeInternalChild();
    BPlusTreeInternalChild(int page_id_, int index_key_num_);
    BPlusTreeInternalChild(int page_id_, std::vector<int> max_key_);
};

struct BPlusTreeInternalNode {
    int prev_page_id, next_page_id;  // 同级节点
    std::list<BPlusTreeInternalChild> children;
    BPlusTreeInternalNode();
    BPlusTreeInternalNode(int prev_page_id_, int next_page_id_);
};

struct BPlusTreeLeafChild {
    std::vector<int> key;
    int page_id, slot_id;
    BPlusTreeLeafChild(IndexValue index_value);
    BPlusTreeLeafChild(int page_id_, int slot_id_, int index_key_num_);
    bool operator<(const BPlusTreeLeafChild& other) const {
        for (size_t i = 0; i < key.size(); i++) {
            if (key[i] < other.key[i]) return true;
            if (key[i] > other.key[i]) return false;
        }
        return false;
    }
    bool operator<=(const BPlusTreeLeafChild& other) const {
        for (size_t i = 0; i < key.size(); i++) {
            if (key[i] < other.key[i]) return true;
            if (key[i] > other.key[i]) return false;
        }
        return true;
    }
    bool operator==(const BPlusTreeLeafChild& other) const {
        for (size_t i = 0; i < key.size(); i++) {
            if (key[i] != other.key[i]) return false;
        }
        return true;
    }
    bool operator<(const BPlusTreeInternalChild& other) const {
        for (size_t i = 0; i < key.size(); i++) {
            if (key[i] < other.max_key[i]) return true;
            if (key[i] > other.max_key[i]) return false;
        }
        return false;
    }
    bool operator<=(const BPlusTreeInternalChild& other) const {
        for (size_t i = 0; i < key.size(); i++) {
            if (key[i] < other.max_key[i]) return true;
            if (key[i] > other.max_key[i]) return false;
        }
        return true;
    }
    bool exact_match(const BPlusTreeLeafChild& other) const {
        for (size_t i = 0; i < key.size(); i++) {
            if (key[i] != other.key[i]) return false;
        }
        return page_id == other.page_id && slot_id == other.slot_id;
    }
};

struct BPlusTreeLeafNode {
    int prev_page_id, next_page_id;  // 同级节点
    std::list<BPlusTreeLeafChild> children;
    BPlusTreeLeafNode();
    BPlusTreeLeafNode(int prev_page_id_, int next_page_id_);
};

}  // namespace index
}  // namespace dbs