#include "index/IndexType.hpp"

namespace dbs {
namespace index {

BPlusTreeInternalChild::BPlusTreeInternalChild() {
    page_id = -1;
    max_key.clear();
}

BPlusTreeInternalChild::BPlusTreeInternalChild(int page_id_,
                                               int index_key_num_) {
    page_id = page_id_;
    for (int i = 0; i < index_key_num_; i++) {
        max_key.push_back(INT_MIN);
    }
}

BPlusTreeInternalChild::BPlusTreeInternalChild(int page_id_,
                                               std::vector<int> max_key_) {
    page_id = page_id_;
    max_key = max_key_;
}

BPlusTreeInternalNode::BPlusTreeInternalNode(int prev_page_id_,
                                             int next_page_id_) {
    prev_page_id = prev_page_id_;
    next_page_id = next_page_id_;
    children.clear();
}

BPlusTreeInternalNode::BPlusTreeInternalNode() {
    prev_page_id = -1;
    next_page_id = -1;
    children.clear();
}

BPlusTreeLeafChild::BPlusTreeLeafChild(int page_id_, int slot_id_,
                                       int index_key_num_) {
    page_id = page_id_;
    slot_id = slot_id_;
    for (int i = 0; i < index_key_num_; i++) {
        key.push_back(INT_MIN);
    }
}

BPlusTreeLeafChild::BPlusTreeLeafChild(IndexValue index_value) {
    page_id = index_value.page_id;
    slot_id = index_value.slot_id;
    key = index_value.key;
}

BPlusTreeLeafNode::BPlusTreeLeafNode() {
    prev_page_id = -1;
    next_page_id = -1;
    children.clear();
}

BPlusTreeLeafNode::BPlusTreeLeafNode(int prev_page_id_, int next_page_id_) {
    prev_page_id = prev_page_id_;
    next_page_id = next_page_id_;
    children.clear();
}

IndexValue::IndexValue() {
    page_id = -1;
    slot_id = -1;
    key.clear();
}

IndexValue::IndexValue(int page_id_, int slot_id_, std::vector<int> key_) {
    page_id = page_id_;
    slot_id = slot_id_;
    key = key_;
}

IndexValue::IndexValue(int page_id_, int slot_id_, int index_key_num_) {
    page_id = page_id_;
    slot_id = slot_id_;
    for (int i = 0; i < index_key_num_; i++) {
        key.push_back(INT_MIN);
    }
}

void indexValueToRecordLocation(
    const std::vector<IndexValue>& index_values,
    std::vector<record::RecordLocation>& locations) {
    locations.clear();
    for (size_t i = 0; i < index_values.size(); i++) {
        record::RecordLocation location;
        location.page_id = index_values[i].page_id;
        location.slot_id = index_values[i].slot_id;
        locations.push_back(location);
    }
}

}  // namespace index
}  // namespace dbs