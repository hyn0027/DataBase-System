#pragma once

#include "common/Config.hpp"
#include "fs/BufPageManager.hpp"
#include "fs/FileManager.hpp"
#include "index/IndexType.hpp"
#include "record/DataType.hpp"
#include "utils/BitOperations.hpp"

namespace dbs {
namespace index {

class IndexManager {
   public:
    /**
     * @brief Construct a new Index Manager object
     *
     * @param fm_ file manager
     * @param bpm_ buffer page manager
     */
    IndexManager(fs::FileManager* fm_, fs::BufPageManager* bpm_);

    /**
     * @brief Destroy the Index Manager object
     *
     */
    ~IndexManager();

    /**
     * @brief create an index file
     *
     * @param file_path
     * @param index_key_num how many int columns are indexed (不算page id &
     slot
     * id)
     */
    void initializeIndexFile(const char* file_path, int index_key_num);

    /**
     * @brief insert an index
     *
     * @param file_path index file path
     * @param index_value index value
     * @return true success false fail (key数量不符)
     */
    bool insertIndex(const char* file_path, const IndexValue& index_value);

    /**
     * @brief delete index, 如果匹配的有多个，删除第一个；exact
     * match尽量选false，效率更高
     *
     * @param file_path index file path
     * @param index_value index value
     * @param exact_match true: slot_id和page_id都要匹配，false: 只要key匹配
     * @return true success false fail (key数量不符)，合法但没找到会返回false
     */
    bool deleteIndex(const char* file_path, const IndexValue& index_value,
                     bool exact_match);

    /**
     * @brief search an index (key exact match)
     *
     * @param file_path index file path
     * @param index_value index value
     * @param record_locations return record locations
     * @return true success， false fail (key数量不符)，合法但没找到会返回true
     */
    bool searchIndex(const char* file_path, const IndexValue& search_value,
                     std::vector<IndexValue>& search_results);

    /**
     * @brief search an index (key range match, 左闭右闭)
     *
     * @param file_path
     * @param index_value_low
     * @param index_value_high
     * @param record_locations
     * @return true success， false fail (key数量不符)，合法但没找到会返回true
     */
    bool searchIndexInRanges(const char* file_path,
                             const IndexValue& search_value_low,
                             const IndexValue& search_value_high,
                             std::vector<IndexValue>& search_results);

    /**
     * @brief delete an index file
     *
     * @param file_path
     * @return true success
     * @return false fail
     */
    bool deleteIndexFile(const char* file_path);

    /**
     * @brief close all current opening file
     */
    void closeAllCurrentFile();

   private:
    /**
     * @brief write a tree interal node into a buftype b,
     * 调用之后需要显式调用markdirty
     * @param b
     * @param node
     * @param index_key_num
     */
    void writeBPlusTreeInternalNode2Page(BufType b,
                                         const BPlusTreeInternalNode& node,
                                         int index_key_num);

    /**
     * @brief write a tree leaf node into a buftype b,
     * 调用之后需要显式调用markdirty
     * @param b
     * @param node
     * @param index_key_num
     */
    void writeBPlusTreeLeafNode2Page(BufType b, const BPlusTreeLeafNode& node,
                                     int index_key_num);

    /**
     * @brief read a tree interal node from a buftype b,
     * 调用之后需要显式调用access
     * @param b
     * @param node
     * @param index_key_num
     */
    void readBPlusTreeInternalNodeFromPage(BufType b,
                                           BPlusTreeInternalNode& node,
                                           int index_key_num);

    /**
     * @brief read a tree leaf node from a buftype b,
     * 调用之后需要显式调用access
     * @param b
     * @param node
     * @param index_key_num
     */
    void readBPlusTreeLeafNodeFromPage(BufType b, BPlusTreeLeafNode& node,
                                       int index_key_num);

    /**
     * @brief 创建一个空的bitmap页，如果页上有值，则全部覆盖
     *
     * @param file_id
     * @param page_id
     */
    void createEmptyBitMapPage(int file_id, int page_id);

    /**
     * @brief 设置bitmap页上的某一位
     *
     * @param file_id
     * @param base_page_id bitmap的第一个页面
     * @param insert_pos 插入的位置，也即bitmap的下标
     * @param insert_val true or false
     * @return true 成功
     * @return false 失败(bitmap 长度不够)
     */
    bool setBitMapPage(int file_id, int base_page_id, int insert_pos,
                       bool insert_val);

    /**
     * @brief 寻找第一个还没有使用的页面，如果页号的bitmap满了会自动继续分配
     * @param file_id
     * @param set 是否设置为已经使用
     * @return int 页号
     */
    int getFirstEmptyPageId(int file_id, bool set);

    /**
     * @brief 给定键的数量，返回B+树的M值
     * @param index_key_num
     * @return
     */
    int getBPlusTreeM(int index_key_num);

    /**
     * @brief 给定键的数量，返回B+树的一条记录的长度, 单位：Byte
     * @param index_key_num
     * @return
     */
    int getBPlusTreeItemLength(int index_key_num);

    /**
     * @brief
     * 使用缓存打开文件，如果文件已经打开，直接返回，否则自动关闭当前打开的文件
     * @param file_path
     * @return
     */
    int openFile(const char* file_path);

    void closeFirstFile();
    void closeFileIfExist(const char* file_path);

    /**
     * @brief 从page_id对应的节点向下找，插入叶节点
     * 不会处理调用根节点的上溢问题，需要调用者自行处理
     * @param file_id
     * @param page_id
     * @param insert_item
     * @param index_key_num
     * @param b_plus_tree_m
     */
    void insertNode(int file_id, int page_id,
                    const BPlusTreeLeafChild& insert_item, int index_key_num,
                    int b_plus_tree_m);

    /**
     * @brief
     * 返回第一个大于等于search_value的叶节点的位置
     * @param file_id
     * @param page_id
     * @param index_value
     * @param leaf_result
     * @return true 成功 false 失败 （没有找到）
     */
    bool searchNode(int file_id, int page_id,
                    const BPlusTreeLeafChild& search_value, int index_key_num,
                    BPlusTreeLeafNode& leaf_result);

    bool deleteNode(int file_id, int page_id,
                    const BPlusTreeLeafChild& delete_value, bool exact_match,
                    int index_key_num, int b_plus_tree_m);

    /**
     * @brief
     * 检查插入子节点的时候，子节点是否出现了上溢；
     * 如果没有，返回false，代表当前节点不需要修改；
     * 如果有，相应的修改node，返回true；
     * 如果当前节点因为子节点的上溢而上溢，则split成当前节点和一个新节点，
     * 新节点写入文件，修改同级页面内的链表指针，
     * 相应修改node和tmp_tree_internal_child和node，返回true；
     *
     * @param file_id
     * @param page_id 原节点所在的页号
     * @param node 原节点
     * @param insert_pos 插入孩子的位置
     * @param index_key_num 键的数量
     * @param b_plus_tree_m B+树的M值
     * @return
     */
    bool handleInternalNodeOverflow(
        int file_id, int page_id, BPlusTreeInternalNode& node,
        const std::list<BPlusTreeInternalChild>::iterator& insert_pos,
        int index_key_num, int b_plus_tree_m);

    /**
     * @brief
     * 检查插入叶节点的时候，叶节点是否出现了上溢；
     * 如果没有，返回false，代表当前节点不需要修改；
     * 如果有，相应的修改node，返回true；
     * 如果当前节点上溢，则split成当前节点和一个新节点，
     * 新节点写入文件，修改同级页面内的链表指针，
     * 相应修改node和tmp_tree_internal_child和node，返回true；
     *
     * @param file_id
     * @param page_id
     * @param node
     * @param index_key_num
     * @param b_plus_tree_m
     */
    void handleLeafNodeOverflow(int file_id, int page_id,
                                BPlusTreeLeafNode& node, int index_key_num,
                                int b_plus_tree_m);

    /**
     * @brief 把origin_node从中间(M + 1) /
     * 2处分成两个节点，修改相应的page链表值，
     * 并修改tmp_tree_internal_child
     * @param origin_node
     * @param origin_node_page_id
     * @param new_node
     * @param new_node_page_id
     * @param index_key_num
     * @param b_plus_tree_m
     */
    void splitInternalNode(BPlusTreeInternalNode& origin_node,
                           BPlusTreeInternalNode& new_node,
                           int origin_node_page_id, int new_node_page_id,
                           int index_key_num, int b_plus_tree_m);

    /**
     * @brief  把origin_node从中间(M + 1) /
     * 2处分成两个节点，修改相应的page链表值，
     * 并修改tmp_tree_internal_child
     * @param origin_node
     * @param new_node
     * @param origin_node_page_id
     * @param new_node_page_id
     * @param index_key_num
     * @param b_plus_tree_m
     * @return
     */
    void splitLeafNode(BPlusTreeLeafNode& origin_node,
                       BPlusTreeLeafNode& new_node, int origin_node_page_id,
                       int new_node_page_id, int index_key_num,
                       int b_plus_tree_m);

    /**
     * @brief 在已有页面上写入节点链表中prev指针的值，mark dirty，其他不做修改
     * @param file_id
     * @param page_id
     * @param prev_page_id
     */
    void setPrevPageID(int file_id, int page_id, int prev_page_id);

    /**
     * @brief  在已有页面上写入节点链表中next指针的值，mark dirty，其他不做修改
     *
     * @param file_id
     * @param page_id
     * @param next_page_id
     */
    void setNextPageID(int file_id, int page_id, int next_page_id);

    /**
     * @brief 在已有叶节点页面上修改某个item的slot id和page id，mark
     * dirty，其他不做修改
     * @param file_id
     * @param page_id
     * @param prev_page_id
     */
    void setLeafChildSlotIDAndPageID(int file_id, int page_id, int child_id,
                                     int item_page_id, int item_slot_id,
                                     int index_key_num);

    /**
     * @brief
     * 从base开始向后查找小于等于search_value_high的所有节点，递归放进search_results里
     *
     * @param file_id
     * @param base_node
     * @param base_slot_itr
     * @param index_key_num
     * @param search_value_high
     * @param search_results
     */
    void searchForward(int file_id, const BPlusTreeLeafNode& base_node,
                       int index_key_num,
                       const BPlusTreeLeafChild& search_value_low,
                       const BPlusTreeLeafChild& search_value_high,
                       std::vector<IndexValue>& search_results);

    fs::FileManager* fm;
    fs::BufPageManager* bpm;

    std::vector<char*> current_opening_file_paths;
    std::vector<int> current_opening_file_ids;
    const int opening_cache_capacity = 10;

    // 插入时表示子节点是否上溢，上溢的话溢出的是哪两个节点
    // 删除时表示maxval是否变化了，仅第一项有效
    BPlusTreeInternalChild tmp_tree_internal_child[2];

    // 删除时表示子节点是否下溢
    bool tmp_tree_underflow = false;
};

}  // namespace index
}  // namespace dbs