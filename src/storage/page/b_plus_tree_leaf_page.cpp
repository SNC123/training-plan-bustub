//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

/* ==================================== ADDED ====================================*/

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetKVAt(int index, const KeyType &key, const ValueType &value) -> void {
  array_[index] = {key, value};
}
/*
 * Helper method to insert key-value pair IN ASCENDING ORDER
 * return false if full or key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertKV(KeyType key, ValueType value, KeyComparator key_comparator) -> bool {
  if (GetSize() == GetMaxSize()) {
    UNREACHABLE("Full leaf page should be spilted");
    return false;
  }
  for (auto idx = 0; idx < GetSize(); ++idx) {
    auto cmp_rlt = key_comparator(KeyAt(idx), key);
    if (cmp_rlt == 0) {
      return false;
    }
    if (cmp_rlt > 0) {
      for (auto move_idx = GetSize(); move_idx > idx; --move_idx) {
        array_[move_idx] = array_[move_idx - 1];
      }
      array_[idx] = {key, value};
      IncreaseSize(1);
      return true;
    }
  }
  array_[GetSize()] = {key, value};
  IncreaseSize(1);
  return true;
}
/*
 * Helper method to get value
 * return true if key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValueByKey(KeyType key, std::vector<ValueType> *result,
                                               KeyComparator key_comparator) const -> bool {
  for (auto idx = 0; idx < GetSize(); ++idx) {
    if (key_comparator(KeyAt(idx), key) == 0) {
      result->push_back(ValueAt(idx));
      return true;
    }
  }
  return false;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
