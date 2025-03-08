//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1); /*reserve first element*/
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/* ==================================== ADDED ====================================*/

/*
 * Helper method to insert (left_value, key, right_value)
 * return false if full or key exists
 * e.g.
 * empty internal node after insert one LvKeyRv:
 * [_|_] → [_|left_value,key|right_value]
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertLvKeyRv(ValueType left_value, KeyType key, ValueType right_value,
                                                   KeyComparator key_comparator) -> bool {
  for (auto idx = 1 /*ignore first key*/; idx < GetSize(); ++idx) {
    auto cmp_rlt = key_comparator(KeyAt(idx), key);
    if (cmp_rlt == 0) {
      return false;
    }
    if (cmp_rlt > 0) {
      for (auto move_idx = GetSize(); move_idx > idx; --move_idx) {
        array_[move_idx] = array_[move_idx - 1];
      }
      array_[idx] = {key, right_value};
      array_[idx - 1].second = left_value;
      IncreaseSize(1);
      return true;
    }
  }
  array_[GetSize()] = {key, right_value};
  array_[GetSize() - 1].second = left_value;
  IncreaseSize(1);
  return true;
}

/*
 * Helper method to find next page id
 * return value is the next page (internal or leaf) id
 * e.g.
 * For internal node contains keys(first invalid) [_,4,7] ：
 * search key = 3 → _
 * search key = 4/5/6 → 4
 * search key = 7/7+ → 7
 *
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetTargetPageIdByKey(KeyType key, KeyComparator key_comparator) const
    -> ValueType {
  auto target_idx = GetSize() - 1;
  for (auto idx = 1 /*ignore first key*/; idx < GetSize(); ++idx) {
    if (key_comparator(KeyAt(idx), key) > 0) {
      target_idx = idx - 1;
      break;
    }
  }
  return ValueAt(target_idx);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
