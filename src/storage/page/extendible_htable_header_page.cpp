//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"
#include <cstdint>

#include "common/config.h"
#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  // necessary?
  max_depth_ = std::min(HTABLE_HEADER_MAX_DEPTH, static_cast<uint64_t>(max_depth));
  std::fill(directory_page_ids_, directory_page_ids_ + HTABLE_HEADER_ARRAY_SIZE, INVALID_PAGE_ID);
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  // special case: "hash(uint32) >> 32" is undefined.
  if (max_depth_ == 0) {
    return 0;
  }
  auto offset = HTABLE_HEADER_PAGE_METADATA_SIZE * 8 - max_depth_;
  return hash >> offset;
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return HTABLE_HEADER_ARRAY_SIZE; }

}  // namespace bustub
