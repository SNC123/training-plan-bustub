//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

// info with color
// #define COLOR_RESET "\033[0m"
// #define COLOR_YELLOW "\033[33m"  // warn
// #define COLOR_PURPLE "\033[35m"  // special debug
// #define COLOR_GREEN "\033[32m"   // info
// #define PRINT_COLOR_MARCO(CODE) fprintf(LOG_OUTPUT_STREAM, "%s", CODE);
// #define START_WARN PRINT_COLOR_MARCO(COLOR_YELLOW)
// #define START_SPECIAL PRINT_COLOR_MARCO(COLOR_PURPLE)
// #define START_INFO PRINT_COLOR_MARCO(COLOR_GREEN)
// #define CLEAR_COLOR PRINT_COLOR_MARCO(COLOR_RESET)

// set my log level
#define LOG_LEVEL LOG_LEVEL_OFF

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K>
auto KeyToLog(const K &key) -> uint64_t {
  if constexpr (std::is_same_v<K, int>) {
    return static_cast<uint64_t>(key);
  } else {
    return key.ToString();
  }
}

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  LOG_DEBUG("header_max_depth %d", header_max_depth_);
  LOG_DEBUG("directory_max_depth %d", directory_max_depth_);
  LOG_DEBUG("bucket_max_size %d", bucket_max_size_);
  // fprintf(LOG_OUTPUT_STREAM, "%s", COLOR_RESET);
  index_name_ = name;
  // initialize header page
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto header_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  header_page_id_ = bucket_page_id;
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  // calculate hash
  uint32_t hash = Hash(key);
  // fetch header
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // fetch directory
  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // fetch bucket
  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto *bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  // fetch value
  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  // calculate hash
  uint32_t hash = Hash(key);
  // fetch header,create directory if not exists
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }
  // gc header
  header_guard.Drop();
  // fetch directory,create bucket if not exists
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  // fetch bucket
  uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_idx, key, value);
  }
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // spilt target bucket and update mapping when full
  while (bucket_page->IsFull()) {
    // record local depth mask
    auto old_local_depth_mask = directory_page->GetLocalDepthMask(bucket_idx);
    // if LD = GD, increase GD
    if (directory_page->GetLocalDepth(bucket_idx) == directory_page->GetGlobalDepth()) {
      if (directory_page->GetGlobalDepth() == directory_max_depth_) {
        // if GD meets limit, fail
        // START_WARN
        LOG_WARN("global depth meets the limit");
        // CLEAR_COLOR
        return false;
      }
      directory_page->IncrGlobalDepth();
    }

    // create new bucket
    page_id_t image_bucket_page_id = INVALID_PAGE_ID;
    auto image_bucket_page_guard = bpm_->NewPageGuarded(&image_bucket_page_id).UpgradeWrite();
    auto image_bucket_page = image_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    image_bucket_page->Init(bucket_max_size_);
    // updating mapping
    uint32_t least_bucket_idx = (bucket_idx & directory_page->GetLocalDepthMask(bucket_idx));
    uint32_t least_spilt_bucket_idx = least_bucket_idx + (1 << directory_page->GetLocalDepth(bucket_idx));
    // increase local depth
    directory_page->IncrLocalDepth(bucket_idx);
    UpdateDirectoryMapping(directory_page, least_bucket_idx, bucket_page_id, directory_page->GetLocalDepth(bucket_idx),
                           directory_page->GetLocalDepthMask(bucket_idx));
    UpdateDirectoryMapping(directory_page, least_spilt_bucket_idx, image_bucket_page_id,
                           directory_page->GetLocalDepth(bucket_idx), directory_page->GetLocalDepthMask(bucket_idx));

    // migrate
    uint32_t entry_idx = 0;
    while (entry_idx < bucket_page->Size()) {
      uint32_t hash = Hash(bucket_page->KeyAt(entry_idx));
      auto old_bucket_idx = (old_local_depth_mask & hash);
      // if hash to new idx, migrate
      if (directory_page->HashToBucketIndex(hash) != old_bucket_idx) {
        // START_SPECIAL
        LOG_DEBUG("move key %ld to page %d", KeyToLog(bucket_page->KeyAt(entry_idx)), image_bucket_page_id);
        // CLEAR_COLOR
        image_bucket_page->Insert(bucket_page->KeyAt(entry_idx), bucket_page->ValueAt(entry_idx), cmp_);
        bucket_page->RemoveAt(entry_idx);
        // because remove operation just move last element to current position
        continue;
      }
      entry_idx++;
    }
    // update target bucket page
    uint32_t hash = Hash(key);
    uint32_t target_bucket_idx = directory_page->HashToBucketIndex(hash);
    page_id_t target_page_id = directory_page->GetBucketPageId(target_bucket_idx);
    if (target_page_id == image_bucket_page_id) {
      bucket_page = image_bucket_page;
      bucket_page_id = image_bucket_page_id;
    }
  }
  // START_SPECIAL
  LOG_DEBUG("insert key %ld to page %d", KeyToLog(key), bucket_page_id);
  // CLEAR_COLOR
  // just insert if there is room
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  // create new directory
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  if (InsertToNewBucket(directory_page, directory_page->HashToBucketIndex(hash), key, value)) {
    header->SetDirectoryPageId(directory_idx, directory_page_id);
    return true;
  }
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  // create new bucket
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_page_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  if (bucket_page->Insert(key, value, cmp_)) {
    // maintain bucket_idx map to page id
    directory->SetBucketPageId(bucket_idx, bucket_page_id);
    return true;
  }
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  /*
      example:
      001 |
      011 |
          ->  [full bucket](LD=1)
      101 |
      111 |
      when insert(001/011/101/111) into this full bucket
      we just need updating bucket_idx with '11' suffix
      '11' = get_spilted_index(001/011/101/111)
  */
  const uint32_t least_bucket_idx = new_bucket_idx;
  const uint32_t step = local_depth_mask + 1;
  for (uint32_t idx = least_bucket_idx; idx < directory->Size(); idx += step) {
    directory->SetBucketPageId(idx, new_bucket_page_id);
    directory->SetLocalDepth(idx, new_local_depth);
  }
}

// template <typename K, typename V, typename KC>
// void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
//                                                        ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
//                                                        uint32_t new_bucket_idx, uint32_t local_depth_mask) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // calculate hash
  uint32_t hash = Hash(key);
  // fetch header,create directory if not exists
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  // gc header
  header_guard.Drop();
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // fetch directory
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  // fecth bucket
  uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }
  if (directory_page->GetGlobalDepth() == 0) {
    return true;
  }
  // START_SPECIAL
  LOG_DEBUG("remove key %ld", KeyToLog(key));
  // CLEAR_COLOR
  // calculate image info
  uint32_t offset = 0;
  uint32_t old_local_depth = directory_page->GetLocalDepth(bucket_idx);
  if (old_local_depth != 0) {
    offset = old_local_depth - 1;
  }
  uint32_t least_bucket_idx = (bucket_idx & directory_page->GetLocalDepthMask(bucket_idx));
  uint32_t least_image_bucket_idx = (bucket_idx ^ (1 << offset));
  page_id_t image_bucket_page_id = directory_page->GetBucketPageId(least_image_bucket_idx);
  // fetch image bucket
  auto least_image_bucket_guard = bpm_->FetchPageWrite(image_bucket_page_id);
  auto least_image_bucket_page = least_image_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  while (bucket_page->IsEmpty() || least_image_bucket_page->IsEmpty()) {
    // special empty case
    if (directory_page->GetGlobalDepth() == 0) {
      directory_page->SetBucketPageId(bucket_idx, INVALID_PAGE_ID);
      break;
    }
    if (directory_page->GetLocalDepth(least_image_bucket_idx) != old_local_depth) {
      return true;
    }
    // merge if local depth is same
    if (bucket_page->IsEmpty()) {
      // point current to image
      UpdateDirectoryMapping(directory_page, least_bucket_idx, image_bucket_page_id, old_local_depth - 1,
                             directory_page->GetLocalDepthMask(bucket_idx));
      UpdateDirectoryMapping(directory_page, least_image_bucket_idx, image_bucket_page_id, old_local_depth - 1,
                             directory_page->GetLocalDepthMask(bucket_idx));
    } else {
      // point image to current
      UpdateDirectoryMapping(directory_page, least_bucket_idx, bucket_page_id, old_local_depth - 1,
                             directory_page->GetLocalDepthMask(bucket_idx));
      UpdateDirectoryMapping(directory_page, least_image_bucket_idx, bucket_page_id, old_local_depth - 1,
                             directory_page->GetLocalDepthMask(bucket_idx));
    }
    // shrink
    if (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
    bucket_guard.Drop();
    least_image_bucket_guard.Drop();
    if (directory_page->GetGlobalDepth() == 0) {
      return true;
    }
    // update current bucket
    if (bucket_page->IsEmpty()) {
      bucket_page_id = image_bucket_page_id;
    }
    bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_idx = directory_page->HashToBucketIndex(hash);
    // least_image_bucket_guard.Drop();
    // calculate new image info
    offset = 0;
    old_local_depth = directory_page->GetLocalDepth(bucket_idx);
    if (old_local_depth != 0) {
      offset = old_local_depth - 1;
    }
    least_bucket_idx = (bucket_idx & directory_page->GetLocalDepthMask(bucket_idx));
    least_image_bucket_idx = (bucket_idx ^ (1 << offset));
    image_bucket_page_id = directory_page->GetBucketPageId(least_image_bucket_idx);
    if (image_bucket_page_id == bucket_page_id) {
      break;
    }
    // fetch new image bucket
    least_image_bucket_guard = bpm_->FetchPageWrite(image_bucket_page_id);
    least_image_bucket_page = least_image_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
