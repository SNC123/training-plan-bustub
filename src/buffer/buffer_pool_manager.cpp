//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <mutex>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  std::cout << "try to new page " << std::endl;
  frame_id_t frame_id;
  // check available position in free_list
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    // check available position in replacer
  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&frame_id);
    auto old_page_id = pages_[frame_id].page_id_;
    // write dirty page to disk and reset page
    if (pages_[frame_id].IsDirty()) {
      FlushPage(old_page_id);
      pages_[frame_id].is_dirty_ = false;
    }
    // remove old page id
    page_table_.erase(old_page_id);
  } else {
    // there is no available position, so return nullptr
    return nullptr;
  }
  // allocate new page and maintain page_id to frame_id
  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;
  // set metadata
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].ResetMemory();

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  std::cout << "try to fetch page " << page_id << std::endl;
  // frame id for storage requested page
  frame_id_t frame_id;
  // search in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&frame_id);
    // remove old page id
    auto old_page_id = pages_[frame_id].page_id_;
    // write dirty page to disk and reset page
    if (pages_[frame_id].IsDirty()) {
      FlushPage(old_page_id);
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(old_page_id);
  } else {
    return nullptr;
  }
  page_table_[page_id] = frame_id;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_++;
  pages_[frame_id].is_dirty_ = false;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].ResetMemory();

  // fetch page from disk
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  // send read request
  disk_scheduler_->Schedule({false, pages_[frame_id].data_, page_id, std::move(promise)});
  if (future.get()) {
    std::cout << "read page " << page_id << " successfully" << std::endl;
  }
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  std::cout << "try to unpin page " << page_id << std::endl;
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];

  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  // warning: we can't unset dirty
  pages_[frame_id].is_dirty_ |= is_dirty;
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::cout << "try to flush page " << page_id << std::endl;
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  // std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id = page_table_[page_id];

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  // tell scheduler to write dirty page to disk
  disk_scheduler_->Schedule({true, pages_[frame_id].data_, page_id, std::move(promise)});
  // wait until worker thread set value
  if (future.get()) {
    std::cout << "flush page " << page_id << "("
              << "frame " << frame_id << ")"
              << " successfully" << std::endl;
    // std::cout << "data: " << pages_[frame_id].GetData() << std::endl;
    pages_[frame_id].is_dirty_ = false;
  }
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  std::cout << "try to flush all page " << std::endl;
  for (auto page : page_table_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, pages_[page.second].data_, page.first, std::move(promise)});
    if (future.get()) {
      std::cout << "flush page " << page.first << " successfully" << std::endl;
      pages_[page.second].is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  std::cout << "try to delete page " << page_id << std::endl;

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ >= 1) {
    return false;
  }
  // delete page id in page table
  page_table_.erase(page_id);
  // remove frame in replacer
  replacer_->Remove(frame_id);
  // insert frame into free_list
  free_list_.push_back(frame_id);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  // flush
  if (pages_[frame_id].IsDirty()) {
    FlushPage(page_id);
    pages_[frame_id].is_dirty_ = false;
  }
  pages_[frame_id].ResetMemory();
  // imitate
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
