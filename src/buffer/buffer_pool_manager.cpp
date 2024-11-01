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
#include <cstddef>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  pages_latches_ = new std::mutex[pool_size_];

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete[] pages_latches_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "try to new page " << std::endl;

  std::unique_lock<std::mutex> free_list_lock(free_list_latch_);
  std::unique_lock<std::shared_mutex> page_table_lock(page_table_latch_);

  frame_id_t frame_id = 0;
  auto replacer_size = replacer_->Size();
  // check available position in free_list
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    // check available position in replacer
  } else if (replacer_size > 0) {
    replacer_->Evict(&frame_id);
  } else {
    // there is no available position, so return nullptr
    return nullptr;
  }
  std::unique_lock<std::mutex> page_lock(pages_latches_[frame_id]);

  if (!free_list_.empty()) {
    free_list_.pop_front();
  } else if (replacer_size > 0) {
    auto old_page_id = pages_[frame_id].page_id_;
    // write dirty page to disk and reset page
    if (pages_[frame_id].IsDirty()) {
      FlushPage(old_page_id);
      pages_[frame_id].is_dirty_ = false;
    }
    // remove old page id
    page_table_.erase(old_page_id);
  }
  // std::unique_lock<std::shared_mutex> page_table_lock(page_table_latch_);
  // std::unique_lock<std::mutex> page_lock(pages_latches_[frame_id]);
  free_list_lock.unlock();
  // allocate new page and maintain page_id to frame_id
  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;
  // free_list_lock.unlock();
  page_table_lock.unlock();
  // set metadata
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "try to fetch page " << page_id << std::endl;

  // search in the buffer pool
  std::unique_lock<std::mutex> free_list_lock(free_list_latch_);
  std::shared_lock<std::shared_mutex> page_table_lock(page_table_latch_);
  // frame id for storage requested page
  frame_id_t frame_id = 0;
  auto replacer_size = replacer_->Size();
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
  } else if (!free_list_.empty()) {
    frame_id = free_list_.front();
  } else if (replacer_size > 0) {
    replacer_->Evict(&frame_id);
  }
  std::unique_lock<std::mutex> page_lock(pages_latches_[frame_id]);

  if (page_table_.find(page_id) != page_table_.end()) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  page_table_lock.unlock();
  std::unique_lock<std::shared_mutex> page_table_lock_write(page_table_latch_);

  if (!free_list_.empty()) {
    free_list_.pop_front();
  } else if (replacer_size > 0) {
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
  // std::unique_lock<std::shared_mutex> page_table_lock(page_table_latch_);
  // std::unique_lock<std::mutex> page_lock(pages_latches_[frame_id]);
  free_list_lock.unlock();

  page_table_[page_id] = frame_id;
  // free_list_lock.unlock();
  page_table_lock_write.unlock();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_++;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // fetch page from disk
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  // send read request
  disk_scheduler_->Schedule({false, pages_[frame_id].data_, page_id, std::move(promise)});
  if (future.get()) {
    // std::cout << "read page " << page_id << " successfully" << std::endl;
  }
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "try to unpin page " << page_id << std::endl;
  std::unique_lock<std::mutex> free_list_lock(free_list_latch_);
  std::shared_lock<std::shared_mutex> page_table_lock(page_table_latch_);
  free_list_lock.unlock();
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  std::unique_lock<std::mutex> page_lock(pages_latches_[frame_id]);
  page_table_lock.unlock();
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
  // std::cout << "try to flush page " << page_id << std::endl;
  // std::unique_lock<std::shared_mutex> page_table_lock(page_table_latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  // std::unique_lock<std::mutex> page_lock(pages_latches_[frame_id]);
  // page_table_lock.unlock();

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  // tell scheduler to write dirty page to disk
  disk_scheduler_->Schedule({true, pages_[frame_id].data_, page_id, std::move(promise)});
  // wait until worker thread set value
  if (future.get()) {
    // std::cout << "flush page " << page_id << "("
    //           << "frame " << frame_id << ")"
    //           << " successfully" << std::endl;
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
      // std::cout << "flush page " << page.first << " successfully" << std::endl;
      pages_[page.second].is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // std::lock_guard<std::mutex> lock(latch_);

  // std::cout << "try to delete page " << page_id << std::endl;
  std::unique_lock<std::mutex> free_list_lock(free_list_latch_);
  std::shared_lock<std::shared_mutex> page_table_lock(page_table_latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  page_table_lock.unlock();
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ >= 1) {
    return false;
  }
  // insert frame into free_list
  free_list_.push_back(frame_id);
  // std::unique_lock<std::shared_mutex> page_table_lock(page_table_latch_);
  std::unique_lock<std::mutex> page_lock(pages_latches_[frame_id]);
  free_list_lock.unlock();
  std::unique_lock<std::shared_mutex> page_table_lock_write(page_table_latch_);
  // delete page id in page table
  page_table_.erase(page_id);
  page_table_lock_write.unlock();
  // remove frame in replacer
  replacer_->Remove(frame_id);
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

auto BufferPoolManager::AllocatePage() -> page_id_t {
  // std::unique_lock<std::mutex> lock(next_page_id_latch_);
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto basic_guard = BasicPageGuard(this, FetchPage(page_id));
  return basic_guard.UpgradeRead();
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto basic_guard = BasicPageGuard(this, FetchPage(page_id));
  return basic_guard.UpgradeWrite();
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
