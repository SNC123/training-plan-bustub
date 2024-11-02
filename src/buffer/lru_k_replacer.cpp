//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <chrono>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKNode::LRUKNode(size_t current_timestamp) {
  history_.push_front(current_timestamp);
  is_evictable_ = false;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  const size_t inf = std::numeric_limits<size_t>::max();
  std::unique_lock<std::mutex> lock(latch_);

  RecordCurrentTimestamp();
  // special case
  if (curr_size_ <= 0) {
    *frame_id = -1;
    return false;
  }
  // this element means (k-dis,last record timestrap,frame_id)
  size_t evict_k_dis = 0;
  size_t evict_last_timestamp = inf;
  size_t evict_frame_id = 0;
  // std::tuple<size_t, size_t, size_t> evict_element = std::make_tuple(0, inf, 0);
  for (auto &iter : node_store_) {
    if (!iter.second.is_evictable_) {
      continue;
    }
    // calculate k-dis for all cases
    size_t diff = 0;
    size_t oldest_timestamp = iter.second.history_.back();
    if (iter.second.history_.size() < k_) {
      diff = inf;
    } else {
      diff = current_timestamp_ - oldest_timestamp;
    }
    // deal with inf and multiple inf cases
    if (diff > evict_k_dis) {
      // evict_element = std::make_tuple(diff, oldest_timestamp, iter.first);
      evict_k_dis = diff;
      evict_last_timestamp = oldest_timestamp;
      evict_frame_id = iter.first;
    } else if (diff == evict_k_dis) {
      if (oldest_timestamp < evict_last_timestamp) {
        // evict_element = std::make_tuple(diff, oldest_timestamp, iter.first);
        evict_k_dis = diff;
        evict_last_timestamp = oldest_timestamp;
        evict_frame_id = iter.first;
      }
    }
  }
  // size_t fid = std::get<2>(evict_element);
  // size_t diff = std::get<0>(evict_element);
  if (evict_k_dis == 0) {
    return false;
  }
  *frame_id = evict_frame_id;
  node_store_.erase(evict_frame_id);
  curr_size_ -= 1;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> lock(latch_);
  // record timestamp
  RecordCurrentTimestamp();
  // exist then update, else insert
  if (node_store_.find(frame_id) != node_store_.end()) {
    node_store_[frame_id].history_.push_front(current_timestamp_);
    if (node_store_[frame_id].history_.size() > k_) {
      node_store_[frame_id].history_.pop_back();
    }
  } else {
    node_store_[frame_id] = LRUKNode(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  // exist then update, else do nothing
  if (node_store_.find(frame_id) != node_store_.end()) {
    curr_size_ += static_cast<size_t>(set_evictable);
    curr_size_ -= static_cast<size_t>(node_store_[frame_id].is_evictable_);
    node_store_[frame_id].is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);
  // make sure existing
  if (node_store_.find(frame_id) != node_store_.end()) {
    node_store_.erase(frame_id);
    curr_size_ -= 1;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::RecordCurrentTimestamp() { current_timestamp_++; }

}  // namespace bustub
