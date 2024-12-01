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
#include <optional>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKNode::LRUKNode(size_t current_timestamp) {
  history_.push_front(current_timestamp);
  is_evictable_ = false;
}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  const size_t inf = std::numeric_limits<size_t>::max();
  std::unique_lock<std::mutex> lock(latch_);

  RecordCurrentTimestamp();
  // special case
  if (curr_size_ <= 0) {
    return std::nullopt;
  }
  // this element means (k-dis,last record timestrap,frame_id)
  std::tuple<size_t, size_t, size_t> evict_element = std::make_tuple(0, inf, 0);
  for (auto &iter : node_store_) {
    if (!iter.second.is_evictable_) {
      continue;
    }
    // calculate k-dis for all cases
    size_t diff = 0;
    if (iter.second.history_.size() < k_) {
      diff = inf;
    } else {
      diff = current_timestamp_ - iter.second.history_.back();
    }
    // deal with inf and multiple inf cases
    if (diff > std::get<0>(evict_element)) {
      evict_element = std::make_tuple(diff, iter.second.history_.back(), iter.first);
    } else if (diff == std::get<0>(evict_element)) {
      if (iter.second.history_.back() < static_cast<size_t>(std::get<1>(evict_element))) {
        evict_element = std::make_tuple(diff, iter.second.history_.back(), iter.first);
      }
    }
  }
  size_t fid = std::get<2>(evict_element);
  size_t diff = std::get<0>(evict_element);
  if (diff == 0) {
    return false;
  }
  node_store_.erase(fid);
  curr_size_ -= 1;

  return fid;
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
