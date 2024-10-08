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
      auto history_iter = iter.second.history_.begin();
      std::advance(history_iter, k_ - 1);
      diff = current_timestamp_ - *history_iter;
    }
    std::cout << iter.first << "<-frame_id diff->" << diff << std::endl;
    // todo solve the bug
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
  if (fid == 0) {
    return false;
  }
  *frame_id = fid;
  node_store_.erase(fid);
  curr_size_ -= 1;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> lock(latch_);
  // record timestamp
  RecordCurrentTimestamp();
  // filter out invalid frame_id
  // BUSTUB_ENSURE(frame_id <= static_cast<int>(replacer_size_) && frame_id >= 1, "frame id out of bound");
  std::cout << current_timestamp_ << "<- timestamp frame_id ->" << frame_id << std::endl;
  // exist then update, else insert
  if (node_store_.find(frame_id) != node_store_.end()) {
    node_store_[frame_id].history_.push_front(current_timestamp_);
  } else {
    node_store_[frame_id] = LRUKNode(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  // filter out invalid frame_id
  // BUSTUB_ENSURE(frame_id <= static_cast<int>(replacer_size_) && frame_id >= 1, "frame id out of bound");
  // exist then update, else do nothing
  if (node_store_.find(frame_id) != node_store_.end()) {
    curr_size_ += static_cast<size_t>(set_evictable);
    curr_size_ -= static_cast<size_t>(node_store_[frame_id].is_evictable_);
    node_store_[frame_id].is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);
  // filter out invalid frame_id
  // BUSTUB_ENSURE(frame_id <= static_cast<int>(replacer_size_) && frame_id >= 1, "frame id out of bound");
  // make sure existing
  if (node_store_.find(frame_id) != node_store_.end()) {
    // filter out non-evictable frame_id
    // BUSTUB_ENSURE(node_store_[frame_id].is_evictable_,
    //               ("frame " + std::to_string(frame_id) + " can't be evcited").c_str());
    node_store_.erase(frame_id);
    curr_size_ -= 1;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::RecordCurrentTimestamp() {
  auto now = std::chrono::system_clock::now();
  auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
  current_timestamp_ = static_cast<size_t>(timestamp);
}

}  // namespace bustub
