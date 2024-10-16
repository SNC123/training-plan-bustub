#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
// #define DEBUG(msg) \
//     if (DEBUG_FLAG) { std::cout << msg << std::endl; }

// const bool DEBUG_FLAG = true;

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  // DEBUG("into BasicPageGuard move constructor")
  // set old to null
  that.Clear();
}

void BasicPageGuard::Drop() {
  // DEBUG("into BasicPageGuard Drop")
  // avoid double free
  if (bpm_ == nullptr || page_ == nullptr) {
    return;
  }
  // unpin and gc
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  Clear();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // DEBUG("into BasicPageGuard move assignment")
  // when that=this, do nothing
  if (this != &that) {
    Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    // set old to null
    that.Clear();
  }

  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  // DEBUG("into BasicPageGuard destructor")
  Drop();
};  // NOLINT

auto BasicPageGuard::Clear() -> void {
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // DEBUG("into UpgradeRead")
  page_->RLatch();
  auto read_guard = ReadPageGuard(bpm_, page_);
  Clear();
  return read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  // DEBUG("into UpgradeWrite")
  page_->WLatch();
  auto write_guard = WritePageGuard(bpm_, page_);
  Clear();
  return write_guard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // DEBUG("into ReadPageGuard move constructor")
  guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // DEBUG("into ReadPageGuard move assignment")
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  // DEBUG("into BasicPageGuard destructor")
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  // // DEBUG("into ReadPageGuard desturctor")
  Drop();
  // std::cout<<"remove ReadPageGuard"<<std::endl;
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); };

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
