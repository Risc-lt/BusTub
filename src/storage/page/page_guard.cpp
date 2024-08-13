#include "storage/page/page_guard.h"
#include <list>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // Just swap the pointers
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  // Set the pointers to nullptr
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  // If the page is not nullptr, then unpin the page
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }

  // Set the pointers to nullptr
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // If the pointers are the same, then return
  if (this == &that) {
    return *this;
  }

  // If the page is not nullptr, then unpin the page
  if (bpm_ != nullptr) {
    this->Drop();
  }

  // Swap the new pointers with the old pointers
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  // Set the pointers to nullptr
  that.bpm_ = nullptr;
  that.page_ = nullptr;

  return *this;
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // Check if the page is nullptr or bpm is nullptr
  if (page_ == nullptr || bpm_ == nullptr) {
    return ReadPageGuard{};
  }

  // Lock the read latch
  page_->RLatch();

  // Construct a ReadPageGuard
  ReadPageGuard read_guard{this->bpm_, bpm_->FetchPage(page_->GetPageId())};

  // Set the pointers to nullptr
  this->Drop();

  return read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  // Check if the page is nullptr or bpm is nullptr
  if (page_ == nullptr || bpm_ == nullptr) {
    return WritePageGuard{};
  }

  // Lock the write latch
  page_->WLatch();

  // Construct a WritePageGuard
  WritePageGuard write_guard{this->bpm_, bpm_->FetchPage(page_->GetPageId())};

  // Set the pointers to nullptr
  this->Drop();

  return write_guard;
}

BasicPageGuard::~BasicPageGuard() {
  // If the page is not nullptr, then unpin the page
  this->Drop();
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // Just swap the pointers
  guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // If the pointers are the same, then return
  if (this == &that) {
    return *this;
  }

  // If the page is not nullptr, then unpin the page
  guard_.Drop();

  // Swap the new pointers with the old pointers
  guard_ = std::move(that.guard_);

  return *this;
}

void ReadPageGuard::Drop() {
  // If the page is not nullptr
  if (guard_.page_ == nullptr) {
    return;
  }

  // Unlatch the page
  guard_.page_->RUnlatch();

  // Unpin the page
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  // If the page is not nullptr, then unpin the page
  this->Drop();
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  // Just swap the pointers
  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  // If the pointers are the same, then return
  if (this == &that) {
    return *this;
  }

  // If the page is not nullptr, then unpin the page
  guard_.Drop();

  // Swap the new pointers with the old pointers
  guard_ = std::move(that.guard_);

  return *this;
}

void WritePageGuard::Drop() {
  // If the page is nullptr
  if (guard_.page_ != nullptr) {
    return;
  }

  // Unlatch the page
  guard_.page_->WUnlatch();

  // Unpin the page
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  // If the page is not nullptr, then unpin the page
  this->Drop();
}

}  // namespace bustub
