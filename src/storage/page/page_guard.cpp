#include "storage/page/page_guard.h"
#include <list>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // Just swap the pointers
  std::swap(bpm_, that.bpm_);
  std::swap(page_, that.page_);
  std::swap(is_dirty_, that.is_dirty_);
}

void BasicPageGuard::Drop() {
  // If the page is not nullptr, then unpin the page
  if (page_ != nullptr) {
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
  this->Drop();

  // Swap the new pointers with the old pointers
  std::swap(bpm_, that.bpm_);
  std::swap(page_, that.page_);
  std::swap(is_dirty_, that.is_dirty_);

  return *this;
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // Lock the read latch
  page_->RLatch();

  // Construct a ReadPageGuard
  ReadPageGuard read_guard{this->bpm_, this->page_};

  // Set the pointers to nullptr
  this->bpm_ = nullptr;
  this->page_ = nullptr;

  return read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  // Lock the write latch
  page_->WLatch();

  // Construct a WritePageGuard
  WritePageGuard write_guard{this->bpm_, this->page_};

  // Set the pointers to nullptr
  this->bpm_ = nullptr;
  this->page_ = nullptr;

  return write_guard;
}

BasicPageGuard::~BasicPageGuard() {
  // If the page is not nullptr, then unpin the page
  this->Drop();
};

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
  if (guard_.page_ != nullptr) {
    // Unlatch the page
    guard_.page_->RUnlatch();

    // Unpin the page
    guard_.Drop();
  }

  // Set the pointers to nullptr
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
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
  // If the page is not nullptr
  if (guard_.page_ != nullptr) {
    // Unlatch the page
    guard_.page_->WUnlatch();

    // Unpin the page
    guard_.Drop();
  }

  // Set the pointers to nullptr
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

WritePageGuard::~WritePageGuard() {
  // If the page is not nullptr, then unpin the page
  this->Drop();
}

}  // namespace bustub
