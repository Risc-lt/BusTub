#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    // Just swap the pointers
    bpm_ = that.bpm_;
    page_ = that.page_;

    // Set the original pointers to nullptr
    that.bpm_ = nullptr;
    that.page_ = nullptr;
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
    bpm_ = that.bpm_;
    page_ = that.page_;

    // Set the original pointers to nullptr
    that.bpm_ = nullptr;
    that.page_ = nullptr;

    return *this;
}

BasicPageGuard::~BasicPageGuard(){
    // If the page is not nullptr, then unpin the page
    this->Drop();

    // Set the pointers to nullptr
    bpm_ = nullptr;
    page_ = nullptr;
};

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { return *this; }

void ReadPageGuard::Drop() {}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
