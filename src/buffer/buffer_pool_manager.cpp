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

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

void BufferPoolManager::SetPage(frame_id_t frame_id, page_id_t page_id) {
  // Get the page
  Page *page = &pages_[frame_id];

  // Set the page_id, dirty flag, pin count, and data
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  // Lock the latch
  std::lock_guard<std::mutex> lock(latch_);

  // Decalre an empty frame_id
  frame_id_t empty_frame_id{0};

  // Firstly check if there is any free frame
  if (free_list_.empty()) {
    // If there is no free frame, we need to evict a page
    if (!replacer_->Evict(&empty_frame_id)) {
      return nullptr;
    }

    // Get the page_id of the page in the frame
    auto prev_page_id = pages_[empty_frame_id].GetPageId();
    page_table_.erase(prev_page_id);

    // If the page is dirty, we need to write it back to the disk
    if (pages_[empty_frame_id].IsDirty()) {
      // Wrap the page in a DiskRequest
      DiskRequest request{.is_write_ = true, .data_ = pages_[empty_frame_id].GetData(), .page_id_ = prev_page_id};

      // Create a promise to signal the completion of the request
      auto feedback{request.callback_.get_future()};
      // Schedule the request
      disk_scheduler_->Schedule(std::move(request));
      // Wait for the request to complete
      feedback.get();
    }
  } else {
    // If there is a free frame, we can use it
    empty_frame_id = free_list_.front();
    free_list_.pop_front();
  }

  // Allocate a new page_id
  page_id_t new_page_id = AllocatePage();
  page_table_[new_page_id] = empty_frame_id;

  // Pin the frame in replacer
  replacer_ -> RecordAccess(empty_frame_id);
  replacer_ -> SetEvictable(empty_frame_id, false);

  // Reset the memory and metadata for the new page
  SetPage(empty_frame_id, new_page_id);

  *page_id = new_page_id;
  return &pages_[empty_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // Lock the latch
  std::lock_guard<std::mutex> lock(latch_);

  // Check if the page is in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    // If the page is in the buffer pool, we can return it
    frame_id_t frame_id = page_table_[page_id];
    replacer_ -> RecordAccess(frame_id);
    replacer_ -> SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }      
    
  // If the page is not in the buffer pool, we need to fetch it from the disk
  frame_id_t empty_frame_id{0};

  // Firstly check if there is any free frame
  if (free_list_.empty()) {
    // If there is no free frame, we need to evict a page
    if (!replacer_->Evict(&empty_frame_id)) {
      return nullptr;
    }

    // Get the page_id of the page in the frame
    auto prev_page_id = pages_[empty_frame_id].GetPageId();
    page_table_.erase(prev_page_id);

    // If the page is dirty, we need to write it back to the disk
    if (pages_[empty_frame_id].IsDirty()) {
      // Wrap the page in a DiskRequest
      DiskRequest request{.is_write_ = true, .data_ = pages_[empty_frame_id].GetData(), .page_id_ = prev_page_id};

      // Create a promise to signal the completion of the request
      auto feedback{request.callback_.get_future()};
      // Schedule the request
      disk_scheduler_->Schedule(std::move(request));
      // Wait for the request to complete
      feedback.get();
    }
  } else {
    // If there is a free frame, we can use it
    empty_frame_id = free_list_.front();
    free_list_.pop_front();
  }

  // Read the page from the disk
  DiskRequest request{.is_write_ = false, .data_ = pages_[empty_frame_id].GetData(), .page_id_ = page_id};

  // Create a promise to signal the completion of the request
  auto feedback{request.callback_.get_future()};
  // Schedule the request
  disk_scheduler_->Schedule(std::move(request));
  // Wait for the request to complete
  feedback.get();

  // Update the metadata for the new page
  page_table_[page_id] = empty_frame_id;
  SetPage(empty_frame_id, page_id);

  // Pin the frame in replacer
  replacer_ -> RecordAccess(empty_frame_id);
  replacer_ -> SetEvictable(empty_frame_id, false);

  return &pages_[empty_frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // Lock the latch
  std::lock_guard<std::mutex> lock(latch_);

  // Check if the page is in the buffer pool
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  // Get the frame_id of the page
  frame_id_t frame_id = page_table_[page_id];

  // Check if the page is already unpinned
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }

  // Decrement the pin count
  pages_[frame_id].pin_count_--;

  // Set the dirty flag
  pages_[frame_id].is_dirty_ = is_dirty;

  // If the pin count reaches 0, the frame should be evictable by the replacer
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_ -> SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  // Lock the latch
  std::lock_guard<std::mutex> lock(latch_);

  // Check if the page is in the buffer pool
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  // Get the frame_id of the page
  frame_id_t frame_id = page_table_[page_id];

  // Flush a page to disk, regardless of the dirty flag.
  DiskRequest request{.is_write_ = true, .data_ = pages_[frame_id].GetData(), .page_id_ = page_id};

  // Create a promise to signal the completion of the request
  auto feedback{request.callback_.get_future()};
  // Schedule the request
  disk_scheduler_->Schedule(std::move(request));
  // Wait for the request to complete
  feedback.get();

  // Unset the dirty flag of the page after flushing
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
