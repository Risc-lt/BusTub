//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  // Set the max depth
  max_depth_ = max_depth;

  // Initialize the glocal depth and local depths
  global_depth_ = 0;

  for (unsigned char &local_depth : local_depths_) {
    local_depth = 0;
  }

  // Set the bucket page ids to invalid page id
  for (page_id_t &bucket_page_id : bucket_page_ids_) {
    bucket_page_id = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  // Get the number of bits to use for the bucket index
  uint32_t mask = (1U << global_depth_) - 1;

  // Get the bucket index
  return hash & mask;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  // Check if the bucket index is valid
  if (bucket_idx >= (1U << global_depth_)) {
    return INVALID_PAGE_ID;
  }

  // Get the bucket page id
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  // Check if the bucket index is valid
  if (bucket_idx >= (1U << global_depth_)) {
    return;
  }

  // Set the bucket page id
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  // Check if the bucket index is valid
  if (bucket_idx >= (1U << global_depth_)) {
    return INVALID_PAGE_ID;
  }

  // Get the local depth of the bucket
  uint8_t local_depth = local_depths_[bucket_idx];

  // Get the number of bits to use for the bucket index
  uint32_t mask = (1U << local_depth) - 1;

  // Get the split image index
  return (bucket_idx & mask) ^ (1U << (local_depths_[bucket_idx] - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // Check if the global depth is at the maximum
  if (global_depth_ == max_depth_) {
    return;
  }

  // Get the size of the directory
  uint32_t size = 1U << global_depth_;

  // Double the size of the directory
  for (uint32_t i = 0; i < size; i++) {
    auto new_index{i + size};
    bucket_page_ids_[new_index] = bucket_page_ids_[i];
    local_depths_[new_index] = local_depths_[i];
  }

  // Increment the global depth
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  // Check if the global depth is at the minimum
  if (global_depth_ == 0) {
    return;
  }

  // Decrement the global depth
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  // Check if all local depth is less than the global depth
  for (uint32_t i = 0; i < (1U << global_depth_); i++) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }

  // Return true if all bucket can be shrunk
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  // Get the size of the directory
  return 1U << global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  // Check if the bucket index is valid
  if (bucket_idx >= (1U << global_depth_)) {
    return 0;
  }

  // Get the local depth of the bucket
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  // Check if the bucket index is valid
  if (bucket_idx >= (1U << global_depth_)) {
    return;
  }

  // Set the local depth of the page in all bucket
  for (uint32_t i = 0; i < Size(); i++) {
    if (bucket_page_ids_[i] == bucket_page_ids_[bucket_idx]) {
      local_depths_[i] = local_depth;
    }
  }
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // Check if the bucket index is valid or the local depth is already at the global depth
  if (bucket_idx >= (1U << global_depth_) || local_depths_[bucket_idx] == global_depth_) {
    return;
  }

  // Increment the local depth of the bucket
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  // Check if the bucket index is valid or the local depth is already at the minimum
  if (bucket_idx >= (1U << global_depth_) || local_depths_[bucket_idx] == 0) {
    return;
  }

  // Decrement the local depth of the bucket
  local_depths_[bucket_idx]--;
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1U << max_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1U << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  // Get the number of bits to use for the bucket index
  uint32_t mask = (1U << local_depths_[bucket_idx]) - 1;

  // Get the local depth mask
  return mask;
}

}  // namespace bustub
