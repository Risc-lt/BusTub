//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  // Initialize the header page
  this->max_depth_ = max_depth;

  // Set the directory page ids to invalid page id
  for (int & directory_page_id : directory_page_ids_) {
    directory_page_id = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  // Get the directory index that the key is hashed to from the high bits of the hash
  return hash >> (32 - max_depth_);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  // Check if the directory index is valid
  if (directory_idx >= (1 << max_depth_)) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Directory index out of range");
  }

  // Return the directory page id at the index
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  // Check if the directory index is valid
  if (directory_idx >= (1 << max_depth_)) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Directory index out of range");
  }

  // Set the directory page id at the index
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t {
  // Return the maximum size of the directory page id array
  return 1 << max_depth_;
}

}  // namespace bustub
