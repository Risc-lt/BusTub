//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // Create the header page 
  auto header_page_guard{bpm->NewPageGuarded(&header_page_id_)};

  // Initialize the header page
  auto header_page{header_page_guard.AsMut<ExtendibleHTableHeaderPage>()};
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  // Get the hash value of the key
  auto hash{Hash(key)};

  // Get the header page guard and data
  auto header_page_guard{bpm_->FetchPageRead(header_page_id_)};
  auto header{header_page_guard.As<ExtendibleHTableHeaderPage>()};

  // Get the directory index
  auto directory_idx{header->HashToDirectoryIndex(hash)};
  page_id_t directory_page_id{static_cast<int32_t>(header->GetDirectoryPageId(directory_idx))};
  if (directory_page_id == INVALID_PAGE_ID) {
    // The corresponding page is not exists.
    return false;
  }

  // Get the directory page guard and data
  auto directory_page_guard{bpm_->FetchPageRead(directory_page_id)};
  auto directory{directory_page_guard.As<ExtendibleHTableDirectoryPage>()};

  // Get the bucket index
  auto bucket_idx{directory->HashToBucketIndex(hash)};
  page_id_t bucket_page_id{static_cast<int32_t>(directory->GetBucketPageId(bucket_idx))};
  if (bucket_page_id == INVALID_PAGE_ID) {
    // The corresponding page is not exists.
    return false;
  }

  // Get the bucket page guard and data
  auto bucket_page_guard{bpm_->FetchPageRead(bucket_page_id)};
  auto bucket_page{bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>()};

  // Lookup the key in the bucket page
  result->resize(1);
  if (bucket_page->Lookup(key, result->at(0), cmp_)) {
    return true;
  }

  // Return empty result and false if the key is not found
  result->resize(0);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
