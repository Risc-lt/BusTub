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
  // Get the hash value of the key
  auto hash{Hash(key)};

  // Check if the key exists
  std::vector<V> result;
  if (GetValue(key, &result, transaction)) {
    return false;
  }

  // Get the header page guard and data
  auto header_page_guard{bpm_->FetchPageWrite(header_page_id_)};
  auto header{header_page_guard.AsMut<ExtendibleHTableHeaderPage>()};

  // Check if the header page is null
  if (header == nullptr) {
    throw Exception("Fetch header failed");
  }

  // Get the directory index
  auto directory_idx{header->HashToDirectoryIndex(hash)};

  // Get the directory page id
  page_id_t directory_page_id{static_cast<int32_t>(header->GetDirectoryPageId(directory_idx))};
  if (directory_page_id == INVALID_PAGE_ID) {
    // The corresponding page is not exists.
    return InsertToNewDirectory(header, directory_idx, hash, key, value);
  }

  // Get the directory page guard and data
  auto directory_page_guard{bpm_->FetchPageWrite(directory_page_id)};
  auto directory{directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>()};

  // Drop the header page guard
  header_page_guard.Drop();

  // Get the bucket index
  auto bucket_idx{directory->HashToBucketIndex(hash)};

  // Get the bucket page id
  page_id_t bucket_page_id{static_cast<int32_t>(directory->GetBucketPageId(bucket_idx))};

  // Get the bucket page guard and data
  auto bucket_page_guard{bpm_->FetchPageWrite(bucket_page_id)};
  auto bucket{bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()};
  if (bucket == nullptr) {
    throw Exception("Bucket page is null");
  }

  /*
   * The bucket page exists, but it may be full. We need to check if the bucket page is full.
   * If the bucket page is full, we need to split the bucket page.
   */

  if (!bucket->IsFull()) {
    // The bucket page is not full.
    // Insert the key-value pair to the bucket page.
    return bucket->Insert(key, value, cmp_);
  }

  // The bucket is full. Attempt to split.
  if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
    // Try to grow directory and insert value.
    if (directory->GetGlobalDepth() == directory->GetMaxDepth()) {
      return false;
    }
    directory->IncrGlobalDepth();
  }

  // Create a new bucket page
  page_id_t new_bucket_page_id;
  auto new_bucket_page_guard{bpm_->NewPageGuarded(&new_bucket_page_id)};
  auto new_bucket{new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()};

  // Check if the new bucket page is created successfully
  if (new_bucket == nullptr) {
    throw Exception(fmt::format("Create bucket failed, page_id:{}", new_bucket_page_id));
  }

  // Initialize the new bucket page
  new_bucket->Init(bucket_max_size_);

  // Increase local depth first. GetSplitImageIndex(idx) returns the new created bucket index.
  directory->IncrLocalDepth(bucket_idx);
  auto new_bucket_idx{directory->GetSplitImageIndex(bucket_idx)};

  // Update the directory mapping
  directory->SetLocalDepth(new_bucket_idx, directory->GetLocalDepth(bucket_idx));
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

  // Migrate entries from the old bucket to the new bucket
  MigrateEntries(bucket, new_bucket, new_bucket_idx, directory->GetLocalDepthMask(bucket_idx));
  auto inserted_bucket{directory->HashToBucketIndex(hash)};
  if (inserted_bucket == new_bucket_idx) {
    if (new_bucket->IsFull()) {
      throw Exception("Bad hash func causes too many collision.");
    }
    new_bucket->Insert(key, value, cmp_);
  } else if (inserted_bucket == bucket_idx) {
    if (bucket->IsFull()) {
      throw Exception("Bad hash func causes too many collision.");
    }
    bucket->Insert(key, value, cmp_);
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  // Create a new directory page
  page_id_t directory_page_id;
  auto g{bpm_->NewPageGuarded(&directory_page_id)};

  // Set the directory page id in the header page
  header->SetDirectoryPageId(directory_idx, directory_page_id);

  // Get the directory page guard and data
  auto directory_page_guard{g.UpgradeWrite()};
  auto directory{directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>()};
  if (directory == nullptr) {
    throw Exception("Create directory failed");
  }

  // Initialize the directory page
  directory->Init(directory_max_depth_);
  auto bucket_idx{directory->HashToBucketIndex(hash)};

  // Insert the key-value pair to the new bucket page
  return InsertToNewBucket(directory, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  // Create a new bucket page
  page_id_t bucket_page_id;
  auto g{bpm_->NewPageGuarded(&bucket_page_id)};

  // Set the bucket page id in the directory page
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  if (directory == nullptr) {
    throw Exception("Create directory failed");
  }

  // Get the bucket page guard and data
  auto bucket_page_guard{g.UpgradeWrite()};
  auto bucket{bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()};
  if (bucket == nullptr) {
    throw Exception("Fetch bucket failed");
  }

  // Initialize the bucket page
  bucket->Init(bucket_max_size_);

  // Insert the key-value pair to the bucket page
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  // Iterate through the old bucket and migrate entries to the new bucket
  for (uint32_t i = 0; i < old_bucket->Size();) {
    auto key{old_bucket->KeyAt(i)};
    auto value{old_bucket->ValueAt(i)};
    auto idx{local_depth_mask & Hash(key)};

    // Migrate the entry to the new bucket if the hash value is in the new bucket
    if (idx == new_bucket_idx) {
      new_bucket->Insert(key, value, cmp_);
      old_bucket->RemoveAt(i);
      continue;
    }
    i++;
  }
}

/*****************************************************************************
 * UNUSED
 *****************************************************************************/
// template <typename K, typename V, typename KC>
// void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
//                                                                uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
//                                                                uint32_t new_local_depth, uint32_t local_depth_mask) {
//   throw NotImplementedException("DiskExtendibleHashTable is not implemented");
// }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // Get the hash value of the key
  auto hash{Hash(key)};

  // Get the header page guard and data
  auto header_page_guard{bpm_->FetchPageWrite(header_page_id_)};
  auto header{header_page_guard.AsMut<ExtendibleHTableHeaderPage>()};
  if (header == nullptr) {
    throw Exception("Fetch header failed");
  }

  // Get the directory index
  auto directory_idx{header->HashToDirectoryIndex(hash)};
  page_id_t directory_page_id{static_cast<int32_t>(header->GetDirectoryPageId(directory_idx))};
  if (directory_page_id == INVALID_PAGE_ID) {
    // The corresponding page is not exists.
    return false;
  }

  // Get the directory page guard and data and drop the header page guard
  auto directory_page_guard{bpm_->FetchPageWrite(directory_page_id)};
  header_page_guard.Drop();
  auto directory{directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>()};
  if (directory == nullptr) {
    throw Exception("Fetch directory failed");
  }

  // Get the bucket index
  auto bucket_idx{directory->HashToBucketIndex(hash)};
  page_id_t bucket_page_id{static_cast<int32_t>(directory->GetBucketPageId(bucket_idx))};
  if (bucket_page_id == INVALID_PAGE_ID) {
    // The corresponding page is not exists.
    return false;
  }

  // Get the bucket page guard and data
  auto bucket_page_guard{bpm_->FetchPageWrite(bucket_page_id)};
  auto bucket{bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()};

  // Throw exception if the bucket page is null
  if (bucket == nullptr) {
    throw Exception("Fetch bucket failed");
  }

  // Remove the key from the bucket page
  if (!bucket->Remove(key, cmp_)) {
    return false;
  }

  // Merge the bucket page if it is empty
  if (bucket->IsEmpty()) {
    Merge(directory, bucket_idx, bucket_page_guard);
    while (directory->CanShrink()) {
      directory->DecrGlobalDepth();
    }
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Merge(ExtendibleHTableDirectoryPage *directory, uint32_t empty_idx,
                                              WritePageGuard &empty_bucket_guard) -> std::pair<uint32_t, uint32_t> {
  // If the empty bucket is the only bucket in the directory, return the empty bucket page id and local depth
  if (directory->GetLocalDepth(empty_idx) == 0) {
    return {directory->GetBucketPageId(empty_idx), 0};
  }

  // Get the split index
  uint32_t split_idx{directory->GetSplitImageIndex(empty_idx)};
  if (directory->GetLocalDepth(split_idx) != directory->GetLocalDepth(empty_idx)) {
    return {directory->GetBucketPageId(empty_idx), directory->GetLocalDepth(empty_idx)};
  }

  // Get the split bucket page id
  directory->SetBucketPageId(empty_idx, directory->GetBucketPageId(split_idx));

  // Decrement the local depth of the empty and split bucket
  directory->DecrLocalDepth(empty_idx);
  directory->DecrLocalDepth(split_idx);

  // Return the empty bucket page id and local depth if the split bucket is empty
  auto another_idx{directory->GetLocalDepth(empty_idx)};
  if (directory->GetLocalDepth(another_idx) != directory->GetLocalDepth(empty_idx)) {
    return {directory->GetBucketPageId(empty_idx), directory->GetLocalDepth(empty_idx)};
  }

  // Get another bucket page id that has the same local depth as the empty bucket
  page_id_t another_page_id{directory->GetBucketPageId(another_idx)};

  // Get the another bucket page guard and data
  auto another_page_guard{bpm_->FetchPageWrite(another_page_id)};
  auto another{another_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()};
  if (another == nullptr) {
    throw Exception("Copy bucket failed");
  }

  // Merge the empty bucket and another bucket
  if (another->IsEmpty()) {
    auto [page_id, depth]{Merge(directory, another_idx, another_page_guard)};
    directory->SetBucketPageId(empty_idx, page_id);
    directory->SetBucketPageId(split_idx, page_id);
    directory->SetLocalDepth(empty_idx, depth);
    directory->SetLocalDepth(split_idx, depth);
  }

  // Return the empty bucket page id and local depth
  return {directory->GetBucketPageId(empty_idx), directory->GetLocalDepth(empty_idx)};
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
