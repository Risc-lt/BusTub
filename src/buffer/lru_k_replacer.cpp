
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
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

size_t times{0};

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // Scan the node_store_ to find the frame_id to evict.
  size_t min_kth_time{std::numeric_limits<size_t>::max()};
  size_t min_time{std::numeric_limits<size_t>::max()};
  frame_id_t fid{-1};
  frame_id_t fkthid{-1};

  // Lock the latch
  std::lock_guard _{latch_};

  // If the replacer is empty, return false.
  if (curr_size_ == 0) {
    return false;
  }

  // Tranverse the node_store_ and maintain separate variables for two cases of frames.
  for (auto &[id, node] : node_store_) {
    // Get all infomation in one iteration.
    if (!node.is_evictable_) {
      continue;
    }

    // If the frame has been accessed more than k times, update the min_kth_time and fkthid.
    if (node.history_.size() >= k_) {
      if (min_kth_time >= node.history_.back()) {
        min_kth_time = node.history_.back();
        fkthid = id;
      }
    } else {
      // If the frame has been accessed less than k times, update the min_time and fid.
      if (min_time >= node.history_.back()) {
        min_time = node.history_.back();
        fid = id;
      }
    }
  }

  // If there exists a frame that has been accessed less than k times, evict the one with least recent timestamp.
  if (fid != -1) {
    *frame_id = fid;
  } else if (fkthid != -1) { // If all frames have been accessed more than k times, evict the one with least recent kth timestamp.
    *frame_id = fkthid;
  } else {
    return false; // If all frames are not evictable, return false.
  }

  // Remove the frame_id from the node_store_.
  node_store_.erase(*frame_id);

  // Decrease the current size.
  curr_size_--;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  // Throw an exception if the frame_id is invalid.
  if (frame_id < 0) {
    throw Exception(fmt::format("Unable to access frame {}", frame_id));
  }

  // Lock the latch.
  std::lock_guard _{latch_};

  // Detect if the frame_id is not in the node_store_.
  auto iter{node_store_.find(frame_id)};
  if (iter == node_store_.end()) {
    // If the replacer is full, throw an exception.
    if (node_store_.size() >= replacer_size_) {
      throw Exception(fmt::format("Unable to add frame {} as Replacer is full", frame_id));
    }

    // Insert the frame_id into the node_store_.
    auto result = node_store_.insert({frame_id, {k_, frame_id}});

    // Update the current size.
    result.first->second.UpdateAccessTime(current_timestamp_++);
  } else {
    // Update the access time of the frame_id.
    iter->second.UpdateAccessTime(current_timestamp_++);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // Lock the latch.
  std::lock_guard _{latch_};

  // Find the frame_id in the node_store_.
  auto iter{node_store_.find(frame_id)};

  // If the frame_id does not exist, throw an exception.
  if (iter == node_store_.end()) {
    throw Exception(fmt::format("Unable to change evictability on frame {} as it does not exists", frame_id));
  }

  // If the evictability is changed, update the current size.
  if (iter->second.is_evictable_ != set_evictable) {
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }

  // Update the evictability of the frame_id.
  iter->second.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // Lock the latch.
  std::lock_guard _{latch_};

  // Find the frame_id in the node_store_.
  auto iter{node_store_.find(frame_id)};

  // If the frame_id does not exist, return.
  if (iter == node_store_.end()) {
    return;
  }

  // If the frame_id is not evictable, throw an exception.
  if (!iter->second.is_evictable_) {
    throw Exception(fmt::format("Unable to remove frame {} as it does not evictable", frame_id));
  }

  // Remove the frame_id from the node_store_.
  node_store_.erase(iter);

  // Update the current size.
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard _{latch_};
  return curr_size_;
}

}  // namespace bustub
