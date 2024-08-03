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
#include <algorithm>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // Check if k and num_frames are valid
  if (k_ < 1) {
    throw Exception(ExceptionType::INVALID, "k must be greater than 0");
  }

  if (replacer_size_ < 0) {
    throw Exception(ExceptionType::INVALID, "replacer size must be greater than 0");
  }

  // Initialize the head and tail nodes
  less_than_k_head_ = std::make_shared<LRUKNode>();
  less_than_k_tail_ = std::make_shared<LRUKNode>();

  // Set the pointers of the head and tail nodes
  less_than_k_head_->next_ = less_than_k_tail_;
  less_than_k_tail_->prev_ = less_than_k_head_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // Lock the latch
  std::lock_guard<std::mutex> lock(latch_);

  // Check if the replacer is empty
  if (curr_size_ == 0) {
    return false;
  }

  // Find the frame with the maximum backward k-distance
  // If there exists +inf, return it
  if (less_than_k_head_->next_ != less_than_k_tail_) {
    for (auto it = less_than_k_head_->next_; it != less_than_k_tail_; it = it->next_) {
      if (it->is_evictable_) {
        // Get the frame id
        *frame_id = it->fid_;

        // Remove the frame from the list
        it->next_->prev_ = it->prev_;
        it->prev_->next_ = it->next_;

        // Decrease the size of the replacer
        curr_size_--;
        return true;
      }
    }
  }

  // If there is no +inf, return the frame with the maximum backward k-distance
  if (!greater_than_k_set_.empty()) {
    for (auto it = greater_than_k_set_.begin(); it != greater_than_k_set_.end(); it++) {
      if ((*it)->is_evictable_) {
        // Get the frame id
        *frame_id = (*it)->fid_;

        // Remove the frame from the set
        greater_than_k_set_.erase(it);

        // Decrease the size of the replacer
        curr_size_--;
        return true;
      }
    }
  }

  return false;
}

auto LRUKReplacer::CheckExist(frame_id_t frame_id) -> bool {
  // Lock the latch
  // std::lock_guard<std::mutex> lock(latch_);

  // Check if the frame is in the replacer
  for (auto it = less_than_k_head_->next_; it != less_than_k_tail_; it = it->next_) {
    if (it->fid_ == frame_id) {
      return true;
    }
  }

  // Check if the frame is in the set
  return std::any_of(greater_than_k_set_.begin(), greater_than_k_set_.end(),
                     [frame_id](auto node) { return node->fid_ == frame_id; });
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  // Lock the latch
  std::lock_guard<std::mutex> lock(latch_);

  // Abort if frame_id is invalid
  if (frame_id < 0 || frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw Exception(ExceptionType::INVALID, "Frame id must be within 0 and replacer_size");
  }

  // Check if the frame is in the replacer
  if (!CheckExist(frame_id)) {
    // Create a new node
    auto new_frame = std::make_shared<LRUKNode>();
    new_frame->fid_ = frame_id;
    new_frame->history_.push_back(current_timestamp_);

    current_timestamp_++;

    // Add the frame to the linked list
    new_frame->prev_ = less_than_k_tail_->prev_;
    new_frame->next_ = less_than_k_tail_;
    less_than_k_tail_->prev_->next_ = new_frame;
    less_than_k_tail_->prev_ = new_frame;

    // Increase the current size (initial state is inevictable, so no need to increase)
    // curr_size_++;

    return;
  }

  // Get the node from the linked list
  for (auto it = less_than_k_head_->next_; it != less_than_k_tail_; it = it->next_) {
    if (it->fid_ == frame_id) {
      // Update the history of the frame
      it->history_.push_back(current_timestamp_);
      current_timestamp_++;

      // Check if the history of the frame is less than k
      if (it->history_.size() >= k_) {
        // Remove the frame from the linked list
        it->next_->prev_ = it->prev_;
        it->prev_->next_ = it->next_;

        // Set the old pointer to nullptr
        it->prev_ = nullptr;
        it->next_ = nullptr;

        // Insert the frame to the set
        greater_than_k_set_.insert(std::move(it));
      } else {
        // Move the frame to the end of the linked list
        it->next_->prev_ = it->prev_;
        it->prev_->next_ = it->next_;

        it->prev_ = less_than_k_tail_->prev_;
        it->next_ = less_than_k_tail_;
        less_than_k_tail_->prev_->next_ = it;
        less_than_k_tail_->prev_ = it;
      }

      return;
    }
  }

  // Get the node from the set
  for (const auto &it : greater_than_k_set_) {
    if (it->fid_ == frame_id) {
      // Copy the target frame
      auto new_frame = std::make_shared<LRUKNode>();
      new_frame->fid_ = frame_id;
      new_frame->history_ = it->history_;
      new_frame->history_.push_back(current_timestamp_);

      // Remove the old frame from the set
      greater_than_k_set_.erase(it);

      // Insert the frame to the set
      greater_than_k_set_.insert(std::move(new_frame));

      // Update the timestamp
      current_timestamp_++;

      return;
    }
  }

  // If the frame is not found, throw an exception
  throw Exception(ExceptionType::INVALID, "Frame not found");
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // Lock the latch
  std::lock_guard<std::mutex> lock(latch_);

  // Check if the frame is in the replacer
  if (!CheckExist(frame_id)) {
    throw Exception(ExceptionType::INVALID, "Frame not found");
  }

  // Get the node from the linked list
  for (auto it = less_than_k_head_->next_; it != less_than_k_tail_; it = it->next_) {
    if (it->fid_ == frame_id) {
      // Check if the frame is evictable
      if (set_evictable) {
        // Increase the size of the replacer if the pre statue is not evictable
        if (!it->is_evictable_) {
          curr_size_++;
        }
        it->is_evictable_ = true;
      } else {
        // Check if the frame is evictable
        if (it->is_evictable_) {
          // Decrease the size of the replacer
          curr_size_--;
        }
        it->is_evictable_ = false;
      }

      return;
    }
  }

  // Get the node from the set
  for (const auto &it : greater_than_k_set_) {
    if (it->fid_ == frame_id) {
      // Check if the frame is evictable
      if (set_evictable) {
        // Increase the size of the replacer if the pre statue is not evictable
        if (!it->is_evictable_) {
          curr_size_++;
        }
        it->is_evictable_ = true;

      } else {
        // Check if the frame is evictable
        if (it->is_evictable_) {
          // Decrease the size of the replacer
          curr_size_--;
        }
        it->is_evictable_ = false;
      }

      return;
    }
  }

  // If the frame is not found, throw an exception
  throw Exception(ExceptionType::INVALID, "Frame not found");
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // Lock the latch
  std::lock_guard<std::mutex> lock(latch_);

  // Check if the frame is in the replacer
  if (!CheckExist(frame_id)) {
    return;
  }

  // Get the node from the linked list
  for (auto it = less_than_k_head_->next_; it != less_than_k_tail_; it = it->next_) {
    if (it->fid_ == frame_id && it->is_evictable_) {
      // Remove the frame from the linked list
      it->next_->prev_ = it->prev_;
      it->prev_->next_ = it->next_;

      // Decrease the size of the replacer
      curr_size_--;

      return;
    }
  }

  // Get the node from the set
  for (const auto &it : greater_than_k_set_) {
    if (it->fid_ == frame_id && it->is_evictable_) {
      // Remove the frame from the set
      greater_than_k_set_.erase(it);

      // Decrease the size of the replacer
      curr_size_--;

      return;
    }
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);

  return curr_size_;
}

}  // namespace bustub
