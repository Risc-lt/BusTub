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
#include <mutex>
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
    if(replacer_size_ == 0) {
        latch_.unlock();
        return false;
    }

    // Find the frame with the maximum backward k-distance
    // If there exists +inf, return it
    if(less_than_k_head_->next_ != less_than_k_tail_) {
        // Get the frame id
        *frame_id = less_than_k_head_->next_->fid_;

        // Remove the frame from the list
        less_than_k_head_->next_->next_->prev_ = less_than_k_head_;
        less_than_k_head_->next_ = less_than_k_head_->next_->next_;

        // Decrement the size of the replacer
        replacer_size_--;

        // Unlock the latch
        latch_.unlock();
        return true;
    }

    // If there is no +inf, return the frame with the maximum backward k-distance
    if(!greater_than_k_set_.empty()) {
        // Get the frame id
        *frame_id = (*greater_than_k_set_.begin())->fid_;

        // Remove the frame from the set
        greater_than_k_set_.erase(greater_than_k_set_.begin());

        // Decrement the size of the replacer
        replacer_size_--;

        // Unlock the latch
        latch_.unlock();
        return true;
    }

    // Unlock the latch
    latch_.unlock();
    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
