//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) { return request_queue_.Put(std::move(r)); }

void DiskScheduler::StartWorkerThread() {
  while (true) {
    // Get a request from the queue
    std::optional r{request_queue_.Get()};

    // If the request is empty, exit the loop
    if (!r.has_value()) {
      return;
    }

    // Process the request
    if (r.value().is_write_) {
      disk_manager_->WritePage(r.value().page_id_, r.value().data_);
    }

    disk_manager_->ReadPage(r.value().page_id_, r.value().data_);

    // Signal the request issuer that the request has been completed
    r.value().callback_.set_value(true);
  }
}

}  // namespace bustub
