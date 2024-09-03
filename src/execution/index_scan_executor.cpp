//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // Get the table info
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);

  // Get the table indexes
  auto indexes = catalog->GetTableIndexes(table_info_->name_);

  // Find the target index and convert it to HashTableIndexForTwoIntegerColumn
  for (auto index : indexes) {
    if (index->index_oid_ == plan_->index_oid_) {
      htable_index_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index->index_.get());
      break;
    }
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Check if the table info and index are valid
  if (table_info_ == nullptr || htable_index_ == nullptr) {
    return false;
  }

  // Construct the key
  std::vector<Value> value{plan_->pred_key_->val_};
  Tuple key{value, htable_index_->GetKeySchema()};

  // Scan the index to get the RIDs
  std::vector<RID> rids;
  htable_index_->ScanKey(key, &rids, exec_ctx_->GetTransaction());
  if (rids.empty()) {
    return false;
  }

  // Check if the RIDs are duplicated
  BUSTUB_ASSERT(rids.size() == 1, "IndexScaned duplicate key");

  // Get the tuple from the table
  *tuple = table_info_->table_->GetTuple(rids[0]).second;
  *rid = rids[0];

  table_info_ = nullptr;  // Reset the table info to avoid multiple scan

  return true;
}

}  // namespace bustub
