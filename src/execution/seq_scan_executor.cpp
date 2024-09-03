//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // Initialize the table heap and the iterator
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = table_info_->table_.get();

  auto iter = table_heap_->MakeIterator();

  // Initialize the rids_ vector and the rids_iter_ iterator
  rids_.clear();
  while (!iter.IsEnd()) {
    rids_.push_back(iter.GetRID());
    ++iter;
  }
  rids_iter_ = rids_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Initialize the meta data of the tuple
  TupleMeta meta{};

  // Iterate through the table heap and find the next tuple that satisfies the predicate
  do {
    // If the iterator reaches the end, return false
    if (rids_iter_ == rids_.end()) {
      return false;
    }

    // Get the tuple and the meta data of the tuple
    meta = table_heap_->GetTupleMeta(*rids_iter_);
    if (!meta.is_deleted_) {
      *tuple = table_heap_->GetTuple(*rids_iter_).second;
      *rid = *rids_iter_;
    }
    ++rids_iter_;

    // If the tuple is deleted or does not satisfy the predicate, continue the loop
  } while (meta.is_deleted_ || (plan_->filter_predicate_ != nullptr &&
                                !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>()));
  return true;
}
}  // namespace bustub