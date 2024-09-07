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
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(nullptr) {
  // Get the table metadata from the catalog
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());

  // Get the transaction from the executor context
  txn_ = exec_ctx->GetTransaction();

  // If the plan has a predicate, append the predicate to the transaction
  if (plan_->filter_predicate_ != nullptr) {
    txn_->AppendScanPredicate(table_info_->oid_, plan_->filter_predicate_);
  }
}

void SeqScanExecutor::Init() {
  // Initialize the iterator
  iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Iterate through the table
  for (; !iter_->IsEnd(); ++(*iter_)) {
    // Get the tuple and the RID
    auto [m, t]{iter_->GetTuple()};
    *rid = iter_->GetRID();

    // Reconstruct the tuple and check if it is deleted
    bool deleted = ReconstructFor(exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(), &t, *rid, m,
                                  &plan_->OutputSchema());

    // If the tuple is deleted or the predicate is not satisfied, continue
    if (deleted) {
      continue;
    }
    if (plan_->filter_predicate_ != nullptr) {
      auto result{plan_->filter_predicate_->Evaluate(&t, table_info_->schema_)};
      if (!result.GetAs<bool>()) {
        continue;
      }
    }

    // If the tuple is not deleted and the predicate is satisfied, return the tuple
    ++(*iter_);
    *tuple = t;

    return true;
  }

  return false;
}

}  // namespace bustub
