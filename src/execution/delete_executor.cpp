//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // Initialize the table info, transaction, table heap, and index info vector
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  Transaction *tx = GetExecutorContext()->GetTransaction();
  TableHeap *table_heap = table_info->table_.get();
  std::vector<IndexInfo *> index_info_vector = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);

  // Initialize the child tuple and the number of lines deleted
  Tuple child_tuple{};
  int line_deleted = 0;

  // Iterate through the child executor and delete the tuples
  while (child_executor_->Next(&child_tuple, rid)) {
    // Mardk the tuple as deleted
    table_heap->UpdateTupleMeta(TupleMeta{tx->GetTransactionTempTs(), true}, *rid);

    // Delete the tuple from the indexes
    for (auto &index_info : index_info_vector) {
      index_info->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *rid, tx);
    }

    line_deleted++;
  }

  // Create the output tuple
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, line_deleted);

  *tuple = Tuple{values, &GetOutputSchema()};

  // Return true if the number of lines deleted is not zero and the executor is not deleted
  if (line_deleted == 0 && !is_deleted_) {
    is_deleted_ = true;
    return true;
  }

  is_deleted_ = true;
  return line_deleted != 0;
}

}  // namespace bustub
