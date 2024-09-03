//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/config.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();

  // Get the catalog from the executor context
  auto catalog = exec_ctx_->GetCatalog();

  // Initialize the table info
  table_info_ = catalog->GetTable(plan_->table_oid_);
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Return false if the update has already been executed
  if (is_updated_) {
    return false;
  }

  // Initialize the new table info, transaction, table heap, and index info vector
  Transaction *tx = GetExecutorContext()->GetTransaction();
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  std::vector<IndexInfo *> index_info_vector = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);

  // Initialize the old tuple and update count
  Tuple old_tuple{};
  int32_t line_updated = 0;

  // Iterate through the child executor and update the tuples
  while (child_executor_->Next(&old_tuple, rid)) {
    // Get the new values from the target expressions
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }

    // Update the tuple in place
    auto to_update_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    bool updated =
        table_info_->table_->UpdateTupleInPlace(TupleMeta{tx->GetTransactionTempTs(), false}, to_update_tuple, *rid);

    // Update the indexes
    if (updated) {
      line_updated++;
      for (auto &index_info : index_info_vector) {
        index_info->index_->DeleteEntry(
            old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
            *rid, tx);
        index_info->index_->InsertEntry(to_update_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                                                     index_info->index_->GetKeyAttrs()),
                                        *rid, tx);
      }
    }
  }

  // Create the output tuple
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, line_updated);
  *tuple = Tuple{values, &GetOutputSchema()};

  // Set the update flag to true
  is_updated_ = true;
  return true;
}

}  // namespace bustub
