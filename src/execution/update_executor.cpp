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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
      // Get the catalog from the executor context
      auto catalog = exec_ctx_->GetCatalog();

      // Initialize the table info and indexes
      table_info_ = catalog->GetTable(plan_->table_oid_);
      indexes_ = catalog->GetTableIndexes(table_info_->name_);

      // Initialize the transaction
      txn_ = exec_ctx_->GetTransaction();
}

void UpdateExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Check if the table info is null
  if (table_info_ == nullptr) {
    return false;
  }

  // Initialize the recording data
  int line_updated = 0;

  // Volcano model
  while (child_executor_->Next(tuple, rid)) {
    // Update the values of the tuple
    std::vector<Value> values{};
    for(auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(tuple, table_info_->schema_));
    }
    Tuple new_tuple{values, &table_info_->schema_};

    // Remove the old tuple
    auto old_tuplemeta = table_info_->table_->GetTupleMeta(*rid);
    old_tuplemeta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(old_tuplemeta, *rid);

    // Insert the new tuple
    auto new_tuplemeta = TupleMeta{INVALID_TXN_ID, false};
    auto new_rid_optional = table_info_->table_->InsertTuple(new_tuplemeta, new_tuple);

    // Update the indexes
    if(new_rid_optional.has_value()) {
      // Update the line updated
      line_updated++;
      *rid = new_rid_optional.value();

      // Remove the old indexes and insert the new indexes
      for(auto &index : indexes_) {
        // Update only modify the values, so the keys of the tuple will not change
        auto key_schema = index->index_->GetKeySchema();
        auto key_attrs = index->index_->GetKeyAttrs();

        // Get the key of the old tuple and the new tuple
        auto old_tuplekey = tuple->KeyFromTuple(table_info_->schema_, *key_schema, key_attrs);
        auto new_tuplekey = new_tuple.KeyFromTuple(table_info_->schema_, *key_schema, key_attrs);

        // Remove the old index
        index->index_->DeleteEntry(old_tuplekey, *rid, txn_);

        // Insert the new index
        index->index_->InsertEntry(new_tuplekey, *rid, txn_);
      }
    }
  }

  // Create the output tuple
  std::vector<Value> values{};
  values.emplace_back(Value(INTEGER, line_updated));
  *tuple = Tuple(values, &GetOutputSchema());

  return true;
  
}

}  // namespace bustub
