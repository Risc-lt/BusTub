//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/schema.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // Get the catalog form the executor context
  Catalog *catalog{exec_ctx_->GetCatalog()};
  // Get the table info and indexes
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
  // Initialize the transaction
  txn_ = exec_ctx_->GetTransaction();
}

void InsertExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

void InsertExecutor::InsertIndexes(std::vector<Value> &v, RID rid, Transaction *txn) {
  // Iterate through all the indexes
  for (const auto &i : indexes_) {
    // Initialize the index vector
    std::vector<Value> index;
    index.reserve(i->key_schema_.GetColumnCount());

    // Iterate through all the columns of the index
    for (const auto &column : i->key_schema_.GetColumns()) {
      auto idx{table_info_->schema_.GetColIdx(column.GetName())};
      index.push_back(v[idx]);
    }

    // Create the tuple and insert it into the index
    Tuple tuple{index, &i->key_schema_};
    if (!i->index_->InsertEntry(tuple, rid, txn)) {
      throw Exception("Insert index failed");
    }
  }
}

auto InsertExecutor::InsertTuple(Tuple &tuple) -> RID {
  // Intialize the tuple meta
  TupleMeta meta{};

  // Insert the tuple into the table and check if succeed
  auto result{table_info_->table_->InsertTuple(meta, tuple)};
  if (!result.has_value()) {
    throw Exception("Tuple too large");
  }

  return *result;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // Check if the table info is null
  if (table_info_ == nullptr) {
    return false;
  }

  // Initialize the line counter and the vector of values
  int line_inserted = 0;
  std::vector<Value> v;
  v.resize(table_info_->schema_.GetColumnCount());

  // Get the tuple from the child executor
  for (; child_executor_->Next(tuple, rid); line_inserted++) {
    // Insert the tuple into the table
    *rid = InsertTuple(*tuple);
    // Get all values from the tuple
    for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
      v[i] = tuple->GetValue(&table_info_->schema_, i);
    }
    InsertIndexes(v, *rid, txn_);
  }

  // Create the return tuple with size and return schema
  Value size{TypeId::INTEGER, line_inserted};
  Schema *return_schema{new Schema({Column{"result", TypeId::INTEGER}})};

  *tuple = Tuple{std::vector{size}, return_schema};

  // Set the table info to null and free the memory
  table_info_ = nullptr;
  delete return_schema;

  return true;
}

}  // namespace bustub
