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

#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  Catalog *catalog{exec_ctx_->GetCatalog()};
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indices_ = catalog->GetTableIndexes(table_info_->name_);
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
}

void DeleteExecutor::Init() { child_executor_->Init(); }

void DeleteExecutor::DeleteIndices(std::vector<Value> &old_v, RID rid, Transaction *txn) {
  // For-each indices, which could be composite indices.
  for (const auto &i : indices_) {
    std::vector<Value> old_i;
    old_i.reserve(i->key_schema_.GetColumnCount());
    for (const auto &column : i->key_schema_.GetColumns()) {
      auto idx{table_info_->schema_.GetColIdx(column.GetName())};
      old_i.push_back(old_v[idx]);
    }
    Tuple old_t{old_i, &i->key_schema_};
    i->index_->DeleteEntry(old_t, rid, txn);
  }
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    return false;
  }
  int line_updated{0};
  auto txn = exec_ctx_->GetTransaction();
  for (; child_executor_->Next(tuple, rid); line_updated++) {
    TupleMeta meta{table_info_->table_->GetTupleMeta(*rid)};
    ConflictDetect(txn_mgr_, txn, meta, *rid, "Delete");
    GenerateDeleteLogSmart(meta, tuple, *rid, table_info_, txn_, txn_mgr_);

    // Indices shall not be deleted.
    txn->AppendWriteSet(table_info_->oid_, *rid);
  }
  Value size{TypeId::INTEGER, line_updated};
  *tuple = Tuple{std::vector{size}, &plan_->OutputSchema()};
  table_info_ = nullptr;
  return true;
}

}  // namespace bustub