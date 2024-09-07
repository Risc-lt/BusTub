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
#include <memory>

#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  Catalog *catalog{exec_ctx_->GetCatalog()};
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indices_ = catalog->GetTableIndexes(table_info_->name_);
  txn_ = exec_ctx_->GetTransaction();
}

void UpdateExecutor::Init() {
  child_executor_->Init();
}

void UpdateExecutor::UpdateIndices(std::vector<Value> &new_v, std::vector<Value> &old_v, RID rid, Transaction *txn) {
  // For-each indices, which could be composite indices.
  for (const auto &i : indices_) {
    std::vector<Value> old_i;
    std::vector<Value> new_i;
    old_i.reserve(i->key_schema_.GetColumnCount());
    new_i.reserve(i->key_schema_.GetColumnCount());
    for (const auto &column : i->key_schema_.GetColumns()) {
      auto idx{table_info_->schema_.GetColIdx(column.GetName())};
      old_i.push_back(old_v[idx]);
      new_i.push_back(new_v[idx]);
    }
    Tuple old_t{old_i, &i->key_schema_};
    i->index_->DeleteEntry(old_t, rid, txn);
    Tuple new_t{new_i, &i->key_schema_};
    if (!i->index_->InsertEntry(new_t, rid, txn)) {
      throw Exception("Insert new index failed");
    }
  }
}

void UpdateExecutor::InsertLog(TupleMeta &meta, RID rid, TransactionManager *txn_mgr, //
                               Transaction *txn, std::vector<Value> &old_v,           //
                               std::vector<Value> &new_v) {
  UndoLog log;
  log.is_deleted_ = meta.is_deleted_;
  log.modified_fields_.resize(table_info_->schema_.GetColumnCount());
  log.ts_ = meta.ts_;

  std::vector<Column> updated_columns;
  std::vector<Value> updated_values;
  for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
    if (new_v[i].CompareExactlyEquals(old_v[i])) {
      continue;
    }
    log.modified_fields_[i] = true;
    updated_columns.push_back(table_info_->schema_.GetColumn(i));
    updated_values.push_back(old_v[i]);
  }
  Schema schema{updated_columns};
  log.tuple_ = Tuple{updated_values, &schema};
  auto link = txn_mgr->GetUndoLink(rid);
  if (link.has_value()) {
    log.prev_version_ = *link;
  }
  txn_mgr->UpdateUndoLink(rid, txn->AppendUndoLog(log));
}

void UpdateExecutor::UpdateLog(TupleMeta &meta, RID rid, TransactionManager *txn_mgr, //
                               Transaction *txn, std::vector<Value> &old_v,           //
                               std::vector<Value> &new_v) {
  auto link = txn_mgr->GetUndoLink(rid);
  if (!link.has_value()) {
    throw Exception{"Invalid deletion to not existed tuple"};
  }
  if (!link->IsValid()) {
    // Empty log, shouldn't update.
    return;
  }
  auto log = txn->GetUndoLog(link->prev_log_idx_);
  std::vector<Column> origin_columns;
  for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
    if (log.modified_fields_[i]) {
      origin_columns.push_back(table_info_->schema_.GetColumn(i));
    }
  }
  Schema origin_schema{origin_columns};
  std::vector<Column> updated_columns;
  std::vector<Value> updated_values;
  for (uint32_t i = 0, j = 0; i < table_info_->schema_.GetColumnCount(); i++) {
    if (new_v[i].CompareExactlyEquals(old_v[i])) {
      if (log.modified_fields_[i]) {
        updated_columns.push_back(table_info_->schema_.GetColumn(i));
        updated_values.push_back(log.tuple_.GetValue(&origin_schema, j));
        j++;
      } else {
        continue;
      }
    } else {
      if (log.modified_fields_[i]) {
        updated_columns.push_back(table_info_->schema_.GetColumn(i));
        updated_values.push_back(log.tuple_.GetValue(&origin_schema, j));
        j++;
      } else {
        log.modified_fields_[i] = true;
        updated_columns.push_back(table_info_->schema_.GetColumn(i));
        updated_values.push_back(old_v[i]);
      }
    }
  }
  Schema schema{updated_columns};
  log.tuple_ = Tuple{updated_values, &schema};
  txn->ModifyUndoLog(link->prev_log_idx_, log);
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    return false;
  }
  // Why a update should read all rows to a local buffer and update each of them?
  int line_updated{0};
  std::vector<Value> new_v(table_info_->schema_.GetColumnCount());
  std::vector<Value> old_v(table_info_->schema_.GetColumnCount());
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  for (; child_executor_->Next(tuple, rid); line_updated++) {
    for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
      new_v[i] = old_v[i] = tuple->GetValue(&table_info_->schema_, i);
      new_v[i] = plan_->target_expressions_[i]->Evaluate(tuple, table_info_->schema_);
    }

    TupleMeta meta{table_info_->table_->GetTupleMeta(*rid)};
    ConflictDetect(txn_mgr, txn, meta, *rid, "Update");
    // The ts is commited, or it update the one create/modified by itself.

    if (txn->GetTransactionTempTs() != meta.ts_) {
      // Which means we have to generate an undo log
      InsertLog(meta, *rid, txn_mgr, txn, old_v, new_v);
    } else {
      // Which means we have to update original undo log
      UpdateLog(meta, *rid, txn_mgr, txn, old_v, new_v);
    }
    // Anyway, we have to update meta.
    meta.ts_ = txn->GetTransactionTempTs();
    table_info_->table_->UpdateTupleMeta(meta, *rid);
    *tuple = Tuple{new_v, &table_info_->schema_};
    table_info_->table_->UpdateTupleInPlace(meta, *tuple, *rid);

    // And update indices.
    UpdateIndices(new_v, old_v, *rid, txn_);
    txn->AppendWriteSet(table_info_->oid_, *rid);
  }
  Value size{TypeId::INTEGER, line_updated};
  *tuple = Tuple{std::vector{size}, &plan_->OutputSchema()};
  table_info_ = nullptr;
  return true;
}

}  // namespace bustub