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
  for (const auto &i : indices_) {
    has_pk_ |= i->is_primary_key_;
  }
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
}

void UpdateExecutor::Init() { child_executor_->Init(); }

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

void UpdateExecutor::InsertLog(TupleMeta &meta, RID rid,   // NOLINT
                               std::vector<Value> &old_v,  // NOLINT
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
  auto link = txn_mgr_->GetUndoLink(rid);
  if (link.has_value()) {
    log.prev_version_ = *link;
  }
  txn_mgr_->UpdateUndoLink(rid, txn_->AppendUndoLog(log));
}

void UpdateExecutor::UpdateLog(TupleMeta &meta, RID rid,   // NOLINT
                               std::vector<Value> &old_v,  // NOLINT
                               std::vector<Value> &new_v) {
  auto link = txn_mgr_->GetUndoLink(rid);
  if (!link.has_value()) {
    throw Exception{"Invalid deletion to not existed tuple"};
  }
  if (!link->IsValid()) {
    // Empty log, shouldn't update.
    return;
  }
  auto log = txn_->GetUndoLog(link->prev_log_idx_);
  std::vector<Column> origin_columns;
  if (log.is_deleted_) {
    log.modified_fields_.resize(table_info_->schema_.GetColumnCount(), false);
  }
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
  txn_->ModifyUndoLog(link->prev_log_idx_, std::move(log));
}

void UpdateExecutor::GetNewValues(Tuple *tuple, std::vector<Value> &new_v) {
  new_v.resize(table_info_->schema_.GetColumnCount());
  for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
    new_v[i] = tuple->GetValue(&table_info_->schema_, i);
    new_v[i] = plan_->target_expressions_[i]->Evaluate(tuple, table_info_->schema_);
  }
}

void UpdateExecutor::GetOldValues(Tuple *tuple, std::vector<Value> &old_v) {
  old_v.resize(table_info_->schema_.GetColumnCount());
  for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
    old_v[i] = tuple->GetValue(&table_info_->schema_, i);
  }
}

auto UpdateExecutor::UpdateWithoutPrimaryKey(Tuple *tuple, RID *rid) -> int {
  int line_updated{0};
  std::vector<Value> new_v(table_info_->schema_.GetColumnCount());
  std::vector<Value> old_v(table_info_->schema_.GetColumnCount());
  for (; child_executor_->Next(tuple, rid); line_updated++) {
    GetNewValues(tuple, new_v);
    GetOldValues(tuple, old_v);

    TupleMeta meta{table_info_->table_->GetTupleMeta(*rid)};
    ConflictDetect(txn_mgr_, txn_, meta, *rid, "Update");
    // The ts is commited, or it update the one create/modified by itself.

    if (txn_->GetTransactionTempTs() != meta.ts_) {
      // Which means we have to generate an undo log
      InsertLog(meta, *rid, old_v, new_v);
    } else {
      // Which means we have to update original undo log
      UpdateLog(meta, *rid, old_v, new_v);
    }
    // Anyway, we have to update meta.
    meta.ts_ = txn_->GetTransactionTempTs();
    table_info_->table_->UpdateTupleMeta(meta, *rid);
    *tuple = Tuple{new_v, &table_info_->schema_};
    table_info_->table_->UpdateTupleInPlace(meta, *tuple, *rid);

    // And update indices.
    UpdateIndices(new_v, old_v, *rid, txn_);
    txn_->AppendWriteSet(table_info_->oid_, *rid);
  }
  return line_updated;
}

void UpdateExecutor::BuildKeys(const std::vector<Value> &values) {
  keys_.resize(indices_.size());
  for (uint32_t i = 0; i < indices_.size(); i++) {
    index_temp_.resize(indices_[i]->key_schema_.GetColumnCount());
    for (uint32_t j = 0; j < indices_[i]->key_schema_.GetColumnCount(); j++) {
      const auto &name = indices_[i]->key_schema_.GetColumn(j).GetName();
      auto idx = table_info_->schema_.GetColIdx(name);
      index_temp_[j] = values[idx];
    }
    keys_[i] = Tuple{index_temp_, &indices_[i]->key_schema_};
  }
}

auto UpdateExecutor::GetRID() -> RID {
  // Since all index updated when insert index. We scan 1 only.
  rids_.resize(0);
  indices_[0]->index_->ScanKey(keys_[0], &rids_, txn_);
  if (rids_.empty()) {
    return {};
  }
  return rids_[0];
}

void UpdateExecutor::InsertNewIndices(RID rid) {
  // For-each indices, which could be composite indices.
  for (uint32_t i = 0; i < indices_.size(); i++) {
    if (!indices_[i]->index_->InsertEntry(keys_[i], rid, txn_)) {
      txn_->SetTainted();
      throw ExecutionException("Duplicated index!");
    }
  }
}

auto UpdateExecutor::InsertNewTuple(const Tuple *tuple) -> RID {
  TupleMeta meta{};
  auto txn = exec_ctx_->GetTransaction();
  meta.ts_ = txn->GetTransactionTempTs();
  meta.is_deleted_ = false;
  // BUSTUB_ASSERT(meta.is_deleted_ != true, "meta shall be false");
  auto result{table_info_->table_->InsertTuple(meta, *tuple)};
  if (!result.has_value()) {
    throw Exception("Tuple too large");
  }
  txn->AppendWriteSet(plan_->table_oid_, *result);
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  txn_mgr->UpdateVersionLink(*result, VersionUndoLink{}, nullptr);
  txn_->AppendWriteSet(table_info_->oid_, *result);
  return *result;
}

void UpdateExecutor::UpdateDeleted(TupleMeta &meta, RID rid,  // NOLINT
                                   Tuple *tuple, TablePage *page) {
  auto link_opt = txn_mgr_->GetUndoLink(rid);
  if (!link_opt.has_value()) {
    throw Exception{"Cannot get undo link"};
  }
  UndoLink link = *link_opt;
  if (link.IsValid()) {
    UndoLog log;
    log.ts_ = meta.ts_, log.prev_version_ = link;
    log.is_deleted_ = true;
    txn_mgr_->UpdateUndoLink(rid, txn_->AppendUndoLog(log));
  }
  meta.is_deleted_ = false;
  meta.ts_ = txn_->GetTransactionTempTs();
  table_info_->table_->UpdateTupleInPlaceWithLockAcquired(meta, *tuple, rid, page);
  txn_->AppendWriteSet(table_info_->oid_, rid);
}

void UpdateExecutor::UpdateSelfOperation(TupleMeta &meta, RID rid,  // NOLINT
                                         Tuple *tuple, TablePage *page) {
  /* there is nothing to do with undo link. It only delete and insert values
   * auto link_opt = txn_mgr_->GetUndoLink(rid);
   * if (!link_opt.has_value()) {
   *   throw Exception{"Cannot get undo link"};
   * }
   */
  meta.is_deleted_ = false;
  table_info_->table_->UpdateTupleInPlaceWithLockAcquired(meta, *tuple, rid, page);
  txn_->AppendWriteSet(table_info_->oid_, rid);
}

auto UpdateExecutor::UpdateWithPrimaryKey(Tuple *tuple, RID *rid) -> int {
  int line_updated{0};
  std::vector<std::vector<Value>> new_vs;
  // delete first
  for (; child_executor_->Next(tuple, rid); line_updated++) {
    std::vector<Value> new_v;
    GetNewValues(tuple, new_v);
    new_vs.emplace_back(std::move(new_v));

    auto page_guard = table_info_->table_->AcquireTablePageWriteLock(*rid);
    auto page = page_guard.AsMut<TablePage>();
    TupleMeta meta{page->GetTupleMeta(*rid)};
    ConflictDetect(txn_mgr_, txn_, meta, *rid, "Update");
    GenerateDeleteLogInPage(meta, tuple, *rid, table_info_, txn_, txn_mgr_, page);
    txn_->AppendWriteSet(table_info_->oid_, *rid);
  }

  // and insert them.
  for (const auto &v : new_vs) {
    // insert with index update.
    BuildKeys(v);
    auto rid = GetRID();
    *tuple = Tuple{v, &table_info_->schema_};
    if (rid.GetPageId() < 0) {
      rid = InsertNewTuple(tuple);
      InsertNewIndices(rid);
      continue;
    }
    auto page_guard = table_info_->table_->AcquireTablePageWriteLock(rid);
    auto page = page_guard.AsMut<TablePage>();
    auto meta{page->GetTupleMeta(rid)};
    ConflictDetect(txn_mgr_, txn_, meta, rid, "Insert");
    if (meta.is_deleted_) {
      // Update log
      if (meta.ts_ == txn_->GetTransactionTempTs()) {
        // Self update
        UpdateSelfOperation(meta, rid, tuple, page);
      } else {  // NOLINT
        UpdateDeleted(meta, rid, tuple, page);
      }
    } else {
      // Not deleted, there is duplicated index.
      txn_->SetTainted();
      throw ExecutionException{"Duplicated index!"};
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return line_updated;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    return false;
  }
  // Why a update should read all rows to a local buffer and update each of them?
  int line_updated = 0;
  if (has_pk_) {
    line_updated = UpdateWithPrimaryKey(tuple, rid);
  } else {
    line_updated = UpdateWithoutPrimaryKey(tuple, rid);
  }
  Value size{TypeId::INTEGER, line_updated};
  *tuple = Tuple{std::vector{size}, &plan_->OutputSchema()};
  table_info_ = nullptr;
  return true;
}

}  // namespace bustub