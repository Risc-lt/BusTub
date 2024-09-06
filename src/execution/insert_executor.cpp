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
#include <mutex>
#include <vector>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/projection_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_executor_{std::move(child_executor)},
      return_schema_{{{"result", TypeId::INTEGER}}} {
  // Catalog
  Catalog *catalog{exec_ctx_->GetCatalog()};
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);

  // Transaction
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
}

void InsertExecutor::Init() { child_executor_->Init(); }

void InsertExecutor::InsertNewIndexes(RID rid) {
  // Iterate all indexes and insert
  for (uint32_t i = 0; i < indexes_.size(); i++) {
    if (!indexes_[i]->index_->InsertEntry(keys_[i], rid, txn_)) {
      // Insert failed, rollback
      txn_->SetTainted();
      throw ExecutionException("Duplicated index!");
    }
  }
}

auto InsertExecutor::InsertNewTuple(const Tuple *tuple) -> RID {
  //  Create meta
  TupleMeta meta{};

  // Set meta data
  meta.ts_ = txn_->GetTransactionTempTs();
  meta.is_deleted_ = false;

  // Insert tuple
  auto result{table_info_->table_->InsertTuple(meta, *tuple)};
  if (!result.has_value()) {
    throw Exception("Tuple too large");
  }

  // Update version link and append write set
  txn_mgr_->UpdateVersionLink(*result, VersionUndoLink{}, nullptr);
  txn_->AppendWriteSet(table_info_->oid_, *result);

  return *result;
}

void InsertExecutor::BuildIndexes(Tuple *tuple) {
  // Copy all values from tuple
  std::vector<Value> v;
  v.resize(table_info_->schema_.GetColumnCount());
  for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
    v[i] = tuple->GetValue(&table_info_->schema_, i);
  }

  // Build keys for all indexes
  keys_.resize(indexes_.size());
  for (uint32_t i = 0; i < indexes_.size(); i++) {
    index_temp_.resize(indexes_[i]->key_schema_.GetColumnCount());
    for (uint32_t j = 0; j < indexes_[i]->key_schema_.GetColumnCount(); j++) {
      const auto &name = indexes_[i]->key_schema_.GetColumn(j).GetName();
      auto idx = table_info_->schema_.GetColIdx(name);
      index_temp_[j] = v[idx];
    }
    keys_[i] = Tuple{index_temp_, &indexes_[i]->key_schema_};
  }
}

auto InsertExecutor::GetRID() -> RID {
  // Set rids_ to empty
  rids_.resize(0);

  // Scan key for the first index
  indexes_[0]->index_->ScanKey(keys_[0], &rids_, txn_);

  // If no RIDs, return a RID with negative page id
  if (rids_.empty()) {
    return {};
  }

  return rids_[0];
}

void InsertExecutor::UpdateSelfOperation(TupleMeta &meta, RID rid, Tuple *tuple) {
  // Get undo link and check if it is valid
  auto link_opt = txn_mgr_->GetUndoLink(rid);
  if (!link_opt.has_value()) {
    throw Exception{"Cannot get undo link"};
  }

  // Update the undo log
  meta.is_deleted_ = false;
  table_info_->table_->UpdateTupleInPlace(meta, *tuple, rid);

  // Append write set
  txn_->AppendWriteSet(table_info_->oid_, rid);
}

void InsertExecutor::UpdateDeleted(TupleMeta &meta, RID rid, Tuple *tuple) {
  // Get undo link and check if it is valid
  auto link_opt = txn_mgr_->GetUndoLink(rid);
  if (!link_opt.has_value()) {
    throw Exception{"Cannot get undo link"};
  }

  // Update the undo log
  UndoLink link = *link_opt;
  if (link.IsValid()) {
    UndoLog log;
    log.ts_ = meta.ts_, log.prev_version_ = link;
    log.is_deleted_ = true;
    txn_mgr_->UpdateUndoLink(rid, txn_->AppendUndoLog(log));
  }

  // Update the tuple and set the meta data
  meta.is_deleted_ = false;
  meta.ts_ = txn_->GetTransactionTempTs();
  table_info_->table_->UpdateTupleInPlace(meta, *tuple, rid);
  txn_->AppendWriteSet(table_info_->oid_, rid);
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Check if table_info_ is null
  if (table_info_ == nullptr) {
    return false;
  }

  int line_inserted{0};
  // Insert all tuples
  for (; child_executor_->Next(tuple, rid); line_inserted++) {
    // If there is no index, insert directly
    if (indexes_.empty()) {
      *rid = InsertNewTuple(tuple);
      continue;
    }

    // Build indexes
    BuildIndexes(tuple);
    *rid = GetRID();
    if (rid->GetPageId() < 0) {
      // New insert tuple
      *rid = InsertNewTuple(tuple);
      InsertNewIndexes(*rid);
      continue;
    }

    // Check if there is conflict
    auto meta = table_info_->table_->GetTupleMeta(*rid);
    ConflictDetect(txn_mgr_, txn_, meta, *rid, "Insert");
    if (meta.is_deleted_) {
      // Update log
      if (meta.ts_ == txn_->GetTransactionTempTs()) {
        // Self update
        UpdateSelfOperation(meta, *rid, tuple);
      } else {  
        UpdateDeleted(meta, *rid, tuple);
      }
    } else {
      // Not deleted, there is duplicated index.
      txn_->SetTainted();
      throw ExecutionException{"Duplicated index!"};
    }
  }

  // Construct the return tuple
  Value size{TypeId::INTEGER, line_inserted};
  *tuple = Tuple{std::vector{size}, &return_schema_};
  table_info_ = nullptr;
  return true;
}

}  // namespace bustub