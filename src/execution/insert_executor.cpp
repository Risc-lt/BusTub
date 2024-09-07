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
      return_schema_{{{"result", TypeId::INTEGER}}},
      child_executor_{std::move(child_executor)} {
  Catalog *catalog{exec_ctx_->GetCatalog()};
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indices_ = catalog->GetTableIndexes(table_info_->name_);
  node_ = plan_->GetChildren().at(0).get();
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
}

void InsertExecutor::Init() { child_executor_->Init(); }

void InsertExecutor::InsertNewIndices(RID rid) {
  // For-each indices, which could be composite indices.
  for (uint32_t i = 0; i < indices_.size(); i++) {
    if (!indices_[i]->index_->InsertEntry(keys_[i], rid, txn_)) {
      txn_->SetTainted();
      throw ExecutionException("Duplicated index!");
    }
  }
}

auto InsertExecutor::InsertNewTuple(const Tuple *tuple) -> RID {
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

void InsertExecutor::BuildIndices(Tuple *tuple) {
  std::vector<Value> v;
  v.resize(table_info_->schema_.GetColumnCount());
  for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
    v[i] = tuple->GetValue(&table_info_->schema_, i);
  }
  keys_.resize(indices_.size());
  for (uint32_t i = 0; i < indices_.size(); i++) {
    index_temp_.resize(indices_[i]->key_schema_.GetColumnCount());
    for (uint32_t j = 0; j < indices_[i]->key_schema_.GetColumnCount(); j++) {
      const auto &name = indices_[i]->key_schema_.GetColumn(j).GetName();
      auto idx = table_info_->schema_.GetColIdx(name);
      index_temp_[j] = v[idx];
    }
    keys_[i] = Tuple{index_temp_, &indices_[i]->key_schema_};
  }
}

auto InsertExecutor::GetRID() -> RID {
  // Since all index updated when insert index. We scan 1 only.
  rids_.resize(0);
  indices_[0]->index_->ScanKey(keys_[0], &rids_, txn_);
  if (rids_.empty()) {
    return {};
  }
  return rids_[0];
}

void InsertExecutor::UpdateSelfOperation(TupleMeta &meta, RID rid, Tuple *tuple) {
  auto link_opt = txn_mgr_->GetUndoLink(rid);
  if (!link_opt.has_value()) {
    throw Exception{"Cannot get undo link"};
  }
  meta.is_deleted_ = false;
  table_info_->table_->UpdateTupleInPlace(meta, *tuple, rid);
  txn_->AppendWriteSet(table_info_->oid_, rid);
}

void InsertExecutor::UpdateDeleted(TupleMeta &meta, RID rid, Tuple *tuple) {
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
  table_info_->table_->UpdateTupleInPlace(meta, *tuple, rid);
  txn_->AppendWriteSet(table_info_->oid_, rid);
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    return false;
  }
  int line_inserted{0};
  for (; child_executor_->Next(tuple, rid); line_inserted++) {
    if (indices_.empty()) {
      *rid = InsertNewTuple(tuple);
      continue;
    }
    BuildIndices(tuple);
    *rid = GetRID();
    if (rid->GetPageId() < 0) {
      // New insert tuple
      *rid = InsertNewTuple(tuple);
      InsertNewIndices(*rid);
      continue;
    }
    auto meta = table_info_->table_->GetTupleMeta(*rid);
    ConflictDetect(txn_mgr_, txn_, meta, *rid, "Insert");
    if (meta.is_deleted_) {
      // Update log
      if (meta.ts_ == txn_->GetTransactionTempTs()) {
        // Self update
        UpdateSelfOperation(meta, *rid, tuple);
      } else {  // NOLINT
        UpdateDeleted(meta, *rid, tuple);
      }
    } else {
      // Not deleted, there is duplicated index.
      txn_->SetTainted();
      throw ExecutionException{"Duplicated index!"};
    }
  }
  Value size{TypeId::INTEGER, line_inserted};
  *tuple = Tuple{std::vector{size}, &return_schema_};
  table_info_ = nullptr;
  return true;
}

}  // namespace bustub