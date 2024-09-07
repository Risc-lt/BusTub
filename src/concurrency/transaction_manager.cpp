//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  timestamp_t read_ts = last_commit_ts_;
  txn_ref->read_ts_ = read_ts;

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::CheckConflict(const Transaction *txn, const RID &rid,  // NOLINT
                                       const TableInfo *table_info) -> bool {
  BUSTUB_ASSERT(txn->GetTransactionState() == TransactionState::RUNNING,  // NOLINT
                "txn state shall be RUNNING");
  auto [meta, tuple] = table_info->table_->GetTuple(rid);
  if (IsTempTs(meta.ts_) && meta.ts_ == txn->GetTransactionTempTs()) {
    // self modify
    return false;
  }
  auto result = ReconstructValuesFromTuple(&table_info->schema_, tuple);
  auto read_ts = txn->GetReadTs();
  auto watermark = GetWatermark();
  auto run_check = [table_info, txn, &result]() -> bool {
    for (const auto &[oid, preds] : txn->scan_predicates_) {
      if (oid != table_info->oid_) {
        continue;
      }
      Tuple temp{result, &table_info->schema_};
      for (const auto &expr : preds) {
        if (expr->Evaluate(&temp, table_info->schema_).GetAs<bool>()) {
          return true;
        }
      }
    }
    return false;
  };

  if (run_check()) {
    return true;
  }
  for (VersionChainIter iter{this, rid}; !iter.IsEnd(); iter.Next()) {
    auto log = iter.Get();
    if (log.is_deleted_) {
      for (auto &v : result) {
        v = ValueFactory::GetNullValueByType(v.GetTypeId());
      }
    } else {
      ApplyModifications(result, &table_info->schema_, log);
    }
    if (run_check()) {
      return true;
    }
    if (watermark >= log.ts_ || read_ts >= log.ts_) {
      // The end of valid chain or the txn can't read older version anymore.
      break;
    }
  }
  return false;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  if (txn->GetWriteSets().empty()) {
    return true;
  }
  std::unordered_map<table_oid_t, std::unordered_set<RID>> oid_rids;
  std::unique_lock lck{txn_map_mutex_};
  for (auto &[id, another] : txn_map_) {
    if (another->GetWriteSets().empty()) {
      continue;
    }
    if (another->GetTransactionState() != TransactionState::COMMITTED) {
      continue;
    }
    if (another->GetCommitTs() <= txn->GetReadTs()) {
      continue;
    }
    // The commited timestamp, and its commit ts > read ts.
    for (const auto &[oid, rids] : another->GetWriteSets()) {
      oid_rids[oid].insert(rids.begin(), rids.end());
    }
  }
  lck.unlock();
  for (const auto &[oid, rids] : oid_rids) {
    auto table_info = catalog_->GetTable(oid);
    for (auto rid : rids) {
      if (CheckConflict(txn, rid, table_info)) {
        return false;
      }
    }
  }
  return true;
}

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  txn->commit_ts_ = ++last_commit_ts_;

  txn->state_ = TransactionState::COMMITTED;

  for (auto &[tid, set] : txn->write_set_){
    for (auto &rid : set){
      auto table_info = catalog_->GetTable(tid);
      if (table_info == nullptr){
        throw Exception("table not found");
      }

      auto &table = table_info->table_;
      auto meta = table->GetTupleMeta(rid);
      meta.ts_ = txn->commit_ts_;

      table->UpdateTupleMeta(meta, rid);
    }
  }

  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  for (const auto &[oid, rids] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(oid);
    const auto &schema = table_info->schema_;
    for (auto rid : rids) {
      // Dead lock fix:
      //   1. unique lock aquired in commit, and then it fetch page lock
      //   2. page lock fetched in abort, and fetch shared log in iter.Next, etc.
      //
      // To fix it:
      //   1. Get all log info first
      //   2. Write to page
      //   3. Write to txn_mgr
      // We need 1 log.
      VersionChainIter iter{this, rid};
      if (iter.IsEnd()) {
        auto guard = table_info->table_->AcquireTablePageWriteLock(rid);
        auto page = guard.AsMut<TablePage>();
        auto [meta, tpl] = table_info->table_->GetTupleWithLockAcquired(rid, page);
        BUSTUB_ASSERT(meta.ts_ == txn->GetTransactionTempTs(),  // NOLINT
                      "ts in meta should equal to temp ts");
        meta.is_deleted_ = true;
        meta.ts_ = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        table_info->table_->UpdateTupleInPlaceWithLockAcquired(meta, tpl, rid, page);
      } else {
        auto log = iter.Get();  // The place where
        iter.Next();            // cause a dead lock.
        auto guard = table_info->table_->AcquireTablePageWriteLock(rid);
        auto page = guard.AsMut<TablePage>();
        auto [meta, tpl] = table_info->table_->GetTupleWithLockAcquired(rid, page);
        /* For debug use.
         * if (meta.ts_ != txn->GetTransactionTempTs()) {
         *   // another dead lock. page locked before, and lock again later.
         *   guard.Drop();
         *   TxnMgrDbg(fmt::format("{:x} {:x} {}", meta.ts_, // NOLINT
         *                txn->GetTransactionTempTs(), rid.ToString()),
         *             this, table_info, table_info->table_.get());
         * }
         */
        BUSTUB_ASSERT(meta.ts_ == txn->GetTransactionTempTs(),  // NOLINT
                      "ts in meta should equal to temp ts");
        auto values = ReconstructValuesFromTuple(&table_info->schema_, tpl);
        if (log.is_deleted_) {
          for (auto &v : values) {
            v = ValueFactory::GetNullValueByType(v.GetTypeId());
          }
          meta.is_deleted_ = true;
        } else {
          ApplyModifications(values, &table_info->schema_, log);
          meta.is_deleted_ = false;
        }
        meta.ts_ = log.ts_;
        tpl = Tuple{values, &schema};  // damn it, forgot to update!
        table_info->table_->UpdateTupleInPlaceWithLockAcquired(meta, tpl, rid, page);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        UpdateUndoLink(rid, iter.GetLink());
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  std::unordered_set<RID> write_set;
  std::unordered_set<txn_id_t> has_accessible_log_txn;

  // Get the watermark and print out for debugging
  auto watermark = GetWatermark();
  fmt::println(stderr, "Watermark {}, {} txn in GC", watermark, txn_map_.size());

  // Iterate all the write set to find out the txn that has the latest log
  const auto &heap = catalog_->GetTable(catalog_->GetTableNames().at(0))->table_;
  {
    // Record all the RIDs that have been written by any txn
    std::shared_lock l{txn_map_mutex_};
    for (const auto &[_, txn] : txn_map_) {
      for (const auto &[_, set] : txn->GetWriteSets()) {
        for (const auto &rid : set) {
          write_set.insert(rid);
        }
      }
    }

    // Iterate all the RID to find out the txn that has the latest log
    for (auto &rid : write_set) {
      auto meta = heap->GetTupleMeta(rid);
      if (meta.ts_ <= watermark) {
        continue;
      }
      for (VersionChainIter iter{this, rid}; !iter.IsEnd(); iter.Next()) {
        auto txn_iter = txn_map_.find(iter.GetLink().prev_txn_);
        if (txn_iter == txn_map_.end()) {
          continue;
        }
        auto ts = iter.Get().ts_;
        has_accessible_log_txn.insert(iter.GetLink().prev_txn_);
        if (ts <= watermark) {
          // The remains should not be iterated.
          break;
        }
      }
    }
  }

  {
    std::unique_lock l{txn_map_mutex_};
    for (auto i = txn_map_.begin(); i != txn_map_.end();) {
      if (i->second->GetTransactionState() == TransactionState::RUNNING) {
        i++;
        continue;
      }
      if (i->second->GetTransactionState() == TransactionState::TAINTED) {
        i++;
        continue;
      }
      if (has_accessible_log_txn.find(i->first) != has_accessible_log_txn.end()) {
        i++;
        continue;
      }
      fmt::println(stderr, "txn_id={:x} GCed", i->second->GetTransactionId());
      txn_map_.erase(i++);
    }
  }
}

}  // namespace bustub
