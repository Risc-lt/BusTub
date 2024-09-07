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

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

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

  // TODO(fall2023): Implement the abort logic!

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
