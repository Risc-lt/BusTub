#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "storage/table/tuple.h"

namespace bustub {

inline auto IsTxnCommited(txn_id_t id) -> bool { return (id & TXN_START_ID) == 0; }

inline auto IsTempTs(timestamp_t ts) -> bool { return (ts & TXN_START_ID) != 0; }

inline auto GetTxnId(txn_id_t id) { return id & ~TXN_START_ID; }

class VersionChainIter {
 public:
  VersionChainIter(TransactionManager *txn_mgr, const RID rid) : txn_mgr_{txn_mgr} {
    auto link_opt = txn_mgr->GetUndoLink(rid);
    if (!link_opt.has_value()) {
      throw Exception{fmt::format("Cannot get link from the rid{}/{}", rid.GetPageId(), rid.GetSlotNum())};
    }
    link_ = link_opt.value();
  }

  void Next() {
    link_ = log_.prev_version_;
    log_.prev_version_.prev_txn_ = INVALID_TXN_ID;
  }

  auto Get() {
    log_ = txn_mgr_->GetUndoLog(link_);
    return log_;
  }

  auto GetLink() { return link_; }

  auto IsEnd() { return !link_.IsValid(); }

 private:
  TransactionManager *txn_mgr_;
  UndoLink link_;
  UndoLog log_;
};

/**
 * @brief Detect conflict. After running the func,
 *        you NEED ONLY to (1) check self modify, do something in this senario.
 *                         (2) runs modification.
 */
inline void ConflictDetect(TransactionManager *txn_mgr, Transaction *txn,  // NOLINT
                           const TupleMeta &meta, RID rid, const std::string &type) {
  if (IsTempTs(meta.ts_)) {
    if (txn->GetTransactionTempTs() != meta.ts_) {
      txn->SetTainted();
      throw ExecutionException{type + " uncommited conflicting occured."};
    }
  } else {  // Commited ts
    if (txn->GetReadTs() < meta.ts_) {
      // Will this possible?
      // Yes. txn A, B start at the same time, but B modified and commit first.
      // In this case, A cannot modify anymore.
      txn->SetTainted();
      throw ExecutionException{type + " commited conflicting occured."};
    }
  }
}

/**
 * @brief this func will
 *        (1) detect self opration, will perform a replace if it is.
 *        (2) update meta in table info.
 *        something not do
 *        (1) update write set.
 *        (2) conflict detect.
 *        (3) any operation of index.
 */
void GenerateDeleteLogSmart(TupleMeta &meta, Tuple *tuple, RID rid,         // NOLINT
                            const TableInfo *table_info, Transaction *txn,  // NOLINT
                            TransactionManager *txn_mgr);

/**
 * @brief similar to GenerateDeleteLogSmart, but with a lock-guarded page.
 */
void GenerateDeleteLogInPage(TupleMeta &meta, Tuple *tuple, RID rid,         // NOLINT
                             const TableInfo *table_info, Transaction *txn,  // NOLINT
                             TransactionManager *txn_mgr, TablePage *page);

/**
 * @brief Insert a tuple with index update.
 */
void InsertWithIndexUpdate();

/**
 * @brief replace the changed values, use the info of log without checking is_deleted.
 */
void ApplyModifications(std::vector<Value> &values,  //
                        const Schema *schema, const UndoLog &log);

/**
 * @brief use a schema and its corrosponding tuple to construct values
 */
auto ReconstructValuesFromTuple(const Schema *schema,  //
                                const Tuple &tuple) -> std::vector<Value>;

/**
 * @brief use the base tuple and its meta info to reconstruct the tuple after applying undo logs
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

/**
 * @brief rebuild tuple, iterate all log chain, return after its ts = read_ts.
 * @return the tuple is deleted or not.
 */
auto ReconstructFor(TransactionManager *txn_mgr, Transaction *txn, Tuple *tuple, RID rid, TupleMeta &meta,
                    const Schema *schema) -> bool;

/**
 * @brief print the debug info of the transaction manager
 */
void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
