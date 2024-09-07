//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * InsertExecutor executes an insert on a table.
 * Inserted values are always pulled from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new InsertExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the insert */
  void Init() override;

  /**
   * Yield the number of rows inserted into the table.
   * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
   * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
   */
  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /**
   * Insert the indices from tuple.
   */
  void InsertNewIndices(RID rid);
  std::vector<Value> index_temp_;
  std::vector<Tuple> keys_;
  std::vector<RID> rids_;
  void BuildIndices(Tuple *tuple);
  auto GetRID() -> RID;

  /**
   * Insert a tuple.
   */
  auto InsertNewTuple(const Tuple *tuple) -> RID;

  /**
   * @brief the txn insert to the deleted by it self.
   */
  void UpdateSelfOperation(TupleMeta &meta, RID rid, Tuple *tuple);
  /**
   * @brief the txn insert to the commited deleted.
   */
  void UpdateDeleted(TupleMeta &meta, RID rid, Tuple *tuple);

  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;

  TableInfo *table_info_;
  std::vector<IndexInfo *> indices_;

  const AbstractPlanNode *node_;
  Transaction *txn_;
  TransactionManager *txn_mgr_;

  /** The schema for return a value */
  Schema return_schema_;
  std::unique_ptr<AbstractExecutor> child_executor_;
};

}  // namespace bustub
