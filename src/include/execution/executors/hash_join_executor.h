//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

/**
 * Construct HashJoinKey with hash_keys_ member variable
 */
namespace bustub {

/** HashJoinKey represents a key in an join operation */
struct HashJoinKey {
  std::vector<Value> hash_keys_;

  /**
   * Compares two hash joi keys for equality
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join key have equivalent values
   */
  auto operator==(const HashJoinKey &other) const -> bool {
    // Compare every hash key in the hash_keys_ vector
    for (uint32_t i = 0; i < other.hash_keys_.size(); ++i) {
      if (hash_keys_[i].CompareEquals(other.hash_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

/**
 * Implement std::hash on AggregateKey
 */
namespace std {
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.hash_keys_) {
      if (!key.IsNull()) {
        // Combine the hash value of the key with the current hash value
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

/**
 * Implement HashJoinExecutor
 */
namespace bustub {
/**
 * A simplified hash table that has all the necessary functionality for join.
 */
class SimpleHashJoinHashTable {
 public:
  /**
   * Insert a key-value pair into the hash table.
   * @param join_key The key to insert.
   * @param tuple The value to insert.
   */
  void InsertKey(const HashJoinKey &join_key, const Tuple &tuple) {
    if (ht_.count(join_key) == 0) {
      std::vector<Tuple> tuple_vector;
      tuple_vector.push_back(tuple);
      ht_.insert({join_key, tuple_vector});
    } else {
      ht_.at(join_key).push_back(tuple);
    }
  }

  /**
   * Get the value associated with a key.
   * @param join_key The key to look up.
   * @return The value associated with the key.
   */
  auto GetValue(const HashJoinKey &join_key) -> std::vector<Tuple> * {
    if (ht_.find(join_key) == ht_.end()) {
      return nullptr;
    }
    return &(ht_.find(join_key)->second);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_{};
};

}  // namespace bustub

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /**
   * Get the join key from the tuple for the left side of the join.
   * @param tuple The tuple to get the join key from.
   * @return The join key.
   */
  auto GetLeftJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
    }
    return {values};
  }

  /**
   * Get the join key from the tuple for the right side of the join.
   * @param tuple The tuple to get the join key from.
   * @return The join key.
   */
  auto GetRightJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
    }
    return {values};
  }

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** The hash table that stores the tuples from the left side of the join. */
  std::unique_ptr<SimpleHashJoinHashTable> jht_;

  /** Iterator for the hash table. */
  std::vector<Tuple>::iterator jht_iter_;

  /** The child executor that produces tuples for the left side of join. */
  std::unique_ptr<AbstractExecutor> left_child_;

  /** The child executor that produces tuples for the right side of join. */
  std::unique_ptr<AbstractExecutor> right_child_;

  /** The tuple from the left side of the join that is being processed. */
  Tuple left_tuple_;
  RID left_rid_;

  /** The tuple from the right side of the join that is being processed. */
  std::vector<Tuple> *right_tuples_{nullptr};

  /**  Flags for join status. */
  bool join_done_{false};
  bool left_next_{false};
};

}  // namespace bustub
