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

#include <cstdint>
#include <memory>
#include <utility>

#include <unordered_map>
#include "catalog/schema.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

/**
 * HashJoinKey is composed of certain attributes(depending on key expressions) of a tuple.
 */
struct HashJoinKey {
  std::vector<Value> attrs_;

  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.attrs_.size(); i++) {
      if (attrs_[i].CompareEquals(other.attrs_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/**
 * HashJoinValue is composed of all attributes of a tuple.
 */
struct HashJoinValue {
  std::vector<Value> values_;
};

struct HashJoinKeyHash {
  auto operator()(const bustub::HashJoinKey &hash_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : hash_key.attrs_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

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

  // added
  auto MakeLeftJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, left_executor_->GetOutputSchema()));
    }
    return {keys};
  }
  auto MakeRightJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> keys;
    // tuple->GetValue(const Schema *schema, uint32_t column_idx)
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, right_executor_->GetOutputSchema()));
    }
    return {keys};
  }
  auto MakeHashJoinValue(const Tuple *tuple, const Schema *schema) -> HashJoinValue {
    std::vector<Value> keys;
    auto column_cnt = schema->GetColumnCount();
    for (uint32_t idx = 0; idx < column_cnt; ++idx) {
      keys.emplace_back(tuple->GetValue(schema, idx));
    }
    return {keys};
  }
  auto JoinValueToTuple(const HashJoinValue *join_value, const Schema *schema) -> Tuple {
    return {join_value->values_, schema};
  }

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  // added
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::unordered_map<HashJoinKey, std::vector<HashJoinValue>, HashJoinKeyHash> inner_hash_table_{};
  std::vector<Tuple> result_tuple_;
  size_t cursor_{0};
};

}  // namespace bustub
