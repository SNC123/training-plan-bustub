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
#include <cstdio>
#include "fmt/core.h"
#define LOG_LEVEL LOG_LEVEL_OFF
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "binder/tokens.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "container/hash/hash_function.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/table/tuple.h"
#include "type/type.h"
namespace bustub {

/*
  Help Function: Check if Tuple A and Tuple B are equal (same schema).
*/
auto TupleEqual(const Schema *schema, const Tuple &a, const Tuple &b) -> bool {
  auto column_count = schema->GetColumnCount();
  for (uint32_t idx = 0; idx < column_count; ++idx) {
    auto a_val = a.GetValue(schema, idx);
    auto b_val = b.GetValue(schema, idx);
    if (a_val.CompareEquals(b_val) != CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  auto table_id = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  auto &table_name = exec_ctx_->GetCatalog()->GetTable(table_id)->name_;
  index_info_vector_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // statistics and schema information
  int32_t updated_tuple_count = 0;
  auto schema = table_info_->schema_;
  // txn information
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  // primary key(pk) infomation
  bool is_pk_col_updated = false;
  int32_t pk_col_idx = -1;
  IndexInfo *pk_index_info{};

  // get pk column idx
  for (auto index_info : index_info_vector_) {
    auto idxes = index_info->index_->GetKeyAttrs();
    if (index_info->is_primary_key_ && idxes.size() == 1) {
      pk_col_idx = idxes[0];
      LOG_DEBUG("pk col idx: %d", pk_col_idx);
      pk_index_info = index_info;
    }
  }
  // check pk column is updated or not
  if (pk_col_idx != -1) {
    int32_t expr_col_idx = 0;
    for (const auto &target_expression : plan_->target_expressions_) {
      if (expr_col_idx != pk_col_idx) {
        ++expr_col_idx;
        continue;
      }
      std::string target_expr = target_expression->ToString();
      std::string unupdated_target_expr = "#0." + std::to_string(pk_col_idx);
      LOG_DEBUG("target expr: %s, unupdated_expr:%s ", target_expr.c_str(), unupdated_target_expr.c_str());
      if (target_expr != unupdated_target_expr) {
        is_pk_col_updated = true;
        break;
      }
      ++expr_col_idx;
    }
  }
  /*
    For primary key column update: check
  */
  if (!is_pk_col_updated) {
    // update non-primary key column(before P4T4.3)
    while (child_executor_->Next(tuple, rid)) {
      LOG_DEBUG("tuple: %s", tuple->ToString(&schema).c_str());

      auto old_rid = *rid;
      auto old_tuple = *tuple;
      // create new tuple
      std::vector<Value> values{};
      values.reserve(schema.GetColumnCount());
      for (const auto &target_expression : plan_->target_expressions_) {
        auto value = target_expression->Evaluate(tuple, schema);
        values.push_back(value);
      }
      *tuple = Tuple(values, &schema);
      LOG_DEBUG("new tuple: %s", tuple->ToString(&schema).c_str());

      // if tuple is not in table heap, it must be write-write conflict (think carefully!!!)
      if (old_tuple.GetRid().GetPageId() == INVALID_PAGE_ID) {
        txn->SetTainted();
        throw ExecutionException("[UpdateExecutor] Detect write-write conflict!");
      }

      auto meta_ts = table_info_->table_->GetTupleMeta(old_rid).ts_;
      // check if tuple is being modified
      if (meta_ts == txn->GetTransactionId()) {
        // maintain undo link
        ModifyHeadUndoLog(txn, txn_mgr, schema, old_rid, old_tuple, *tuple);
        // update old tuple
        table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, *tuple, old_rid);
        txn->AppendWriteSet(table_info_->oid_, old_rid);

      } else {
        CheckConflictAndLockLink(txn, txn_mgr, table_info_, old_rid, "UpdateExecutor");

        // build undo log(partially modified fields)
        old_tuple = table_info_->table_->GetTuple(old_rid).second;
        std::vector<bool> modified_fields;
        auto column_count = table_info_->schema_.GetColumnCount();
        std::vector<Value> values;
        std::vector<Column> columns;
        for (uint32_t idx = 0; idx < column_count; ++idx) {
          auto old_value = old_tuple.GetValue(&schema, idx);
          auto new_value = tuple->GetValue(&schema, idx);
          if (!old_value.CompareExactlyEquals(new_value)) {
            modified_fields.emplace_back(true);
            values.emplace_back(old_value);
            columns.emplace_back(schema.GetColumn(idx));
          } else {
            modified_fields.emplace_back(false);
          }
        }
        auto partial_schema = Schema{columns};
        auto partial_tuple = Tuple{values, &partial_schema};
        meta_ts = table_info_->table_->GetTupleMeta(old_rid).ts_;
        auto new_undo_log = UndoLog{false, modified_fields, partial_tuple, meta_ts, {}};

        // build and update version link
        std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(old_rid);
        if (version_link.has_value()) {
          auto prev_link = version_link->prev_;
          new_undo_log.prev_version_ = prev_link;
        }
        std::optional<UndoLink> new_undo_link = txn->AppendUndoLog(new_undo_log);
        auto new_version_link = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
        new_version_link->in_progress_ = true;
        txn_mgr->UpdateVersionLink(old_rid, new_version_link);
        table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, *tuple, old_rid);
        txn->AppendWriteSet(table_info_->oid_, old_rid);

        UnlockVersionLink(txn_mgr, old_rid);
      }
      // txn->AppendWriteSet(table_info_->oid_, old_rid);

      ++updated_tuple_count;
    }
  } else {
    std::cout << "meeting situation of update pk column !" << std::endl;

    std::vector<Tuple> old_tuples{};
    std::vector<Tuple> new_tuples{};
    std::vector<Tuple> old_keys{};
    std::vector<Tuple> new_keys{};
    auto key_schema = pk_index_info->key_schema_;
    auto key_attrs = pk_index_info->index_->GetKeyAttrs();
    std::unordered_set<uint64_t> new_keys_hashset{};
    bool is_new_keys_unique = true;
    // Firstly, precheck the conflict of index after update.
    // Iterate to get all old and new primary keys
    while (child_executor_->Next(tuple, rid)) {
      auto old_tuple = *tuple;
      old_tuples.emplace_back(old_tuple);
      auto old_key = old_tuple.KeyFromTuple(schema, key_schema, key_attrs);
      LOG_DEBUG("old key: %s", old_key.ToString(&key_schema).c_str());
      old_keys.emplace_back(old_key);
      // create new tuple
      std::vector<Value> values{};
      values.reserve(schema.GetColumnCount());
      for (const auto &target_expression : plan_->target_expressions_) {
        auto value = target_expression->Evaluate(tuple, schema);
        values.push_back(value);
      }
      auto new_tuple = Tuple(values, &schema);
      new_tuples.emplace_back(new_tuple);
      auto new_key = new_tuple.KeyFromTuple(schema, key_schema, key_attrs);
      LOG_DEBUG("new key: %s", new_key.ToString(&key_schema).c_str());
      new_keys.emplace_back(new_key);
      HashFunction<std::string> hasher;
      auto new_key_hash = hasher.GetHash(new_key.ToString(&key_schema));
      LOG_DEBUG("new key hash: %ld", new_key_hash);
      if (new_keys_hashset.find(new_key_hash) != new_keys_hashset.end()) {
        is_new_keys_unique = false;
      } else {
        new_keys_hashset.insert(new_key_hash);
      }
    }
    // Avoid new keys duplicated
    if (!is_new_keys_unique) {
      txn->SetTainted();
      throw ExecutionException("[UpdateExecutor] New keys are duplicated!");
    }
    // Check conflict of index
    for (const auto &new_key : new_keys) {
      std::vector<RID> found_rids{};
      pk_index_info->index_->ScanKey(new_key, &found_rids, nullptr);
      // new key meets undeleted key, so conflict
      if (!found_rids.empty()) {
        bool is_key_deleted = false;
        // check key deleted while updating
        for (const auto &old_key : old_keys) {
          if (TupleEqual(&key_schema, new_key, old_key)) {
            is_key_deleted = true;
            break;
          }
        }
        // check key deleted before
        RID found_rid = found_rids[0];
        if (table_info_->table_->GetTupleMeta(found_rid).is_deleted_) {
          is_key_deleted = true;
        }
        if (!is_key_deleted) {
          LOG_DEBUG("conflict new key: %s", new_key.ToString(&key_schema).c_str());
          txn->SetTainted();
          throw ExecutionException("[UpdateExecutor] Detect primary key index update conflict!");
        }
      }
    }

    // Then, delete and insert respectively
    // Delete old tuple
    for (auto &old_tuple : old_tuples) {
      DeleteTuple(txn, txn_mgr, table_info_, old_tuple.GetRid(), old_tuple);
    }
    // Insert new tuple
    for (auto &new_tuple : new_tuples) {
      UpsertTuple(txn, txn_mgr, table_info_, index_info_vector_, new_tuple);
    }
    updated_tuple_count += old_tuples.size();
  }

  if (!is_updated_) {
    is_updated_ = true;
    // return count tuple
    auto integer_value = ValueFactory::GetIntegerValue(updated_tuple_count);
    auto value_vector = std::vector<Value>{integer_value};
    *tuple = Tuple(value_vector, &GetOutputSchema());
    return true;
  }
  return false;
}

}  // namespace bustub
