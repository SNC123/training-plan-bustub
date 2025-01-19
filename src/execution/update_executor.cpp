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
// #define LOG_LEVEL LOG_LEVEL_OFF
#include "execution/executors/update_executor.h"
#include <cstdint>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>
#include "binder/tokens.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "container/hash/hash_function.h"
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
  // statistc and schema
  int32_t updated_tuple_count = 0;
  auto schema = table_info_->schema_;
  // txn information
  auto txn = exec_ctx_->GetTransaction();
  auto txn_id = txn->GetTransactionId();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto tmp_ts = txn->GetTransactionTempTs();
  // pk infomation
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
      if (target_expression->children_.size() >= 2) {
        LOG_DEBUG("target expr: %s", target_expression->ToString().c_str());
        is_pk_col_updated = true;
        break;
      }
      ++expr_col_idx;
    }
  }

  child_executor_->Init();
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
      // auto read_ts = txn->GetReadTs();
      // check if tuple is being modified
      if (meta_ts >= TXN_START_ID) {
        if (meta_ts == txn_id) {
          // auto prev_link = txn_mgr->GetUndoLink(old_rid);
          auto version_link = txn_mgr->GetVersionLink(old_rid);
          if (version_link.has_value()) {
            auto header_log = txn_mgr->GetUndoLog(version_link->prev_);
            // get old partial schema
            std::vector<Column> old_partial_columns;
            auto old_column_count = schema.GetColumnCount();
            for (uint32_t idx = 0; idx < old_column_count; idx++) {
              if (header_log.modified_fields_[idx]) {
                old_partial_columns.emplace_back(schema.GetColumn(idx));
              }
            }
            auto old_partial_schema = Schema{old_partial_columns};
            LOG_DEBUG("header log before %s", header_log.tuple_.ToString(&old_partial_schema).c_str());
            // only add new column
            std::vector<bool> modified_fields;
            const auto column_count = schema.GetColumnCount();
            auto old_partial_count = 0;
            std::vector<Value> values;
            std::vector<Column> columns;
            for (uint32_t idx = 0; idx < column_count; ++idx) {
              if (header_log.modified_fields_[idx]) {
                modified_fields.emplace_back(true);
                values.emplace_back(header_log.tuple_.GetValue(&old_partial_schema, old_partial_count));
                columns.emplace_back(old_partial_schema.GetColumn(old_partial_count));
                old_partial_count++;
              } else {
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
            }
            auto partial_schema = Schema{columns};
            auto partial_tuple = Tuple{values, &partial_schema};
            header_log.modified_fields_ = modified_fields;
            header_log.tuple_ = partial_tuple;
            LOG_DEBUG("header log after %s", header_log.tuple_.ToString(&partial_schema).c_str());
            auto header_log_txn = txn_mgr->txn_map_[version_link->prev_.prev_txn_];
            header_log_txn->ModifyUndoLog(version_link->prev_.prev_log_idx_, header_log);
          }
          // update old tuple
          table_info_->table_->UpdateTupleInPlace({tmp_ts, false}, *tuple, old_rid);
        }
      } else {
        // insert one log with partial columns into link
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
        std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(old_rid);
        auto new_undo_log = UndoLog{false, modified_fields, partial_tuple, meta_ts, {}};
        if (version_link.has_value()) {
          auto prev_link = version_link->prev_;
          new_undo_log.prev_version_ = prev_link;
        }
        std::optional<UndoLink> new_undo_link = txn->AppendUndoLog(new_undo_log);
        auto new_version_link = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
        txn_mgr->UpdateVersionLink(old_rid, new_version_link);
        table_info_->table_->UpdateTupleInPlace({tmp_ts, false}, *tuple, old_rid);
      }
      txn->AppendWriteSet(table_info_->oid_, old_rid);

      // update all index for current tuple
      for (auto index_info : index_info_vector_) {
        LOG_DEBUG("index_info: %s", index_info->key_schema_.ToString().c_str());
        auto key_schema = index_info->key_schema_;
        auto key_attrs = index_info->index_->GetKeyAttrs();
        LOG_DEBUG("delete index :");
        // deleted index
        index_info->index_->DeleteEntry(old_tuple.KeyFromTuple(schema, key_schema, key_attrs), old_rid, nullptr);
        // created index
        LOG_DEBUG("create index :");
        bool is_index_created =
            index_info->index_->InsertEntry(tuple->KeyFromTuple(schema, key_schema, key_attrs), *rid, nullptr);
        if (!is_index_created) {
          return false;
        }
      }
      ++updated_tuple_count;
    }
  } else {
    std::cout << "meeting situation of update pk column !" << std::endl;

    std::vector<Tuple> old_tuples{};
    std::vector<Tuple> new_tuples{};
    // Firstly, precheck the conflict of index after update.

    std::vector<Tuple> old_keys{};
    std::vector<Tuple> new_keys{};
    auto key_schema = pk_index_info->key_schema_;
    auto key_attrs = pk_index_info->index_->GetKeyAttrs();
    std::unordered_set<uint64_t> new_keys_hashset{};
    bool is_new_keys_unique = true;
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
      HashFunction<Tuple> hasher;
      auto new_key_hash = hasher.GetHash(new_key);
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
    for (const auto &old_tuple : old_tuples) {
      if (old_tuple.GetRid().GetPageId() == INVALID_PAGE_ID) {
        txn->SetTainted();
        throw ExecutionException("[UpdateExecutor] Detect write-write conflict while deleting !");
      }

      auto old_rid = old_tuple.GetRid();
      auto meta_ts = table_info_->table_->GetTupleMeta(old_rid).ts_;
      if (meta_ts >= TXN_START_ID) {
        if (meta_ts == txn_id) {
          // delete old tuple(just set is_deleted to true)
          table_info_->table_->UpdateTupleMeta({tmp_ts, true}, old_rid);
        }
      } else {
        // insert one log with full columns into link
        std::vector<bool> modified_fields;
        auto column_count = table_info_->schema_.GetColumnCount();
        for (uint32_t i = 0; i < column_count; ++i) {
          modified_fields.emplace_back(true);
        }
        // check version link exists or not
        std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(old_rid);
        auto new_undo_log = UndoLog{false, modified_fields, old_tuple, meta_ts, {}};
        if (version_link.has_value()) {
          auto prev_link = version_link->prev_;
          new_undo_log.prev_version_ = prev_link;
        }

        std::optional<UndoLink> new_undo_link = txn->AppendUndoLog(new_undo_log);
        auto new_version_link = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
        txn_mgr->UpdateVersionLink(old_rid, new_version_link);
        table_info_->table_->UpdateTupleMeta({tmp_ts, true}, old_rid);
      }
      txn->AppendWriteSet(table_info_->oid_, old_rid);
    }
    // Insert new tuple
    for (auto &new_tuple : new_tuples) {
      auto key_schema = pk_index_info->key_schema_;
      auto key_attrs = pk_index_info->index_->GetKeyAttrs();
      auto target_key = new_tuple.KeyFromTuple(schema, key_schema, key_attrs);
      std::vector<RID> found_rids{};
      pk_index_info->index_->ScanKey(target_key, &found_rids, nullptr);
      // check target index exists or not
      if (!found_rids.empty()) {
        RID pk_rid = found_rids[0];
        auto is_tuple_deleted = table_info_->table_->GetTupleMeta(pk_rid).is_deleted_;
        // if there is undeleted tuple in table heap, do abort.
        if (!is_tuple_deleted) {
          txn->SetTainted();
          throw ExecutionException("[UpdateExecutor] mapped tuple has existed while inserting !");
        }
        // deleted tuple -> inserted tuple (in place)
        auto old_meta_ts = table_info_->table_->GetTupleMeta(pk_rid).ts_;
        table_info_->table_->UpdateTupleInPlace({tmp_ts, false}, new_tuple, pk_rid);
        txn->AppendWriteSet(table_info_->oid_, pk_rid);
        // if deleted by self, no log(think carefully!!!)
        // bustub tests fail to detect this.
        if (old_meta_ts == txn->GetTransactionId()) {
          continue;
        }
        // else insert delete undolog
        std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(pk_rid);
        auto column_count = schema.GetColumnCount();
        std::vector<bool> modified_fields(column_count);
        std::vector<Value> values(column_count);
        for (size_t idx = 0; idx < column_count; ++idx) {
          modified_fields[idx] = false;
        }
        auto new_undo_log = UndoLog{true, modified_fields, {}, old_meta_ts, {}};
        if (version_link.has_value()) {
          auto prev_link = version_link->prev_;
          new_undo_log.prev_version_ = prev_link;
        }
        std::optional<UndoLink> new_undo_link = txn->AppendUndoLog(new_undo_log);
        auto new_version_link = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
        txn_mgr->UpdateVersionLink(pk_rid, new_version_link);

        continue;
      }

      auto opt_rid = table_info_->table_->InsertTuple({tmp_ts, false}, new_tuple);
      if (!opt_rid.has_value()) {
        return false;
      }
      auto new_rid = opt_rid.value();
      txn->AppendWriteSet(table_info_->oid_, new_rid);
      // just set check function to 'nullptr' to skip Write-Write conflict detection
      txn_mgr->UpdateVersionLink(new_rid, std::nullopt, nullptr);
      ++updated_tuple_count;

      bool is_index_inserted = pk_index_info->index_->InsertEntry(target_key, new_rid, nullptr);
      if (!is_index_inserted) {
        // detect multiple index map to same tuple
        txn->SetTainted();
        throw ExecutionException(
            "[UpdateExecutor] Write-Write conflict: multiple indexes map to same tuple while inserting!");
        return false;
      }
    }
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
