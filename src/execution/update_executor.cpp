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
#define LOG_LEVEL LOG_LEVEL_OFF
#include "execution/executors/update_executor.h"
#include <cstdint>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
namespace bustub {

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
  int32_t updated_tuple_count = 0;
  auto schema = table_info_->schema_;
  // pull tuple until empty
  while (child_executor_->Next(tuple, rid)) {
    LOG_DEBUG("tuple: %s", tuple->ToString(&schema).c_str());

    // modified at P4T3.3
    auto txn = exec_ctx_->GetTransaction();
    auto txn_id = txn->GetTransactionId();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    auto tmp_ts = txn->GetTransactionTempTs();
    // delete old tuple(just set is_deleted to true)
    auto old_rid = *rid;
    auto old_tuple = *tuple;
    // LOG_DEBUG("delete tuple: %s", old_tuple.ToString(&schema).c_str());
    // table_info_->table_->UpdateTupleMeta({tmp_ts, true}, old_rid);
    // LOG_DEBUG("delete rid: %s", rid->ToString().c_str());
    // create new tuple
    std::vector<Value> values{};
    values.reserve(schema.GetColumnCount());
    for (const auto &target_expression : plan_->target_expressions_) {
      auto value = target_expression->Evaluate(tuple, schema);
      values.push_back(value);
    }
    *tuple = Tuple(values, &schema);
    LOG_DEBUG("new tuple: %s", tuple->ToString(&schema).c_str());
    // insert new tuple
    // auto opt_rid = table_info_->table_->InsertTuple({tmp_ts, false}, *tuple);
    // if (!opt_rid.has_value()) {
    //   return false;
    // }
    // *rid = opt_rid.value();
    // LOG_DEBUG("created rid: %s", rid->ToString().c_str());
    // ++updated_tuple_count;
    // LOG_DEBUG("index_info size: %zu", index_info_vector_.size());

    // if tuple is not in table heap, it must be write-write conflict (think carefully!!!)
    if (old_tuple.GetRid().GetPageId() == INVALID_PAGE_ID) {
      txn->SetTainted();
      throw ExecutionException("Detect write-write conflict !");
    }

    auto meta_ts = table_info_->table_->GetTupleMeta(old_rid).ts_;
    // auto read_ts = txn->GetReadTs();
    // check if tuple is being modified
    if (meta_ts >= TXN_START_ID) {
      if (meta_ts == txn_id) {
        auto prev_link = txn_mgr->GetUndoLink(old_rid);
        if (prev_link.has_value()) {
          auto header_log = txn_mgr->GetUndoLog(prev_link.value());
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
          txn->ModifyUndoLog(prev_link->prev_log_idx_, header_log);
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
      auto prev_link = txn_mgr->GetUndoLink(old_rid);
      auto new_undo_log = UndoLog{false, modified_fields, partial_tuple, meta_ts, {}};
      if (prev_link.has_value()) {
        new_undo_log.prev_version_ = prev_link.value();
      }
      auto new_undo_link = txn->AppendUndoLog(new_undo_log);
      txn_mgr->UpdateUndoLink(old_rid, new_undo_link);
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
