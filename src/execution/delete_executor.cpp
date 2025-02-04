//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#define LOG_LEVEL LOG_LEVEL_OFF
#include <cstdint>
#include "common/config.h"
#include "common/exception.h"
#include "execution/execution_common.h"

#include <memory>
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  auto table_id = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  auto &table_name = exec_ctx_->GetCatalog()->GetTable(table_id)->name_;
  index_info_vector_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int32_t deleted_tuple_count = 0;
  auto schema = table_info_->schema_;
  // pull tuple until empty
  while (child_executor_->Next(tuple, rid)) {
    LOG_DEBUG("tuple: %s", tuple->ToString(&schema).c_str());
    LOG_DEBUG("rid: %s", tuple->GetRid().ToString().c_str());

    // modified at P4T3.3
    auto txn = exec_ctx_->GetTransaction();
    // auto txn_id = txn->GetTransactionId();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    // auto tmp_ts = txn->GetTransactionTempTs();

    // if tuple is not in table heap, it must be write-write conflict (think carefully!!!)
    if (tuple->GetRid().GetPageId() == INVALID_PAGE_ID) {
      txn->SetTainted();
      throw ExecutionException("[DeleteExecutor] Detect write-write conflict !");
    }

    auto meta_ts = table_info_->table_->GetTupleMeta(*rid).ts_;
    // auto read_ts = txn->GetReadTs();
    // check if tuple is being modified
    if (meta_ts >= TXN_START_ID) {
      if (meta_ts == txn->GetTransactionId()) {
        auto version_link = txn_mgr->GetVersionLink(*rid);
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
              auto old_value = tuple->GetValue(&schema, idx);
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
        // delete old tuple(just set is_deleted to true)
        // tmp_ts = txn->GetTransactionTempTs();
        table_info_->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, *rid);
      }
    } else {
      meta_ts = table_info_->table_->GetTupleMeta(*rid).ts_;
      if (IsWriteWriteConflict(txn, meta_ts)) {
        UnlockVersionLink(txn_mgr, *rid);
        txn->SetTainted();
        throw ExecutionException("[DeleteExecutor] write-write conflict!");
      }
      // check version link exists or not
      bool is_locked = LockVersionLink(txn_mgr, *rid);
      if (!is_locked) {
        txn->SetTainted();
        throw ExecutionException("[DeleteExecutor] other threads are using version link !");
      }
      meta_ts = table_info_->table_->GetTupleMeta(*rid).ts_;
      if (IsWriteWriteConflict(txn, meta_ts)) {
        UnlockVersionLink(txn_mgr, *rid);
        txn->SetTainted();
        throw ExecutionException("[DeleteExecutor] write-write conflict!");
      }

      // insert one log with full columns into link
      std::vector<bool> modified_fields;
      auto column_count = table_info_->schema_.GetColumnCount();
      for (uint32_t i = 0; i < column_count; ++i) {
        modified_fields.emplace_back(true);
      }
      meta_ts = table_info_->table_->GetTupleMeta(*rid).ts_;
      Tuple re_tuple = table_info_->table_->GetTuple(*rid).second;
      auto new_undo_log = UndoLog{false, modified_fields, re_tuple, meta_ts, UndoLink{}};
      // if (txn_mgr->GetUndoLink(*rid).has_value()) {
      //   new_undo_log.prev_version_ = txn_mgr->GetUndoLink(*rid).value();
      // }
      std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(*rid);
      if (version_link.has_value()) {
        auto prev_link = version_link->prev_;
        new_undo_log.prev_version_ = prev_link;
      }
      std::optional<UndoLink> new_undo_link = txn->AppendUndoLog(new_undo_log);
      auto new_version_link = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
      new_version_link->in_progress_ = true;
      txn_mgr->UpdateVersionLink(*rid, new_version_link);
      // tmp_ts = txn->GetTransactionTempTs();
      table_info_->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, *rid);
      UnlockVersionLink(txn_mgr, *rid);
    }
    txn->AppendWriteSet(table_info_->oid_, *rid);

    ++deleted_tuple_count;
    LOG_DEBUG("index_info size: %zu", index_info_vector_.size());
    // after P4T4.2(included), we don't need to delete index.

    // update all index for current tuple
    // for (auto index_info : index_info_vector_) {
    //   auto key_schema = index_info->key_schema_;
    //   auto key_attrs = index_info->index_->GetKeyAttrs();
    //   // delete index
    //   index_info->index_->DeleteEntry(tuple->KeyFromTuple(schema, key_schema, key_attrs), *rid, nullptr);
    // }
  }
  if (!is_deleted_) {
    is_deleted_ = true;
    // return count tuple
    auto integer_value = ValueFactory::GetIntegerValue(deleted_tuple_count);
    auto value_vector = std::vector<Value>{integer_value};
    *tuple = Tuple(value_vector, &GetOutputSchema());
    return true;
  }
  return false;
}

}  // namespace bustub
