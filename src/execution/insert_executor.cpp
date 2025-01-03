//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "common/exception.h"
#include "common/rid.h"
#include "type/value_factory.h"

#define LOG_LEVEL LOG_LEVEL_OFF

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  auto &table_id = plan_->table_oid_;
  auto &table_name = exec_ctx_->GetCatalog()->GetTable(table_id)->name_;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  index_info_vector_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int32_t inserted_tuple_count = 0;
  auto schema = table_info_->schema_;
  // pull tuple until empty
  while (child_executor_->Next(tuple, rid)) {
    // modified at P4T4.1
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    auto tmp_ts = txn->GetTransactionTempTs();

    // check index exists or not
    if (index_info_vector_.empty()) {
      auto opt_rid = table_info_->table_->InsertTuple({tmp_ts, false}, *tuple);
      if (!opt_rid.has_value()) {
        return false;
      }
      *rid = opt_rid.value();
      txn->AppendWriteSet(table_info_->oid_, *rid);
      // just set check function to 'nullptr' to skip Write-Write conflict detection
      txn_mgr->UpdateVersionLink(*rid, std::nullopt, nullptr);
      ++inserted_tuple_count;
    } else {
      // update all index for current tuple
      for (auto index_info : index_info_vector_) {
        auto key_schema = index_info->key_schema_;
        auto key_attrs = index_info->index_->GetKeyAttrs();
        auto target_key = tuple->KeyFromTuple(schema, key_schema, key_attrs);
        std::vector<RID> found_rids{};

        if (index_info->is_primary_key_) {
          index_info->index_->ScanKey(target_key, &found_rids, nullptr);
          // check target index exists or not
          if (!found_rids.empty()) {
            RID pk_rid = found_rids[0];
            auto is_tuple_deleted = table_info_->table_->GetTupleMeta(pk_rid).is_deleted_;
            // if there is undeleted tuple in table heap, do abort.
            if (!is_tuple_deleted) {
              txn->SetTainted();
              throw ExecutionException("[InsertExecutor] mapped tuple has existed!");
            }
            // deleted tuple -> inserted tuple (in place)
            auto old_meta_ts = table_info_->table_->GetTupleMeta(pk_rid).ts_;
            table_info_->table_->UpdateTupleInPlace({tmp_ts, false}, *tuple, pk_rid);
            // insert delete undolog
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
            txn->AppendWriteSet(table_info_->oid_, pk_rid);
            continue;
          }
        }

        auto opt_rid = table_info_->table_->InsertTuple({tmp_ts, false}, *tuple);
        if (!opt_rid.has_value()) {
          return false;
        }
        *rid = opt_rid.value();
        txn->AppendWriteSet(table_info_->oid_, *rid);
        // just set check function to 'nullptr' to skip Write-Write conflict detection
        txn_mgr->UpdateVersionLink(*rid, std::nullopt, nullptr);
        ++inserted_tuple_count;

        bool is_index_inserted = index_info->index_->InsertEntry(target_key, *rid, nullptr);
        if (!is_index_inserted) {
          // detect multiple index map to same tuple
          txn->SetTainted();
          throw ExecutionException("[InsertExecutor] Write-Write conflict: multiple indexes map to same tuple!");
          return false;
        }
      }
    }
    // insert current tuple
  }
  if (!is_inserted_) {
    is_inserted_ = true;
    // return count tuple
    auto integer_value = ValueFactory::GetIntegerValue(inserted_tuple_count);
    auto value_vector = std::vector<Value>{integer_value};
    *tuple = Tuple(value_vector, &GetOutputSchema());
    return true;
  }
  return false;
}

}  // namespace bustub
