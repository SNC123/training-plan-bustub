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
#include "common/logger.h"
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
  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn->read_ts_.store(last_commit_ts_.load());
  txn->state_ = TransactionState::RUNNING;
  running_txns_.AddTxn(txn_ref->read_ts_);
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!

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

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = last_commit_ts_+1;
  auto write_set = txn->GetWriteSets();
  // update tuple ts (tmp_ts -> commit_ts)
  for(const auto& ws : write_set){
    auto table_id = ws.first;
    auto talbe_info = catalog_->GetTable(table_id);
    for(auto rid : ws.second){
      auto tuple_meta = talbe_info->table_->GetTupleMeta(rid);
      tuple_meta.ts_ = txn->commit_ts_;
      talbe_info->table_->UpdateTupleMeta(tuple_meta, rid);
    }
  }
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  // added at P4T3.2, why update in the end???
  ++last_commit_ts_;
  // WE CAN'T DO THAT, BACAUSE OF RESOURCE RELEASE AND LATER REFERENCE
  // txn_id is unique, so it's right to GC later?
  // txn_map_.erase(txn->txn_id_);

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
  // txn_map_.erase(txn->txn_id_);
}

void TransactionManager::GarbageCollection() { UNIMPLEMENTED("not implemented"); }

}  // namespace bustub
