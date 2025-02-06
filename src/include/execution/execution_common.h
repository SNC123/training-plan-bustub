#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

auto LockVersionLink(TransactionManager *txn_mgr, RID rid) -> bool;

auto UnlockVersionLink(TransactionManager *txn_mgr, RID rid) -> bool;

auto IsWriteWriteConflict(Transaction *txn, timestamp_t meta_ts) -> bool;

auto ModifyHeadUndoLog(Transaction *txn, TransactionManager *txn_mgr, const Schema &schema, RID rid,
                       const Tuple &old_tuple, const Tuple &new_tuple) -> void;

auto CheckConflictAndLockLink(Transaction *txn, TransactionManager *txn_mgr, const TableInfo *table_info, RID rid,
                              std::string location_str) -> void;

auto UpsertTuple(Transaction *txn, TransactionManager *txn_mgr, const TableInfo *table_info,
                 const std::vector<IndexInfo *> &index_info_vector, Tuple &tuple) -> bool;

auto DeleteTuple(Transaction *txn, TransactionManager *txn_mgr, const TableInfo *table_info, RID rid, Tuple &tuple)
    -> void;
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
