/************************************************************************
Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "nl_log_store.hxx"

#include "nuraft.hxx"

#include <cassert>
#include <climits>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

namespace nuraft {

using namespace ROCKSDB_NAMESPACE;

void printStringAsHex(const std::string& str) {
    for (char c: str) {
        // Cast char to int to ensure correct hexadecimal representation
        // Use std::hex to print in hexadecimal
        // Use std::setw(2) and std::setfill('0') for two-digit, zero-padded hex
        std::cout << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(static_cast<unsigned char>(c)) << " ";
    }
    std::cout << std::endl;
}

void nl_log_store::InitDBGeneralOptions() {
    // open DB
    options_.create_if_missing = true;
    options_.statistics = CreateDBStatistics();
    options_.write_buffer_size = 4 * 1024 * 1024;        // 4MB
    options_.max_bytes_for_level_base = 8 * 1024 * 1024; // 8MB
    options_.max_bytes_for_level_multiplier = 2;
    options_.level0_file_num_compaction_trigger = 2;
    options_.level0_slowdown_writes_trigger = 3;
    options_.level0_stop_writes_trigger = 4;
    options_.compression = kNoCompression;

    default_txn_options_ = TransactionOptions();
    default_txn_options_.set_snapshot = true;

    default_read_options_ = ReadOptions();
    default_read_options_.io_activity = Env::IOActivity::kGet;

    default_write_options_ = WriteOptions();

    // Bloom filter: 10 bits per key
#if 0
    BlockBasedTableOptions* table_options =
        reinterpret_cast<BlockBasedTableOptions*>(options_.table_factory->GetOptions());
    table_options->filter_policy.reset(NewBloomFilterPolicy(10, false));
#endif
}

void nl_log_store::InitDB(const std::string& db_path) {
    InitDBGeneralOptions();

    Status s = TransactionDB::Open(options_, txn_db_options_, db_path, &txn_db_);
    assert(s.ok());

    db_ = txn_db_->GetBaseDB();
    assert(db_ != nullptr);
}

void nl_log_store::write_log_entry(Slice key, ptr<log_entry> entry) {
    ptr<buffer> s_entry = entry->serialize();
    std::string entry_str(reinterpret_cast<char*>(s_entry->data_begin()),
                          s_entry->size());

    Transaction* txn1 =
        txn_db_->BeginTransaction(default_write_options_, default_txn_options_);
    Slice value(entry_str);
    rocksdb::Status status = txn1->Put(key, value);
    assert(status.ok());
    txn1->Commit();
    delete txn1;

    Transaction* txn2 =
        txn_db_->BeginTransaction(default_write_options_, default_txn_options_);
    std::string test_value;
    status = txn2->Get(default_read_options_, key, &test_value);
    assert(status.ok());
    txn2->Commit();
    delete txn2;

    assert(value.ToString() == test_value);
    std::cout << "Value: " << value.ToString() << std::endl;

    rocksdb_keys_.insert(std::stoull(key.ToString()));
}

void nl_log_store::write_log_entry(ulong key, ptr<log_entry> entry) {
    Slice k(std::to_string(key));
    write_log_entry(k, entry);
}

rocksdb::Status nl_log_store::read_log_entry(Slice key, ptr<log_entry>* entry) const {
    // value will contain log entry, serialized
    std::string value;

    Transaction* txn =
        txn_db_->BeginTransaction(default_write_options_, default_txn_options_);
    Status s = txn->Get(default_read_options_, key, &value);
    if (s.IsNotFound()) {
        txn->Get(default_read_options_, "0", &value);
    }
    txn->Commit();
    delete txn;

    ptr<buffer> buf = buffer::alloc(value.size());
    std::memcpy(buf->data_begin(), value.data(), value.size());
    *entry = log_entry::deserialize(*buf);
    return status;
}

rocksdb::Status nl_log_store::read_log_entry(ulong key, ptr<log_entry>* entry) const {
    Slice k(std::to_string(key));
    return read_log_entry(k, entry);
}

nl_log_store::nl_log_store(int srv_id)
    : start_idx_(1)
    , raft_server_bwd_pointer_(nullptr) {

    // Initializes a transactional RocksDB instance
    InitDB("./logs" + std::to_string(srv_id));

#if 0
    Status s;
    std::string test_val;
    s = db_->Get(rocksdb::ReadOptions(), "0", &test_val);
    if (s.ok()) {
        std::cout << "Found value for key 0: " << test_val << std::endl;
    } else if (s.IsNotFound()) {
        std::cout << "Key 0 not found, creating dummy entry." << std::endl;
        db_->Put(rocksdb::WriteOptions(), "0", "dummy");
        db_->Get(rocksdb::ReadOptions(), "0", &test_val);
        std::cout << "Created dummy entry for key 0: " << test_val << std::endl;
    } else {
        // Should not reach here
        assert(false);
    }

    delete txn_db_;
    exit(1);
#endif

    // get all keys and print them out while we're at it
    Transaction* txn =
        txn_db_->BeginTransaction(default_write_options_, default_txn_options_);
    rocksdb::Iterator* it = txn->GetIterator(default_read_options_);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        Slice key = it->key();
        Slice value = it->value();

        std::cout << "key: " << key.ToString() << std::endl;
        rocksdb_keys_.insert(std::stoull(key.ToString()));
        printStringAsHex(value.ToString());

        ptr<log_entry> log;
        read_log_entry(key, &log);
        std::cout << "Log entry:" << log->get_term() << " size: " << log->get_buf().size()
                  << std::endl;
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;
    txn->Commit();
    delete txn;

    std::cout << "Exact number of keys: " << rocksdb_keys_.size() << std::endl;

    Slice key0("0");

    // Make a dummy entry
    ptr<buffer> buf = buffer::alloc(sz_ulong);
    ptr<log_entry> dummy = cs_new<log_entry>(0, buf);

    write_log_entry(key0, dummy);
}

nl_log_store::~nl_log_store() { delete txn_db_; }

ptr<log_entry> nl_log_store::make_clone(const ptr<log_entry>& entry) {
    // NOTE:
    //   Timestamp is used only when `replicate_log_timestamp_` option is on.
    //   Otherwise, log store does not need to store or load it.
    ptr<log_entry> clone = cs_new<log_entry>(entry->get_term(),
                                             buffer::clone(entry->get_buf()),
                                             entry->get_val_type(),
                                             entry->get_timestamp(),
                                             entry->has_crc32(),
                                             entry->get_crc32(),
                                             false);
    return clone;
}

ulong nl_log_store::next_slot() const {
    std::lock_guard<std::mutex> l(log_lock_);
    // Exclude the dummy entry.
    return start_idx_ + rocksdb_keys_.size() - 1;
}

ulong nl_log_store::start_index() const { return start_idx_; }

ptr<log_entry> nl_log_store::last_entry() const {
    ptr<log_entry> entry;
    ulong next_idx = next_slot();
    std::lock_guard<std::mutex> l(log_lock_);
    // read_log_entry wil return dummy entry if not found
    read_log_entry(next_idx - 1, &entry);

    return make_clone(entry);
}

ulong nl_log_store::append(ptr<log_entry>& entry) {
    ptr<log_entry> clone = make_clone(entry);

    std::lock_guard<std::mutex> l(log_lock_);
    size_t idx = start_idx_ + rocksdb_keys_.size() - 1;
    write_log_entry(idx, clone);

    return idx;
}

void nl_log_store::write_at(ulong index, ptr<log_entry>& entry) {
    ptr<log_entry> clone = make_clone(entry);

    // Discard all logs equal to or greater than `index.
    std::lock_guard<std::mutex> l(log_lock_);
    auto it = rocksdb_keys_.lower_bound(index);
    while (it != rocksdb_keys_.end()) {
        Status s = db_->Delete(rocksdb::WriteOptions(), std::to_string(*it));
        assert(s.ok());
        it = rocksdb_keys_.erase(it);
    }
    write_log_entry(index, clone);
}

ptr<std::vector<ptr<log_entry>>> nl_log_store::log_entries(ulong start, ulong end) {
    ptr<std::vector<ptr<log_entry>>> ret = cs_new<std::vector<ptr<log_entry>>>();

    ret->resize(end - start);
    ulong cc = 0;
    for (ulong ii = start; ii < end; ++ii) {
        ptr<log_entry> src = nullptr;
        {
            std::lock_guard<std::mutex> l(log_lock_);
            // if we have to return the dummy entry something went wrong
            if (!read_log_entry(ii, &src).ok()) {
                assert(0);
            }
        }
        (*ret)[cc++] = make_clone(src);
    }
    return ret;
}

ptr<std::vector<ptr<log_entry>>>
nl_log_store::log_entries_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes) {
    ptr<std::vector<ptr<log_entry>>> ret = cs_new<std::vector<ptr<log_entry>>>();

    if (batch_size_hint_in_bytes < 0) {
        return ret;
    }

    size_t accum_size = 0;
    for (ulong ii = start; ii < end; ++ii) {
        ptr<log_entry> src = nullptr;
        {
            std::lock_guard<std::mutex> l(log_lock_);
            // if we have to return the dummy entry something went wrong
            if (!read_log_entry(ii, &src).ok()) {
                assert(0);
            }
        }
        ret->push_back(make_clone(src));
        accum_size += src->get_buf().size();
        if (batch_size_hint_in_bytes && accum_size >= (ulong)batch_size_hint_in_bytes)
            break;
    }
    return ret;
}

ptr<log_entry> nl_log_store::entry_at(ulong index) {
    ptr<log_entry> src = nullptr;
    {
        std::lock_guard<std::mutex> l(log_lock_);
        read_log_entry(index, &src);
    }
    return make_clone(src);
}

ulong nl_log_store::term_at(ulong index) {
    ulong term = 0;
    ptr<log_entry> src = nullptr;
    {
        std::lock_guard<std::mutex> l(log_lock_);
        read_log_entry(index, &src);
        term = src->get_term();
    }
    return term;
}

ptr<buffer> nl_log_store::pack(ulong index, int32 cnt) {
    std::vector<ptr<buffer>> logs;

    size_t size_total = 0;
    for (ulong ii = index; ii < index + cnt; ++ii) {
        ptr<log_entry> le = nullptr;
        {
            std::lock_guard<std::mutex> l(log_lock_);
            rocksdb::Status s = read_log_entry(ii, &le);
            assert(s.ok());
        }
        // i think this assert checks that we are not with a dummy pointer
        assert(le.get());
        ptr<buffer> buf = le->serialize();
        size_total += buf->size();
        logs.push_back(buf);
    }

    ptr<buffer> buf_out = buffer::alloc(sizeof(int32) + cnt * sizeof(int32) + size_total);
    buf_out->pos(0);
    buf_out->put((int32)cnt);

    for (auto& entry: logs) {
        ptr<buffer>& bb = entry;
        buf_out->put((int32)bb->size());
        buf_out->put(*bb);
    }
    return buf_out;
}

void nl_log_store::apply_pack(ulong index, buffer& pack) {
    pack.pos(0);
    int32 num_logs = pack.get_int();

    for (int32 ii = 0; ii < num_logs; ++ii) {
        ulong cur_idx = index + ii;
        int32 buf_size = pack.get_int();

        ptr<buffer> buf_local = buffer::alloc(buf_size);
        pack.get(buf_local);

        ptr<log_entry> le = log_entry::deserialize(*buf_local);
        {
            std::lock_guard<std::mutex> l(log_lock_);
            write_log_entry(cur_idx, le);
        }
    }

    {
        std::lock_guard<std::mutex> l(log_lock_);
        auto entry = rocksdb_keys_.upper_bound(0);
        if (entry != rocksdb_keys_.end()) {
            start_idx_ = *entry;
        } else {
            start_idx_ = 1;
        }
    }
}

bool nl_log_store::compact(ulong last_log_index) {
    std::lock_guard<std::mutex> l(log_lock_);
    for (ulong ii = start_idx_; ii <= last_log_index; ++ii) {
        auto entry = rocksdb_keys_.find(ii);
        if (entry != rocksdb_keys_.end()) {
            // delete and assert it succeeded
            Status s = db_->Delete(rocksdb::WriteOptions(), std::to_string(*entry));
            assert(s.ok());
            rocksdb_keys_.erase(entry);
        }
    }

    // WARNING:
    //   Even though nothing has been erased,
    //   we should set `start_idx_` to new index.
    if (start_idx_ <= last_log_index) {
        start_idx_ = last_log_index + 1;
    }
    return true;
}

bool nl_log_store::flush() { return true; }

void nl_log_store::close() {}

ulong nl_log_store::last_durable_index() {
    uint64_t last_log = next_slot() - 1;

    return last_log;
}

} // namespace nuraft
