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

#include <iostream>
#include <cassert>
#include <fstream>
#include <climits>
#include <cassert>
#include "rocksdb/db.h"
#include <iomanip>
#include <cstdint>

namespace nuraft {

void printStringAsHex(const std::string& str) {
    for (char c : str) {
        // Cast char to int to ensure correct hexadecimal representation
        // Use std::hex to print in hexadecimal
        // Use std::setw(2) and std::setfill('0') for two-digit, zero-padded hex
        std::cout << std::hex << std::setw(2) << std::setfill('0') 
                  << static_cast<int>(static_cast<unsigned char>(c)) << " ";
    }
    std::cout << std::endl;
}

void nl_log_store::write_log_entry_string(std::string key, ptr<log_entry> entry) {
    std::cout << "writing " + key << std::endl;
    ptr<buffer> s_entry = entry->serialize();
    std::string str(reinterpret_cast<char*>(s_entry->data_begin()), s_entry->size());
    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    rocksdb::Status status = rocksdb_log_->Put(rocksdb::WriteOptions(), key, str);
    assert(status.ok());
    rocksdb_keys_.insert(std::stoull(key));
}

void nl_log_store::write_log_entry(ulong key, ptr<log_entry> entry) {
    std::cout << "Entering function: write_log_entry" << std::endl;
    write_log_entry_string(std::to_string(key), entry);
}

rocksdb::Status nl_log_store::read_log_entry_string(std::string key, ptr<log_entry> *entry) const {
    std::cout << "reading " + key << std::endl;
    std::string value;
    rocksdb::ReadOptions read_options = rocksdb::ReadOptions();
    // value will contain log entry, serialized
    rocksdb::Status status = 
    rocksdb_log_->Get(rocksdb::ReadOptions(), key, &value);
    if (!status.ok()) {
        rocksdb_log_->Get(rocksdb::ReadOptions(), "0", &value);
    }
    ptr<buffer> buf = buffer::alloc(value.size());
    std::memcpy(buf->data_begin(), value.data(), value.size());
    *entry = log_entry::deserialize(*buf);
    std::cout << "reading " + key << "ends" << std::endl;
    return status;
}

rocksdb::Status nl_log_store::read_log_entry(ulong key, ptr<log_entry> *entry) const {
    std::cout << "Entering function: read_log_entry" << std::endl;
    return read_log_entry_string(std::to_string(key), entry);
}

nl_log_store::nl_log_store(int srv_id)
    : start_idx_(1)
    , raft_server_bwd_pointer_(nullptr)
{
    std::cout << "Entering function: nl_log_store constructor" << std::endl;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status =
        rocksdb::DB::Open(options, "./logs" + std::to_string(srv_id), &rocksdb_log_);
    assert(status.ok());

    // get all keys and print them out while we're at it
    rocksdb::Iterator* it = rocksdb_log_->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string value = it->value().ToString();
    std::string key = it->key().ToString();
    std::cout << "key: " << key << std::endl;
    rocksdb_keys_.insert(std::stoull(key));
    printStringAsHex(value);

    ptr<log_entry> log;
    read_log_entry_string(key, &log);
    std::cout << "Log entry:" << log->get_term() << " size: " << log->get_buf().size() << std::endl;
   
    }
    delete it;

    std::cout << "Exact number of keys: " << rocksdb_keys_.size() << std::endl;
    

    ptr<buffer> buf = buffer::alloc(sz_ulong);
    buf->put(0UL);
    // make a dummy entry
    ptr<log_entry> dummy =  cs_new<log_entry>(0, buf);
    write_log_entry_string("0", dummy);
    
    std::cout << "Completed constructor" << std::endl;
}

nl_log_store::~nl_log_store() {
    std::cout << "Entering function: nl_log_store destructor" << std::endl;
    // rocksdb_log_->Flush(rocksdb::FlushOptions());
    // rocksdb_log_->SyncWAL();
    delete rocksdb_log_;
}

ptr<log_entry> nl_log_store::make_clone(const ptr<log_entry>& entry) {
    std::cout << "Entering function: make_clone" << std::endl;
    // NOTE:
    //   Timestamp is used only when `replicate_log_timestamp_` option is on.
    //   Otherwise, log store does not need to store or load it.
    ptr<log_entry> clone = cs_new<log_entry>
                           ( entry->get_term(),
                             buffer::clone( entry->get_buf() ),
                             entry->get_val_type(),
                             entry->get_timestamp(),
                             entry->has_crc32(),
                             entry->get_crc32(),
                             false );
    std::cout << "Leaving function: make_clone" << std::endl;
    return clone;
}

ulong nl_log_store::next_slot() const {
    std::cout << "Entering function: next_slot" << std::endl;
    std::lock_guard<std::mutex> l(log_lock_);
    // Exclude the dummy entry.
    return start_idx_ + rocksdb_keys_.size() - 1;
}

ulong nl_log_store::start_index() const {
    std::cout << "Entering function: start_index" << std::endl;
    return start_idx_;
}

ptr<log_entry> nl_log_store::last_entry() const {
    std::cout << "Entering function: last_entry" << std::endl;
    ptr<log_entry> entry;
    ulong next_idx = next_slot();
    std::lock_guard<std::mutex> l(log_lock_);
    // read_log_entry wil return dummy entry if not found
    read_log_entry(next_idx - 1, &entry);

    return make_clone(entry);
}

ulong nl_log_store::append(ptr<log_entry>& entry) {
    std::cout << "Entering function: append" << std::endl;
    ptr<log_entry> clone = make_clone(entry);

    std::lock_guard<std::mutex> l(log_lock_);
    size_t idx = start_idx_ + rocksdb_keys_.size() - 1;
    write_log_entry(idx, clone);
    return idx;
}

void nl_log_store::write_at(ulong index, ptr<log_entry>& entry) {
    std::cout << "Entering function: write_at" << std::endl;
    ptr<log_entry> clone = make_clone(entry);

    // Discard all logs equal to or greater than `index.
    std::lock_guard<std::mutex> l(log_lock_);
    auto it = rocksdb_keys_.lower_bound(index);
    while (it != rocksdb_keys_.end()) {
        assert(rocksdb_log_->Delete(rocksdb::WriteOptions(), std::to_string(*it)).ok());
        it = rocksdb_keys_.erase(it);
    }
    write_log_entry(index, clone);

}

ptr< std::vector< ptr<log_entry> > >
    nl_log_store::log_entries(ulong start, ulong end)
{
    std::cout << "Entering function: log_entries" << std::endl;
    ptr< std::vector< ptr<log_entry> > > ret =
        cs_new< std::vector< ptr<log_entry> > >();

    ret->resize(end - start);
    ulong cc=0;
    for (ulong ii = start ; ii < end ; ++ii) {
        ptr<log_entry> src = nullptr;
        {   std::lock_guard<std::mutex> l(log_lock_);
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
    nl_log_store::log_entries_ext(ulong start,
                                     ulong end,
                                     int64 batch_size_hint_in_bytes)
{
    std::cout << "Entering function: log_entries_ext" << std::endl;
    ptr< std::vector< ptr<log_entry> > > ret =
        cs_new< std::vector< ptr<log_entry> > >();

    if (batch_size_hint_in_bytes < 0) {
        return ret;
    }

    size_t accum_size = 0;
    for (ulong ii = start ; ii < end ; ++ii) {
        ptr<log_entry> src = nullptr;
        {   std::lock_guard<std::mutex> l(log_lock_);
            // if we have to return the dummy entry something went wrong
            if (!read_log_entry(ii, &src).ok()) {
                assert(0);
            }
        }
        ret->push_back(make_clone(src));
        accum_size += src->get_buf().size();
        if (batch_size_hint_in_bytes &&
            accum_size >= (ulong)batch_size_hint_in_bytes) break;
    }
    return ret;
}

ptr<log_entry> nl_log_store::entry_at(ulong index) {
    std::cout << "Entering function: entry_at" << std::endl;
    ptr<log_entry> src = nullptr;
    {   std::lock_guard<std::mutex> l(log_lock_);
        read_log_entry(index, &src);
    }
    return make_clone(src);
}

ulong nl_log_store::term_at(ulong index) {
    std::cout << "Entering function: term_at" << std::endl;
    ulong term = 0;
    ptr<log_entry> src = nullptr;
    {   std::lock_guard<std::mutex> l(log_lock_);
        read_log_entry(index, &src);
        term = src->get_term();
    }
    return term;
}

ptr<buffer> nl_log_store::pack(ulong index, int32 cnt) {
    std::cout << "Entering function: pack" << std::endl;
    std::vector< ptr<buffer> > logs;

    size_t size_total = 0;
    for (ulong ii=index; ii<index+cnt; ++ii) {
        ptr<log_entry> le = nullptr;
        {   std::lock_guard<std::mutex> l(log_lock_);
             rocksdb::Status s = read_log_entry(ii, &le);
             assert(s.ok());
        }
        // i think this assert checks that we are not with a dummy pointer
        assert(le.get());
        ptr<buffer> buf = le->serialize();
        size_total += buf->size();
        logs.push_back( buf );
    }

    ptr<buffer> buf_out = buffer::alloc
                          ( sizeof(int32) +
                            cnt * sizeof(int32) +
                            size_total );
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
    std::cout << "Entering function: apply_pack" << std::endl;
    pack.pos(0);
    int32 num_logs = pack.get_int();

    for (int32 ii=0; ii<num_logs; ++ii) {
        ulong cur_idx = index + ii;
        int32 buf_size = pack.get_int();

        ptr<buffer> buf_local = buffer::alloc(buf_size);
        pack.get(buf_local);

        ptr<log_entry> le = log_entry::deserialize(*buf_local);
        {   std::lock_guard<std::mutex> l(log_lock_);
            write_log_entry(cur_idx, le);
        }
    }

    {   std::lock_guard<std::mutex> l(log_lock_);
        auto entry = rocksdb_keys_.upper_bound(0);
        if (entry != rocksdb_keys_.end()) {
            start_idx_ = *entry;
        } else {
            start_idx_ = 1;
        }
    }
}

bool nl_log_store::compact(ulong last_log_index) {
    std::cout << "Entering function: compact" << std::endl;
    std::lock_guard<std::mutex> l(log_lock_);
    for (ulong ii = start_idx_; ii <= last_log_index; ++ii) {
        auto entry = rocksdb_keys_.find(ii);
        if (entry != rocksdb_keys_.end()) {
            // delete and assert it succeeded
            assert(rocksdb_log_->Delete(rocksdb::WriteOptions(), std::to_string(*entry)).ok());
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

bool nl_log_store::flush() {
    std::cout << "Entering function: flush" << std::endl;
    rocksdb_log_->FlushWAL(true);
    return true;
}

void nl_log_store::close() {
    std::cout << "Entering function: close" << std::endl;
}


ulong nl_log_store::last_durable_index() {
    std::cout << "Entering function: last_durable_index" << std::endl;
    uint64_t last_log = next_slot() - 1;

    return last_log;
}


}
