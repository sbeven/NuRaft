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
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include <iomanip>
#include <cstdint>

namespace nuraft {

void printStringAsHex(const std::string& str) {
    // save the old state of std::cout before we modify it with options
    std::ios old_state(nullptr);
    old_state.copyfmt(std::cout);

    for (char c : str) {
        // Cast char to int to ensure correct hexadecimal representation
        // Use std::hex to print in hexadecimal
        // Use std::setw(2) and std::setfill('0') for two-digit, zero-padded hex
        std::cout << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(static_cast<unsigned char>(c)) << " ";
    }
    std::cout << std::endl;

    std::cout.copyfmt(old_state);
}

void nl_log_store::write_log_entry_string(std::string key, ptr<log_entry> entry) {
    ptr<buffer> s_entry = entry->serialize();
    std::string str(reinterpret_cast<char*>(s_entry->data_begin()), s_entry->size());
    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    rocksdb::Status status = rocksdb_log_->Put(write_options, key, str);
    assert(status.ok());
    rocksdb_keys_.insert(std::stoull(key));
}

void nl_log_store::write_log_entry(ulong key, ptr<log_entry> entry) {
    write_log_entry_string(std::to_string(key), entry);
}

rocksdb::Status nl_log_store::read_log_entry_string(std::string key, ptr<log_entry> *entry) const {
    std::string value;
    // value will contain log entry, serialized
    rocksdb::Status status = 
    rocksdb_log_->Get(rocksdb::ReadOptions(), key, &value);
      if (!status.ok()) {
        rocksdb_log_->Get(rocksdb::ReadOptions(), "0", &value);
    }
    ptr<buffer> buf = buffer::alloc(value.size());
    std::memcpy(buf->data_begin(), value.data(), value.size());
    *entry = log_entry::deserialize(*buf);
    return status;
}

void nl_log_store::update_start_idx() {
    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    rocksdb::Status status = rocksdb_log_->Put(write_options, "start_idx_", std::to_string(start_idx_));
    assert(status.ok());
}

rocksdb::Status nl_log_store::read_log_entry(ulong key, ptr<log_entry> *entry) const {
    return read_log_entry_string(std::to_string(key), entry);
}

nl_log_store::nl_log_store(int srv_id)
    : start_idx_(1)
{
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
        if (key == "start_idx_") {
            std::cout << "Start index value: " << value << std::endl;
        } else {
            rocksdb_keys_.insert(std::stoull(key));
            printStringAsHex(value);

            ptr<log_entry> log;
            read_log_entry_string(key, &log);
            std::cout << " size: " << log->get_buf().size() << std::endl;
        }
    }
    delete it;

    std::cout << "Exact number of keys: " << rocksdb_keys_.size() << std::endl;

    std::string value;
    status = rocksdb_log_->Get(rocksdb::ReadOptions(), "start_idx_", &value);
    if (status.ok()) {
        start_idx_ = std::stoull(value);
    } else if (status.IsNotFound()) {
        update_start_idx();
    }

    ptr<buffer> buf = buffer::alloc(sz_ulong);
    ulong zeroValue = 0;
    buf->put(zeroValue);
    // make a dummy entry
    ptr<log_entry> dummy =  cs_new<log_entry>(0, buf);
    write_log_entry_string("0", dummy);
}

nl_log_store::~nl_log_store() {
    assert(rocksdb_log_ == nullptr);
}

ptr<log_entry> nl_log_store::make_clone(const ptr<log_entry>& entry) {
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
    return clone;
}

ulong nl_log_store::next_slot() const {
    std::lock_guard<std::mutex> l(log_lock_);
    // Exclude the dummy entry.
    return start_idx_ + rocksdb_keys_.size() - 1;
}

ulong nl_log_store::start_index() const {
    return start_idx_;
}

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
        assert(rocksdb_log_->Delete(rocksdb::WriteOptions(), std::to_string(*it)).ok());
        it = rocksdb_keys_.erase(it);
    }
    write_log_entry(index, clone);

}

ptr< std::vector< ptr<log_entry> > >
    nl_log_store::log_entries(ulong start, ulong end)
{
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
    ptr<log_entry> src = nullptr;
    {   std::lock_guard<std::mutex> l(log_lock_);
        read_log_entry(index, &src);
    }
    return make_clone(src);
}

ulong nl_log_store::term_at(ulong index) {
    ulong term = 0;
    ptr<log_entry> src = nullptr;
    {   std::lock_guard<std::mutex> l(log_lock_);
        read_log_entry(index, &src);
        term = src->get_term();
    }
    return term;
}

ptr<buffer> nl_log_store::pack(ulong index, int32 cnt) {
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
        update_start_idx();
    }
}

bool nl_log_store::compact(ulong last_log_index) {
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
        update_start_idx();
    }
    return true;
}

bool nl_log_store::flush() {
    return true;
}

void nl_log_store::close() {
    if (rocksdb_log_) {
        rocksdb::Status s = rocksdb_log_->Close();
        if (!s.ok()) {
            std::cerr << "Error closing rocksdb log store: " << s.ToString() << std::endl;
        }
        delete rocksdb_log_;
        rocksdb_log_ = nullptr;
    }
}

ulong nl_log_store::last_durable_index() {
    uint64_t last_log = next_slot() - 1;

    return last_log;
}


}
