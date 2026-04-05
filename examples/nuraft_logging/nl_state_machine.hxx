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

#pragma once

#include "nuraft.hxx"

#include <atomic>
#include <cassert>
#include <deque>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include <string>

#include "nl_log.hxx"

using namespace nuraft;

class nl_state_machine : public state_machine {
public:
    nl_state_machine()
        : last_committed_idx_(0)
        {}

    ~nl_state_machine() {}

    ptr<buffer> pre_commit(const ulong log_idx, buffer& data) {
        nl_log log_entry = nl_log::deserialize(data);

        // Just print.
        std::cout << "pre_commit " << log_idx << ": "
                  << log_entry.to_string() << std::endl;
        return nullptr;
    }

    ptr<buffer> commit(const ulong log_idx, buffer& data) {
        nl_log log_entry = nl_log::deserialize(data);

        // Just print.
        std::cout << "commit " << log_idx << ": "
                  << log_entry.to_string() << std::endl;

        // Update latest CSN separately from KV store modification.
        // Since DEL operations have CSN 0 they shouldn't affect latest CSN
        {
            std::lock_guard<std::mutex> ll(latest_csn_lock_);
            latest_csn_ = std::max(latest_csn_, log_entry.csn);
        }

        // Apply to local in-memory KV store.
        {   std::lock_guard<std::mutex> ll(kv_store_lock_);
            if (log_entry.op == nl_log::PUT) {
                kv_store_[log_entry.key].emplace_back(log_entry.value,
                                                    log_entry.csn);
            } else if (log_entry.op == nl_log::DEL) {
                kv_store_.erase(log_entry.key);
            }
        }

        // Update last committed index number.
        last_committed_idx_ = log_idx;
        return nullptr;
    }
    
    // optional
    void commit_config(const ulong log_idx, ptr<cluster_config>& new_conf) {
        // Nothing to do with configuration change. Just update committed index.
        last_committed_idx_ = log_idx;
    }

    void rollback(const ulong log_idx, buffer& data) {
        // Extract string from `data.
        buffer_serializer bs(data);
        std::string str = bs.get_str();

        // we do nothing for precommit so we do nothing for rollback
        std::cout << "rollback " << log_idx << ": "
                  << str << std::endl;
    }

    int read_logical_snp_obj(snapshot& s,
                             void*& user_snp_ctx,
                             ulong obj_id,
                             ptr<buffer>& data_out,
                             bool& is_last_obj)
    {
        // Put dummy data.
        data_out = buffer::alloc( sizeof(int32) );
        buffer_serializer bs(data_out);
        bs.put_i32(0);

        is_last_obj = true;
        return 0;
    }

    void save_logical_snp_obj(snapshot& s,
                              ulong& obj_id,
                              buffer& data,
                              bool is_first_obj,
                              bool is_last_obj)
    {
        std::cout << "save snapshot " << s.get_last_log_idx()
                  << " term " << s.get_last_log_term()
                  << " object ID " << obj_id << std::endl;
        // Request next object.
        obj_id++;
    }

    bool apply_snapshot(snapshot& s) {
        std::cout << "apply snapshot " << s.get_last_log_idx()
                  << " term " << s.get_last_log_term() << std::endl;
        // Clone snapshot from `s`.
        {   std::lock_guard<std::mutex> l(last_snapshot_lock_);
            ptr<buffer> snp_buf = s.serialize();
            last_snapshot_ = snapshot::deserialize(*snp_buf);
        }
        return true;
    }

    void free_user_snp_ctx(void*& user_snp_ctx) { }

    ptr<snapshot> last_snapshot() {
        // Just return the latest snapshot.
        std::lock_guard<std::mutex> l(last_snapshot_lock_);
        return last_snapshot_;
    }

    ulong last_commit_index() {
        return last_committed_idx_;
    }

    uint64_t get_latest_csn() {
        std::lock_guard<std::mutex> ll(latest_csn_lock_);
        return latest_csn_;
    }

    void create_snapshot(snapshot& s,
                         async_result<bool>::handler_type& when_done)
    {
        std::cout << "create snapshot " << s.get_last_log_idx()
                  << " term " << s.get_last_log_term() << std::endl;
        // Clone snapshot from `s`.
        {   std::lock_guard<std::mutex> l(last_snapshot_lock_);
            ptr<buffer> snp_buf = s.serialize();
            last_snapshot_ = snapshot::deserialize(*snp_buf);
        }
        ptr<std::exception> except(nullptr);
        bool ret = true;
        when_done(ret, except);
    }

    // Print the in-memory key/value store for debugging.
    void print_kv_store() {
        std::lock_guard<std::mutex> ll(kv_store_lock_);
        std::cout << "KV store contents (" << kv_store_.size() << " entries):\n";
        for (const auto &p : kv_store_) {
            if (p.second.empty()) continue;
            std::cout << "  \"" << p.first << "\":\n";
            for (size_t i = 0; i < p.second.size(); ++i) {
                const auto &entry = p.second[i];
                bool latest = (i + 1 == p.second.size());
                std::cout << "    [" << i << "] value=\"" << entry.first << "\""
                          << " csn=" << entry.second;
                if (latest) {
                    std::cout << "  <-- latest";
                }
                std::cout << "\n";
            }
        }
    }

    std::pair<std::string, uint64_t> get_value(std::string key, uint64_t csn) {
        std::lock_guard<std::mutex> ll(kv_store_lock_);
        auto it = kv_store_.find(key);
        if (it == kv_store_.end() || it->second.empty()) {
            return {std::string(), 0};
        }
        // Iterate from back to front to find the largest csn <= input csn
        for (auto rit = it->second.rbegin(); rit != it->second.rend(); ++rit) {
            if (rit->second <= csn) {
                return *rit;
            }
        }
        return {std::string(), 0};
    }

private:
    // Last committed Raft log number.
    std::atomic<uint64_t> last_committed_idx_;
    uint64_t latest_csn_ = 0;
    std::mutex latest_csn_lock_;

    // In-memory key/value store for PUT/DEL operations.
    std::map<std::string, std::deque<std::pair<std::string, uint64_t>>> kv_store_;
    std::mutex kv_store_lock_;

    // Last snapshot.
    ptr<snapshot> last_snapshot_;

    // Mutex for last snapshot.
    std::mutex last_snapshot_lock_;
};

