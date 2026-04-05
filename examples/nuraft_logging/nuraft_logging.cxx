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

#include "nl_state_machine.hxx"
#include "nl_state_mgr.hxx"
#include "logger_wrapper.hxx"

#include "nuraft.hxx"

#include "test_common.h"

#include <iostream>
#include <sstream>

#include <stdio.h>

using namespace nuraft;

namespace nuraft_logging {

static const raft_params::return_method_type CALL_TYPE
    = raft_params::blocking;
//  = raft_params::async_handler;
}
//specify namespaces for intellisense
#include "nl_common.hxx"
#include "nl_log.hxx"

namespace nuraft_logging{

nl_state_machine* get_sm() {
    return static_cast<nl_state_machine*>( stuff.sm_.get() );
}

void handle_result(ptr<TestSuite::Timer> timer,
                   raft_result& result,
                   ptr<std::exception>& err)
{
    if (result.get_result_code() != cmd_result_code::OK) {
        // Something went wrong.
        // This means committing this log failed,
        // but the log itself is still in the log store.
        std::cout << "failed: " << result.get_result_code() << ", "
                  << TestSuite::usToString( timer->getTimeUs() )
                  << std::endl;
        return;
    }
    std::cout << "succeeded, "
              << TestSuite::usToString( timer->getTimeUs() )
              << std::endl;
}

// append a log entry to the distributed log
void append_log(ptr<buffer> new_log)
{   

    // To measure the elapsed time.
    ptr<TestSuite::Timer> timer = cs_new<TestSuite::Timer>();

    // Do append.
    ptr<raft_result> ret = stuff.raft_instance_->append_entries( {new_log} );

    if (!ret->get_accepted()) {
        // Log append rejected, usually because this node is not a leader.
        std::cout << "failed to replicate: "
                  << ret->get_result_code() << ", "
                  << TestSuite::usToString( timer->getTimeUs() )
                  << std::endl;
        return;
    }
    // Log append accepted, but that doesn't mean the log is committed.
    // Commit result can be obtained below.

    if (CALL_TYPE == raft_params::blocking) {
        // Blocking mode:
        //   `append_entries` returns after getting a consensus,
        //   so that `ret` already has the result from state machine.
        ptr<std::exception> err(nullptr);
        handle_result(timer, *ret, err);

    } else if (CALL_TYPE == raft_params::async_handler) {
        // Async mode:
        //   `append_entries` returns immediately.
        //   `handle_result` will be invoked asynchronously,
        //   after getting a consensus.
        ret->when_ready( std::bind( handle_result,
                                    timer,
                                    std::placeholders::_1,
                                    std::placeholders::_2 ) );

    } else {
        assert(0);
    }
}

// API FUNCTIONS
// these functions are the intended entry point for any api to the state machine.
// the CLI operations will also call these functions.
void put(const std::string& key,
                   const std::string& value,
                   uint64_t csn)
{
    ptr<buffer> new_log = nl_log(nl_log::PUT, key, value, csn).serialize();
    append_log(new_log);
}

void del(const std::string& key)
{
    ptr<buffer> new_log = nl_log(nl_log::DEL, key).serialize();
    append_log(new_log);
}

// Note: returns {std::string(), 0} if key not found or no csn <= input_csn
std::pair<std::string, uint64_t> get(const std::string& key, uint64_t csn)
{
    return get_sm()->get_value(key, csn);
}

uint64_t get_latest_csn()
{
    return get_sm()->get_latest_csn();
}

// add a node to the cluster
void add_node(int64 server_id,
         std::string server_addr)
{   
    // we make the server id nonzero since atoi parsing errors return 0 (for cli)
    if ( !server_id || server_id == stuff.server_id_ ) {
        std::cout << "wrong server id: " << server_id << std::endl;
        return;
    }
    srv_config srv_conf_to_add( server_id, server_addr );
    ptr<raft_result> ret = stuff.raft_instance_->add_srv(srv_conf_to_add);

    if (!ret->get_accepted()) {
        std::cout << "failed to add server: "
                  << ret->get_result_code() << std::endl;
        return;
    }
    std::cout << "async request is in progress (check with `list` command)"
              << std::endl;
}

// flips a node in the cluster to learner mode
void flip_to_learner(int64 server_id)
{
    if ( !server_id || server_id == stuff.server_id_ ) {
        std::cout << "wrong server id: " << server_id << std::endl;
        return;
    }
    ptr<raft_result> ret = stuff.raft_instance_->flip_learner_flag(server_id, true);

    if (!ret->get_result_code() == 0) {
        std::cout << "failed to flip to learner: "
                << ret->get_result_code() << std::endl;
        return;
    }

    std::cout << "async request is in progress (check with `list` command)"
        << std::endl;
}
// END OF API FUNCTIONS

void print_status(const std::string& cmd,
                  const std::vector<std::string>& tokens)
{
    ptr<log_store> ls = stuff.smgr_->load_log_store();
    std::cout
        << "my server id: " << stuff.server_id_ << std::endl
        << "leader id: " << stuff.raft_instance_->get_leader() << std::endl
        << "Raft log range: "
            << ls->start_index()
            << " - " << (ls->next_slot() - 1) << std::endl
        << "last committed index: "
            << stuff.raft_instance_->get_committed_log_idx() << std::endl;
        get_sm()->print_kv_store();
}

// TODO: this is outdated, may or may not update
void help(const std::string& cmd,
          const std::vector<std::string>& tokens)
{
    std::cout
    << "echo message: msg <operand>\n"
    << "    e.g.) msg hello world!\n"
    << "\n"
    << "put a key/value: put <key> <value> <csn>\n"
    << "    e.g.) put mykey myvalue 42\n"
    << "\n"
    << "get a key at csn: get <key> <csn>\n"
    << "    e.g.) get mykey 42\n"
    << "\n"
    << "get latest CSN: csn\n"
    << "    e.g.) csn\n"
    << "\n"
    << "add server: add <server id> <address>:<port>\n"
    << "    e.g.) add 2 127.0.0.1:20000\n"
    << "\n"
    << "get current server status: st (or stat)\n"
    << "\n"
    << "get the list of members: ls (or list)\n"
    << "\n";
}

bool do_cmd(const std::vector<std::string>& tokens) {
    if (!tokens.size()) return true;

    const std::string& cmd = tokens[0];

    if (cmd == "q" || cmd == "exit") {
        // Shutdown log_store and rocksdb instance before calling stuff.reset(). 
        auto ls = stuff.smgr_->load_log_store();
        ls->close();
        stuff.launcher_.shutdown(5);
        return false;

    } else if ( cmd == "put" ) {
        // e.g. put k v csn
        if (tokens.size() != 4) {
            std::cout << "not the right number of arguments" << std::endl;
            // return true so as to not exit the main loop for CLI
            return true;
        }
        std::string key = tokens[1];
        std::string value = tokens[2];
        uint64_t csn = 0;
        try {
            csn = std::stoull(tokens[3]);
        } catch (...) {
            std::cout << "invalid csn value" << std::endl;
            return true;
        }
        put(key, value, csn);
    } else if ( cmd == "del" ) {
        // e.g. delete k
         if (tokens.size() != 2) {
            std::cout << "not the right number of arguments" << std::endl;
            return true;
        }
        std::string key = tokens[1];
        del(key);
    } else if ( cmd == "get" ) {
        // e.g. get k csn
        if (tokens.size() != 3) {
            std::cout << "not the right number of arguments" << std::endl;
            return true;
        }
        std::string key = tokens[1];
        uint64_t input_csn = 0;
        try {
            input_csn = std::stoull(tokens[2]);
        } catch (...) {
            std::cout << "invalid csn value" << std::endl;
            return true;
        }
        std::pair<std::string, uint64_t> result = get(key, input_csn);
        const std::string &value = result.first;
        uint64_t csn = result.second;

        if (value.empty()) {
            std::cout << "Key '" << key << "' not found at csn <= " << input_csn << std::endl;
        } else {
            std::cout << "Value for key '" << key << "' at csn <= " << input_csn << ": " << value
                      << " (csn=" << csn << ")" << std::endl;
        }
    } else if ( cmd == "csn" ) {
        if (tokens.size() != 1) {
            std::cout << "csn takes no arguments" << std::endl;
            return true;
        }
        uint64_t latest = get_latest_csn();
        std::cout << "Latest CSN: " << latest << std::endl;
    } else if ( cmd == "add" ) {
        // e.g. add 2 localhost:12345
        if (tokens.size() < 3) {
            std::cout << "too few arguments" << std::endl;
            return true;
        }
        int server_id_to_add = atoi(tokens[1].c_str());
        std::string endpoint_to_add = tokens[2];
        add_node(server_id_to_add, endpoint_to_add);
    } else if ( cmd == "st" || cmd == "stat" ) {
        print_status(cmd, tokens);

    } else if ( cmd == "ls" || cmd == "list" ) {
        server_list(cmd, tokens);

    } else if ( cmd == "h" || cmd == "help" ) {
        help(cmd, tokens);
    } else if (cmd == "flip") {
        if (tokens.size() != 2) {
            std::cout << "not the right number of arguments" << std::endl;
            return true;
        }
        int server_id = atoi(tokens[1].c_str());
        flip_to_learner(server_id);
    }
    return true;
}

}; // namespace nuraft_logging;
using namespace nuraft_logging;

int main(int argc, char** argv) {
    if (argc < 3) usage(argc, argv);

    set_server_info(argc, argv);

    std::cout << "    -- Echo Server with Raft --" << std::endl;
    std::cout << "               Version 0.1.0" << std::endl;
    std::cout << "    Server ID:    " << stuff.server_id_ << std::endl;
    std::cout << "    Endpoint:     " << stuff.endpoint_ << std::endl;
    init_raft( cs_new<nl_state_machine>() );
    loop();

    return 0;
}

