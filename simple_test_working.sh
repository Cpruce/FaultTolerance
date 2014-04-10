#!/usr/bin/bash

erlc storage_process_working.erl
erlc non_storage_process.erl
# erlc advertise_id.erl
erlc key_value_node_working.erl
erl -noshell -run key_value_node_working main 3 node1
# erlc key_value_node_working.erl && erl -noshell -run key_value_node_working main 3 node2 node1@wl-203-34
