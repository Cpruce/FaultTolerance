#!/usr/bin/bash

erlc storage_process.erl
erlc advertise_id.erl
erlc key_value_node.erl
erl -noshell -run key_value_node main 3 ct_node ct_node@$1
# erlc key_value_node_working.erl && erl -noshell -run key_value_node_working main 3 node2 node1@wl-203-34
