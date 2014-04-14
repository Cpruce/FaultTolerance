#!/usr/bin/bash

erlc storage_process.erl
erlc advertise_id.erl
erlc key_value_node.erl
erl -noshell -run key_value_node main 3 ct_node0
# erlc key_value_node.erl && erl -noshell -run key_value_node main 3 node2 node1@econ2gw-194-156-dhcp
