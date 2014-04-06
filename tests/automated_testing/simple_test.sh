#!/usr/bin/bash

erlc storage_process_temp.erl && erlc key_value_node_temp.erl && erl -noshell -run key_value_node_temp main 10 node1