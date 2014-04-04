#!/usr/bin/bash

erlc key_value_node.erl && erl -noshell -run key_value_node main 10 node1
