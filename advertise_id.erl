%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(advertise_id).
%% ====================================================================
%%                             Public API
%% ====================================================================
-export([main/1]).
%% ====================================================================
%%                             Constants
%% ====================================================================
%% ====================================================================
%%                            Main Function
%% ====================================================================
% The main/1 function.
main(Params) ->
	%% Example:
	%{ash:1} erl -noshell -run key_value_node main 10 node1
	%{elm:1} erl -noshell -run key_value_node main 10 node2 node1@ash
	%{oak:1} erl -noshell -run key_value_node main 10 node3 node1@ash
    
      % try
        % The first parameter is m, the value that determines the number
        % of storage processes in the system.
        M = hd(Params),
	% The second parameter is the name of the node to register. This
	% should be a lowercase ASCII string with no periods or @ signs.
	NodeName = hd(tl(Params)),
        % 0 or 1 additional parameters. If not nil, then the extra 
	% parameter is the registered name of anoter node.
	[Neighbor] = tl(tl(Params)),
	% IMPORTANT: Start the empd daemon!
        os:cmd("epmd -daemon"),
        % format microseconds of timestamp to get an
        % effectively-unique node name
        net_kernel:start([list_to_atom(NodeName), shortnames]),
        register(NodeName, self()),
        % begin storage service 
        node_enter(M, [Neighbor]),
    halt().

%% get and update global list of registered processes 
%% from the one other known neighbor, connect, and
%% assign unique node number.
global_processes_update(_M, []) -> {0, []};
global_processes_update(M, [Neighbor])->
	case net_kernel:connect_node(Neighbor) of 
		true -> print("Connected to neighbor ~p~n", [Neighbor]), 
			Neighbors = reg_connect(global:registered_names()--Neighbor, [], M),
			Id = assign_id(math:pow(2, M), Neighbors),
			{Id, Neighbors};
		false -> print("Could not connect to neighbor ~p~n", [Neighbor]),
			{0, []} 
	end.

%% finds the lowest id number that is not taken
assign_id(0, )
assign_id()

%% connects with the rest of the registered names, returns list of node numbes
reg_connect([], M) -> 
	Storage = spawn_storprocs(M);
reg_connect(Neighbors, M) ->
	

%% spawns storage processes
spawn_storprocs(0)->
	Pid = spawn(storage_process, storage_process, []);
spawn_storprocs(M)->
	Pid = spawn(storage_process, storage_process, []), 
	spawn_storprocs(M-1).

%% initiate rebalancing and update all nodes
%% when this node enters.
node_enter(M, [Neighbor])-> 
	% connect with nodes, assign id, get nodenum list, and update global list.
	{Id, Neighbors} = global_processes_update(M, [list_to_atom(Neighbor)]),
	
	% get the global set of registered processes from neighbor.
	%NeighborsNames = global:registered_names(),
	
	% can only talk to neighbors with ids i + 2^k mod 2^m for 0 <= k < m
	%Neighbors = filter_neighbors(Neigbors
	
	% start storage service!
        storage_serve(M, Id, Neighbors).
	ok.


