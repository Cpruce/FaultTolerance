%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(key_value_node).
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
    % try
        % The first parameter is m, the value that determines the number
        % of storage processes in the system.
        M = hd(Params),
	% The second parameter is the name of the node to register. This
	% should be a lowercase ASCII string with no periods or @ signs.
	NodeName = hd(hd(Params)),
        % 0 or 1 additional parameters. If not nil, then the extra 
	% parameter is the registered name of anoter node.
	[Neighbor] = tl(Params),
	% IMPORTANT: Start the empd daemon!
        os:cmd("epmd -daemon"),
        % format microseconds of timestamp to get an
        % effectively-unique node name
        net_kernel:start([list_to_atom(NodeName), shortnames]),
        register(NodeName, self()),
        % begin storage service 
        node_enter(M, [Neighbor], dict:new()),
    halt().

%% get and update global list of registered processes 
%% from the one other known neighbor, connect, and
%% assign unique node number.
global_processes_update(_M, []) -> {0, []};
global_processes_update(M, [Neighbor])->
	case net_kernel:connect_node(Neighbor) of 
		true -> print("Connected to neighbor ~p~n", [Neighbor]), 
			Neighbors = reg_connect(global:registered_names(), []),
			Id = assign_id(M, Neighbors),
			{Id, Neighbors};
		false -> print("Could not connect to neighbor ~p~n", [Neighbor]),
			{0, []} 
	end.

%% connects with the rest of the registered names, returns list of node numbes
reg_connect([], Neighbors) -> 
	;
reg_connect(Names, Neighbors) ->
	case net_kernel:connect_node(hd(Names)) of 
		true ->  print("Connected to neighbor ~p~n", [hd(Names)]),
			 % build Neighbors list of {Name, Id}
			 reg_connect(tl(Names), Neighbors ++ request_id(hd(Names))); 
	        false -> print("Could not connect to neighbor ~p~n", [hd(Names)]),
			 reg_connect(tl(Names))
	end.	  

%% initiate rebalancing and update all nodes
%% when this node enters.
node_enter(M, [Neighbor], Storage)-> 
	% connect with nodes, assign id, get nodenum list, and update global list.
	{Id, Neighbors} = global_processes_update(M, [list_to_atom(Neighbor)]),
	
	% get the global set of registered processes from neighbor.
	%NeighborsNames = global:registered_names(),
	
	% can only talk to neighbors with ids i + 2^k mod 2^m for 0 <= k < m
	% Neighbors = filter_neighbors(Neigbors
	
	% start storage service!
        storage_serve(M, Id, Neighbors, Storage).

%% put together list of {Neighbor, NeighborId} 
%assemble_neighbors([], Neighbors) -> Neighbors;
%assemble_neighbors(NeighborsNames, Neighbors)->
%	Rcvr = hd(NeighborsNames),
	

%% primary storage service function; handles
%% general communication and functionality.
storage_serve(M, Id, Neighbors, Storage)-> 
	receive 
		{Pid, Ref, store, Key, Value} -> ok;
		{Ref, stored, Oldval} -> ok;
		{Pid, Ref, retrieve, Key} -> ok;
		{Ref, retrieved, Value} -> ok;
		{Pid, Ref, first_key} -> ok;
		{Pid, Ref, last_key} -> ok;
		{Pid, Ref, num_keys} -> ok;
		{Pid, Ref, node_list} -> ok;
		{Ref, result, Result} -> ok;
		{Ref, failure} -> ok
	end.

%% sum digits in string
str_sum([]) -> 0;
str_sum([X|XS]) -> $X + str_sum(XS).

%% hash function to uniformly distribute among 
%% storage processes.
hash(Str, M) when M >= 0 -> str_sum(Str) rem (math:pow(2, M));
hash(_, _) -> -1. 	%% error if no storage
			%% processes are open.


%% monitor neighbors for crashes
check_neighbors([], _)-> ok;
check_neighbors([X|XS], ParentPid) ->
    spawn(fun() ->  monitor_neighbor(X, ParentPid) end),
    check_neighbors(XS, ParentPid).
monitor_neighbor(Neighbor, ParentPid) ->
    erlang:monitor(process,{neighbor, Neighbor}), %{RegName, Node}
       receive
        {'DOWN', _Ref, process, _Pid,  normal}  ->
            ParentPid ! {self(), check, Neighbor};
        {'DOWN', _Ref, process, _Pid,  _Reason} ->
            ParentPid ! {self(), missing, Neighbor}
       end.

%% Chandy-Lamport Snapshot Algorithm


%% Leader Election Algorithm

% Helper functions for timestamp handling.
get_two_digit_list(Number) ->
  if Number < 10 ->
       ["0"] ++ integer_to_list(Number);
     true ->
       integer_to_list(Number)
  end.
get_three_digit_list(Number) ->
  if Number < 10 ->
       ["00"] ++ integer_to_list(Number);
     Number < 100 ->
         ["0"] ++ integer_to_list(Number);
     true ->
       integer_to_list(Number)
  end.
get_formatted_time() ->
  {MegaSecs, Secs, MicroSecs} = now(),
  {{Year, Month, Date},{Hour, Minute, Second}} =
    calendar:now_to_local_time({MegaSecs, Secs, MicroSecs}),
  integer_to_list(Year) ++ ["-"] ++
  get_two_digit_list(Month) ++ ["-"] ++
  get_two_digit_list(Date) ++ [" "] ++
  get_two_digit_list(Hour) ++ [":"] ++
  get_two_digit_list(Minute) ++ [":"] ++
  get_two_digit_list(Second) ++ ["."] ++
  get_three_digit_list(MicroSecs div 1000).
% print/1
% includes system time.
print(To_Print) ->
  io:format(get_formatted_time() ++ ": " ++ To_Print).
% print/2
print(To_Print, Options) ->
  io:format(get_formatted_time() ++ ": " ++ To_Print, Options).
