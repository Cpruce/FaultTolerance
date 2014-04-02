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
global_processes_update(M, []) -> 
	% first node. create all 2^M storage processes
	IdPidList = init_storprocs(math:pow(2,M), 0);
global_processes_update(M, [Neighbor])->
	case net_kernel:connect_node(Neighbor) of 
		true -> print("Connected to neighbor ~p~n", [Neighbor]), 
			Neighbors = reg_connect(global:registered_names()--Neighbor, [], M),
			NodeId = assign_id(math:pow(2, M), Neighbors),
			{Id, Neighbors};
		false -> print("Could not connect to neighbor ~p~n", [Neighbor]),
			{0, []} 
	end.

%% init storage processes. return tuple list of Ids with Pid's
init_storprocs(0, _Id, _Neighbors)-> [];
init_storprocs(N, Id, Neighbors)->
	Pid = spawn(storage_process, storage_process, [M, Id, Neighbors]),
	[{Id, Pid}] ++ init_storprocs(N-1, Id++).	

%% finds the lowest id number that is not taken
%assign_id(0, )
%assign_id()

%% creates a list of all and any other neighbors. 
%reg_connect([], M) -> [];	
%reg_connect(Neighbors, M) ->
%	{hd(Neighbors), } ! {},
%	receive
%		{Node, }
		

%% initiate rebalancing and update all nodes
%% when this node enters.
node_enter(M, [Neighbor])-> 
	% connect with nodes, assign id, get nodenum list, and update global list.
	{Id, Neighbors} = global_processes_update(M, [list_to_atom(Neighbor)]),
	
	Pid = spawn(advertise_id, advertise_id, []),
			

	ok.


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
