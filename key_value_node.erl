%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant distributed key-value storage system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(key_value_node).

-import(storage_process, [init_store/5]).
-import(advertise_id, [init_adv/5]).
%% ====================================================================
%%                             Public API
%% ====================================================================
-export([main/1, print/1, print/2, println/1, println/2]).
%% ====================================================================
%%                             Constants
%% ====================================================================
%% ====================================================================
%%                            Main Function
%% ====================================================================
% The main/1 function.
main(Params) ->
  %% Example:
  % {ash:1} erl -noshell -run key_value_node main 10 node1
  % {elm:1} erl -noshell -run key_value_node main 10 node2 node1@ash
  % {oak:1} erl -noshell -run key_value_node main 10 node3 node1@ash
  
  % try
  % The first parameter is m, the value that determines the number
  % of storage processes in the system.
  MString = hd(Params),
  % convert to an integer. a float will be acceptable and will be casted 
  % to int. Any other format of m will halt. We use to_integer instead
  % of list_to
  M = case string:to_integer(MString) of
     {error, Reason} ->
       halt("Error in m: " ++ atom_to_list(Reason));
     {Int, _} ->
      Int
  end,
  println("m = " ++ integer_to_list(M)),
  % The second parameter is the name of the node to register. This
  % should be a lowercase ASCII string with no periods or @ signs.
  NodeName = hd(tl(Params)),
  println("nodename = " ++ NodeName),
  % 0 or more additional parameters. If not nil, then the extra 
  % parameter is the registered name of anoter node.
  NeighborsList = tl(tl(Params)),
  Neighbors = lists:map(fun(Node) -> list_to_atom(Node) end, NeighborsList),
  if
    Neighbors == [] ->
      println("This node has no neighbors. It must be the first node.")
  end,
  % IMPORTANT: Start the epmd daemon!
  os:cmd("epmd -daemon"),
  % format microseconds of timestamp to get an
  % effectively-unique node name
  net_kernel:start([list_to_atom(NodeName), shortnames]),
  % begin storage service 
  node_enter(M, NodeName, Neighbors).

% initiate rebalancing and update all nodes
% when this node enters.
node_enter(M, NodeName, Neighbors) ->
  TwoToTheM = round(math:pow(2, M)),
  case Neighbors of
      [] ->
        % first node. create all 2^M storage processes, starting with id 0.
        StorageProcs = init_storage_processes(M, NodeName, TwoToTheM, 0),
	_Pid = spawn(advertise_id, init_adv, [0, NodeName, [], StorageProcs, TwoToTheM]),
	ok;
      [Neighbor] ->
	% contact known neighbor to get list of everyone.
        % assign id and balance load. End with advertising,	
	{Id, PrevId, NextId, NodeList} = global_processes_update(TwoToTheM, Neighbor, NodeName),
	case PrevId == -1 of
	       true -> 
	       		StorageProcs = init_storage_processes(M, NodeName, TwoToTheM, 0),
			_Pid = spawn(advertise_id, init_adv, [Id, NodeName, NodeList, StorageProcs, TwoToTheM]);
	       false ->
			StorageProcs = enter_load_balance(Id, PrevId, NextId, [], TwoToTheM),
			_Pid = spawn(advertise_id, init_adv, [Id, NodeName, NodeList, StorageProcs, TwoToTheM])
	end,
	ok;
      _ -> ok
  end.


%% retreive storage processes from other nodes
enter_load_balance(Id, PrevId, NextId, Storage_Procs, TwoToTheM)->
	% send message to node with Id PrevId to get 
	% storage processes [Id, NextId] including Id and 
	% excluding NextId
	
	ok.

%% Case: if this node is the first node
%% init storage processes. return tuple list of Ids with Pid's
init_storage_processes(0, _NodeName,  _, _Id) -> [];
init_storage_processes(_M, _NodeName, TwoToTheM, TwoToTheM) -> [];
init_storage_processes(M, NodeName, TwoToTheM, Id) ->
  % Allowed to communicate to  Id + 2^k from k = 0 to M - 1
  Neighbors = [ Id + round(math:pow(2, K)) || K <-lists:seq(0, M - 1)],
  println("Spawning a storage process with id = ~p...", [Id]),
  println("Storage process ~p's neighbors will be the following: ~p", [Id, Neighbors]),
  % spawn's arguments are: Module, Function, Args
  Pid = spawn(storage_process, init_store, [TwoToTheM, NodeName, Id, Neighbors, []]),
  println("Storage process ~p spawned! Its PID is ~p", [Id, Pid]),
  [{Id, Pid}] ++ init_storage_processes(M, NodeName, TwoToTheM, Id + 1). 

%% get and update global list of registered nodes 
%% from the one other known neighbor, connect, and
%% assign unique node number.
global_processes_update(TwoToTheM, [Neighbor], NodeName) ->
   case net_kernel:connect_node(Neighbor) of 
     true -> print("Connected to neighbor ~p~n", [Neighbor]), 
       NeighborsNames = global:registered_names(),
       NodeList = lists:sort(get_global_list(NeighborsNames, Neighbor, NodeName)),
       {Id, PrevId, NextId} = assign_id(hd(NodeList), tl(NodeList), {-1, 0, -1}, TwoToTheM),
       {Id, PrevId, NextId, NodeList};
     false -> print("Could not connect to neighbor ~p~n", [Neighbor]),
       {0, -1, -1, []} 
   end.

%% gets global list of {Node, Id, Pid}'s
get_global_list(NeighborsNames, Neighbor, NodeName)->
	print("Sending request to ~p for the node list~n", [Neighbor]),
	Neighbor ! {NodeName, node_list},
	receive
		{Pid, node_list, NodeList} ->
			print("Recieved node list from ~p~n", [Pid]),
			NodeList;
		{_Pid, failure} ->
			print("Neighbor ~p failed. Now trying ~p~n", [Neighbor, hd(NeighborsNames)]),
			get_global_list(tl(NeighborsNames), hd(Neighbor), NodeName)	
	end.

%% compute furthest distance between any two node ids
%% X is the greatest difference and Id is the lower of the 
%% two Id's. NewId is half the greatest distance plus the 
%% lower of the two Id's, mod 2^M.
%% Assumption: NodeList is sorted.
assign_id(_Hd, [], {PrevId,X, NextId}, TwoToTheM)-> 
	{(PrevId + (X div 2)) rem TwoToTheM, PrevId, NextId};
assign_id(Hd, [IdN], {PrevId, X, NextId}, TwoToTheM)->
	% difference between last elem and the head
	% only non-increasing difference (mod 2^M)
	Dif = TwoToTheM - (IdN - Hd), 
        case Dif > X of
		true ->
			{(IdN + (Dif div 2)) rem TwoToTheM, IdN, Hd};
		false ->
			{(PrevId + (X div 2)) rem TwoToTheM, PrevId, NextId}
	end;	
assign_id(Hd, [IdM, IdN, NodeList], {PrevId, X, NextId}, TwoToTheM)->
	Dif = IdN - IdM, % Id's are now guaranteed to be increasing
	case Dif > X of
		true -> 
			assign_id(Hd, [IdN]++NodeList, {IdM, Dif, IdN}, TwoToTheM);
		false ->
			assign_id(Hd, [IdN]++NodeList, {PrevId, X, NextId}, TwoToTheM)
	end.

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

% println/1
% print and add a new line at the end
println(To_Print) ->
  print(To_Print ++ "~n").

% println/2
println(To_Print, Options) ->
  print(To_Print ++ "~n", Options).

% print/1
% includes system time.
print(To_Print) ->
  io:format(get_formatted_time() ++ ": " ++ To_Print).
% print/2
print(To_Print, Options) ->
  io:format(get_formatted_time() ++ ": " ++ To_Print, Options).
