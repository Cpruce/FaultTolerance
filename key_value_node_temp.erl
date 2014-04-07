%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant distributed key-value storage system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(key_value_node_temp).

-import(storage_process, [storage_serve/4]).
-import(non_storage_process, [run/0]).
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
  % 'neighoors' is a list of atoms
  Neighbors = lists:map(fun(Node) -> list_to_atom(Node) end, NeighborsList),
  case length(Neighbors) of
    0 ->
      println("This node has no neighbors. It must be the first node.");
    1 ->
      println("Neighbors = ~p", [Neighbors]);
    _ ->
      halt("There cannot be more than one neighbor to connect to!")
  end,
  % IMPORTANT: Start the epmd daemon!
  os:cmd("epmd -daemon"),
  % format microseconds of timestamp to get an
  % effectively-unique node name
  case net_kernel:start([list_to_atom(NodeName), shortnames]) of
    {ok, _Pid} ->
      println("kernel started successfully with the shortnames " ++ NodeName),
      println("~p", [node()]);
    {error, TheReason} ->
      println("fail to start kernel! intended shortnames: " ++ NodeName),
      println("Reason: ~p", TheReason)
  end,
  % spawn and register a non-storage process, so that the node can be discovered.
  NonStorageProcessPid = spawn(non_storage_process, run, []),
  global:register_name("NonStorageProcessAt" ++ NodeName, NonStorageProcessPid),
  println("Registered a non storage process with name = ~p, PID = ~p",
    ["NonStorageProcessAt" ++ NodeName, NonStorageProcessPid]),
  % begin storage service 
  node_enter(M, Neighbors).


%% ====================================================================
%%                           Node Functions
%% ====================================================================


% initiate rebalancing and update all nodes
% when this node enters.
node_enter(M, []) ->
  TwoToTheM = round(math:pow(2, M)),
  IdPidList = init_storage_processes(M, TwoToTheM, 0),
  % globally register each of the 2^m processes
  lists:map(
    fun({Id, Pid}) -> global:register_name("StorageProcess" ++ integer_to_list(Id), Pid) end,
    IdPidList
  );
node_enter(M, Neighbors) ->
  % connect with nodes, assign id, get nodenum list, and update global list.
  {Id, NewNeighbors} = global_processes_update(M, Neighbors).



  % (module, name, args)
  % Pid = spawn(advertise_id, advertise_id, [Id, NodeName]),
  % ok.

%% ====================================================================
%%                       Storage Process Functions
%% ====================================================================

%% Case: if this node is the first node
%% init storage processes. return tuple list of Ids with Pid's
init_storage_processes(0, _TwoToTheM, _Id) -> [];
init_storage_processes(_M, TwoToTheM, TwoToTheM) -> [];
init_storage_processes(M, TwoToTheM, Id) ->
  % Allowed to communicate to  Id + 2^k from k = 0 to M - 1
  Neighbors = [Id + round(math:pow(2, K)) || K <-lists:seq(0, M - 1)],
  println("Spawning a storage process with id = ~p...", [Id]),
  println("Storage process ~p's neighbors will be the following: ~s",
    [Id, pretty_print_list_of_nums(Neighbors)]),
  % spawn's arguments: Module, Function, Args
  % storate_serve's arguments: M, Id, Neighbors, Storage
  Pid = spawn(storage_process, storage_serve, [M, Id, Neighbors, []]),
  println("Storage process ~p spawned! Its PID is ~p", [Id, Pid]),
  [{Id, Pid}] ++ init_storage_processes(M, TwoToTheM, Id + 1). 



% %% get and update global list of registered processes 
% %% from the one other known neighbor, connect, and
% %% assign unique node number.

global_processes_update(M, [Neighbor]) ->
  case net_kernel:connect_node(Neighbor) of
    true ->
      println("node = ~p", [node()]),
      println("Connected to the neighbor ~p", [Neighbor]),
      NeighborsNames = global:registered_names(),
      println("List of registered names is empty: ~p", [NeighborsNames == []]);
    false ->
      println("Could not connect to neighbor ~p", [Neighbor])
  end,
  {1,2}.
  % case net_kernel:connect_node(Neighbor) of 
  %   true -> println("Connected to neighbor ~p~n", [Neighbor]), 
  %     % Neighbors = reg_connect(global:registered_names() -- Neighbor, [], M),
  %     % NodeId = assign_id(math:pow(2, M), Neighbors),
  %     {0, []};
  %     %{Id, Neighbors};
  %   false -> println("Could not connect to neighbor ~p~n", [Neighbor]),
  %     {0, []} 
  % end.

%% ====================================================================
%%                       Pretty Print Functions
%% ====================================================================
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
println(ToPrint) ->
  print(ToPrint ++ "~n").

% println/2
println(ToPrint, Options) ->
  print(ToPrint ++ "~n", Options).

% print/1
% includes system time.
print(ToPrint) ->
  io:format(get_formatted_time() ++ ": " ++ ToPrint).
% print/2
print(ToPrint, Options) ->
  io:format(get_formatted_time() ++ ": " ++ ToPrint, Options).


% pretty_print_list_of_nums/1
% The parameter is a list L. If L = [1, 2, 3], it returns "[1, 2, 3]".
pretty_print_list_of_nums(L) ->
    StringList = lists:map(fun(Num) -> integer_to_list(Num) end, L),
    "[" ++ string:join(StringList, ", ") ++ "]".

