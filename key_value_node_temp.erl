%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant distributed key-value storage system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(key_value_node_temp).

-import(storage_process_temp, [storage_serve/4]).
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
  Neighbors = lists:map(fun(Node) -> list_to_atom(Node) end, NeighborsList),
  if
    Neighbors == [] ->
      println("This node has no neighbors. It must be the first node.");
    true ->
      println("Neighbors = ~p", [Neighbors])
  end,
  % IMPORTANT: Start the epmd daemon!
  os:cmd("epmd -daemon"),
  % format microseconds of timestamp to get an
  % effectively-unique node name
  net_kernel:start([list_to_atom(NodeName), shortnames]),
  % begin storage service 
  node_enter(M, NodeName, Neighbors).


%% ====================================================================
%%                           Node Functions
%% ====================================================================


% initiate rebalancing and update all nodes
% when this node enters.
node_enter(M, NodeName, []) ->
  TwoToTheM = round(math:pow(2, M)),
  IdPidList = init_storage_processes(M, TwoToTheM, 0),
  % globally regiser each of the 2^m processes
  lists:map(fun({Name, Pid}) -> global:register_name(Name, Pid) end, IdPidList);
node_enter(M, NodeName, Neighbors) ->
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
init_storage_processes(0, _, _Id) -> [];
init_storage_processes(M, TwoToTheM, TwoToTheM) -> [];
init_storage_processes(M, TwoToTheM, Id) ->
  % Allowed to communicate to  Id + 2^k from k = 0 to M - 1
  Neighbors = [Id + round(math:pow(2, K)) || K <-lists:seq(0, M - 1)],
  println("Spawning a storage process with id = ~p...", [Id]),
  println("Storage process ~p's neighbors will be the following: ~s",
    [Id, pretty_print_list_of_nums(Neighbors)]),
  % spawn's arguments are: Module, Function, Args
  Pid = spawn(storage_process_temp, storage_serve, [M, Id, Neighbors, []]),
  println("Storage process ~p spawned! Its PID is ~p", [Id, Pid]),
  [{Id, Pid}] ++ init_storage_processes(M, TwoToTheM, Id + 1). 



% %% get and update global list of registered processes 
% %% from the one other known neighbor, connect, and
% %% assign unique node number.
global_processes_update(M, Neighbors) ->
  Neighbor = hd(Neighbors),
  case net_kernel:connect_node(list_to_atom("node1@Js-MacBook-Pro-8")) of
    true ->
      println("Connected to neighbor ~p", [Neighbor]);
    false ->
      println("Could not connect to neighbor ~p", [Neighbor])
  end,
  println("~p", [global:registered_names() == []]),
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


% pretty_print_list_of_nums/1
% The parameter is a list L. If L = [1, 2, 3], it returns "[1, 2, 3]".
pretty_print_list_of_nums(L) ->
    StringList = lists:map(fun(Num) -> integer_to_list(Num) end, L),
    "[" ++ string:join(StringList, ", ") ++ "]".

