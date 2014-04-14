%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant distributed key-value storage system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(key_value_node_working).

-import(storage_process_working, [init_store/4, getStorageProcessName/1]).
-import(non_storage_process, [run/0]).
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
      println("node() = ~p", [node()]);
    {error, TheReason} ->
      println("fail to start kernel! intended shortnames: " ++ NodeName),
      println("Reason: ~p", TheReason)
  end,
  % spawn and register a non-storage process, so that the node can be discovered.
  NonStorageProcessPid = spawn(non_storage_process, run, []),
  global:register_name("NonStorageProcessAt" ++ NodeName, NonStorageProcessPid),
  println("~s > Registered a non storage process with name = ~p, PID = ~p",
    [node(), "NonStorageProcessAt" ++ NodeName, NonStorageProcessPid]),
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
    fun({Id, Pid}) ->
      global:register_name(getStorageProcessName(Id), Pid)
    end,
    IdPidList
  );
node_enter(M, Neighbors) ->
  % connect with nodes, assign id, get nodenum list, and update global list.
  {_Id, _NewNeighbors} = global_processes_update(M, Neighbors).


%% ====================================================================
%%                       Storage Process Functions
%% ====================================================================

%% Case: if this node is the first node
%% init storage processes. return tuple list of Ids with Pid's
init_storage_processes(0, _TwoToTheM, _Id) -> [];
init_storage_processes(_M, TwoToTheM, TwoToTheM) -> [];
init_storage_processes(M, TwoToTheM, Id) ->
  % Allowed to communicate to  Id + 2^k from k = 0 to M - 1
  Neighbors = [(Id + round(math:pow(2, K))) rem TwoToTheM || K <- lists:seq(0, M - 1)],
  println("~s > Spawning a storage process with id = ~p...", [node(), Id]),
  println("~s > Storage process ~p's neighbors will be the following: ~s",
    [node(), Id, pretty_print_list_of_nums(Neighbors)]),
  % spawn's arguments: Module, Function, Args
  % storate_serve's arguments: M, Id, Neighbors, Storage
  NodeID = 0,
  Pid = spawn(storage_process_working, init_store, [M, Id, NodeID]),
  println("~s > Storage process ~p spawned! Its PID is ~p", [node(), Id, Pid]),
  [{Id, Pid}] ++ init_storage_processes(M, TwoToTheM, Id + 1). 



% %% get and update global list of registered processes 
% %% from the one other known neighbor, connect, and
% %% assign unique node number.

global_processes_update(M, [Neighbor]) ->
  case net_kernel:connect_node(Neighbor) of
    true ->
      println("~s > node = ~p", [node(), node()]),
      println("~s > Connected to the neighbor ~p", [node(), Neighbor]),
      % sleep for 0.5 seconds -- we need to wait until names are successfully registered
      timer:sleep(500),
      TwoToTheM = round(math:pow(2, M)),
      _GlobalNames = global:registered_names(),
      NodeList = get_node_list(TwoToTheM),
      AllNodeList = lists:seq(0, TwoToTheM - 1),
      AvailableNodeList = AllNodeList -- NodeList,
      % pick a random element from a list
      AssignedNodeId = lists:nth(random:uniform(length(AvailableNodeList)), AvailableNodeList),
      println("~s > Will assign node id to ~p", [node(), AssignedNodeId]);
      % NodeList = lists:sort(get_node_list(NeighborsNames, node())),
      % println("List of neighbors: ~p", [NeighborsNames]);
    false ->
      println("~s > Could not connect to neighbor ~p", [node(), Neighbor])
  end,
  {1,2}.


get_node_list(TwoToTheM) ->
  RandomId = random:uniform(TwoToTheM) - 1, % pick a random number between 0 to 2^m - 1 inclusive.
  TargetStorageProcess = getStorageProcessName(RandomId),
  println("~s > Will try to ask ~p for a node list.", [node(), TargetStorageProcess]),
  Ref = make_ref(),
  println("~s:~p > Sending a node_list request...", [node(), Ref]),
  global:send(TargetStorageProcess, {self(), Ref, node_list}),
  receive
    {Ref, result, Result} ->
        println("~s:~p > The result is ~p", [node(), Ref, Result]),
        Result;
      _ ->
        println("~s > Error: Bad response!", [node()]),
        get_node_list(TwoToTheM)
  end.

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

