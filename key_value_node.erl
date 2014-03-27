%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc The show must go on!
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
        % The first parameter is destination node name
        %  It is a lowercase ASCII string with no periods or @ signs in it.
        NodeName = hd(Params),
        % 0 or more additional parameters, each of which is the Erlang node
        % name of a neighbor of the philosopher.
        NeighborsList = tl(Params),
        Neighbors = lists:map(fun(Node) -> list_to_atom(Node) end,
          NeighborsList),
        %% IMPORTANT: Start the empd daemon!
        os:cmd("epmd -daemon"),
        % format microseconds of timestamp to get an
        % effectively-unique node name
        net_kernel:start([list_to_atom(NodeName), shortnames]),
        register(philosopher, self()),
        %joining
        philosophize(joining, Neighbors, dict:new()),
    halt().

%% hash function to uniformly distribute among 
%% storage processes.
hash(z, n) when n > 0 -> z rem n;
hash(_, _) -> -1. 	%% error if no storage
			%% processes are open.

%check_neighbors([], _)-> ok;
%check_neighbors([X|XS], ParentPid) ->
%    spawn(fun() ->  monitor_neighbor(X, ParentPid) end),
%    check_neighbors(XS, ParentPid).
%monitor_neighbor(Philosopher, ParentPid) ->
%    erlang:monitor(process,{philosopher, Philosopher}), %{RegName, Node}
%       receive
%        {'DOWN', _Ref, process, _Pid,  normal}  ->
%            ParentPid ! {self(), check, Philosopher};
%        {'DOWN', _Ref, process, _Pid,  _Reason} ->
%            ParentPid ! {self(), missing, Philosopher}
%       end.


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
