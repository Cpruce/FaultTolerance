%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(advertise_id).
%% ====================================================================
%%                             Public API
%% ====================================================================
-export([init/5]).
%% ====================================================================
%%                             Constants
%% ====================================================================
%% ====================================================================
%%                            Main Function
%% ====================================================================
%% register and advertise the storage node
init(Id, NodeName, Neighbors, StorageProcs, TwoToTheM)->
	register(NodeName, self()),
	advertise(Id, NodeName, Neighbors, StorageProcs, TwoToTheM).

%% wait for any Id, rebalancing, or neighbors list queries
advertise(Id, NodeName, Neighbors, StorageProcs, TwoToTheM)->
	receive
		{RetNode, id} ->
			print("Received Id request from ~p~n", [RetNode]),
			RetNode ! {self(), Id},
			advertise(Id, NodeName, Neighbors, StorageProcs, TwoToTheM);
		{RetNode, nodes_list} ->
			print("Received NodeList request from ~p~n", [RetNode]),
			RetNode ! {NodeName, Neighbors},
			advertise(Id, NodeName, Neighbors, StorageProcs, TwoToTheM)
	
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
% print/1
% includes system time.
%print(To_Print) ->
%  io:format(get_formatted_time() ++ ": " ++ To_Print).
% print/2
print(To_Print, Options) ->
  io:format(get_formatted_time() ++ ": " ++ To_Print, Options).

