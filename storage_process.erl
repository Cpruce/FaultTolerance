%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(storage_process).
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
        % The first parameter is 2^M, the value that determines the number
        % of storage processes in the system.
	M = compute_power2(hd(Params)),
	% The storage process Id is the second; convert to String
	Id = lists:flatten(io_lib:format("~p", [hd(tl(Params))])),
	% The neighbors are the third parameter
	Neighbors = tl(tl(Params)),
	% register as storage_process8 where 8 is the Id
	register(list_to_atom("storage_process"++Id), self()),
        % begin storage service! 
	storage_serve(M, Id, Neighbors, dict:new()),
    halt().

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

%% compute M from 2^M
compute_power2(N) when N < 2 -> 0;
compute_power2(N) -> 
	1+compute_power2(math:pow(N, 1/2)).

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
