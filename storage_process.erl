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
        % The first parameter is m, the value that determines the number
        % of storage processes in the system.
        M = hd(Params),
	% The storage process Id is the second
	Id = hd(tl(Params)),
	% The neighbors are the third parameter
	Neighbors = tl(tl(Params)),
        register(storage_process, self()),
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


