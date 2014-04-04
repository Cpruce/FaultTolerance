%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(storage_process_temp).
%% ====================================================================
%%                             Public API
%% ====================================================================
-export([storage_serve/4]).
%% ====================================================================
%%                             Constants
%% ====================================================================
%% ====================================================================
%%                            Main Function
%% ====================================================================
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