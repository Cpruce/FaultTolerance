%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(storage_process).

-import(key_value_node_temp, [print/1, print/2]).
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

%% find neighbor by Id
find_neighbor([], _Id) -> [];
find_neighbor(Neighbors, Id)->
  {Name, IdN, _Pid} = hd(Neighbors),
  case Id == IdN of
    true ->
      %% Id's match, return name.
      [Name];
    false ->
      find_neighbor(tl(Neighbors), Id)
  end.

%% primary storage service function; handles
%% general communication and functionality.
storage_serve(M, Id, Neighbors, Storage) ->
  register(list_to_atom("StorageProcess" ++ integer_to_list(Id)), self()),
  receive 
    {Pid, Ref, store, Key, Value} ->
      print("Received store command at key ~p of value ~p from ~p~n", [Key, Value, Pid]),  
      case hash(Key, M) == Id of
        % operation to be done at this process
        true ->
          % save old value, replace it, and send message back
          Oldval = dict:fetch(Key, Storage),
          NewStore = dict:store(Key, Value, Storage),
          Pid ! {self(), stored, Oldval};
        % pass on computation
        false ->
          % find and send to correct recipient
          Recv = find_neighbor(Neighbors, Id),    %% ADD IN ONLY IF 'i + 2^k'
          Recv ! {Pid, self(), store, Key, Value}
      end;
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

%% hash function to uniformly distribute among 
%% storage processes.
hash(Str, M) when M >= 0 -> str_sum(Str) rem (math:pow(2, M));
hash(_, _) -> -1.   %% error if no storage
      %% processes are open.

%% sum digits in string
str_sum([]) -> 0;
str_sum([X|XS]) -> $X + str_sum(XS).

%% compute M from 2^M
compute_power2(N) when N < 2 -> 0;
compute_power2(N) -> 
  1 + compute_power2(math:pow(N, 1/2)).
