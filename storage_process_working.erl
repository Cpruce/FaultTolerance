%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(storage_process_working).

-import(key_value_node_working, [println/1, println/2]).
%% ====================================================================
%%                             Public API
%% ====================================================================
-export([init_store/4, hash/2]).
%% ====================================================================
%%                             Constants
%% ====================================================================
%% ====================================================================
%%                            Main Function
%% ====================================================================


% getStorageProcessName/1
% converts a storage process id to its globally registered name.
getStorageProcessName(Id) ->
  "StorageProcess" ++ integer_to_list(Id).

init_store(M, Id, Neighbors, Storage) ->
  Table = ets:new(table, [ordered_set]),
  % Backups = backup_neighbors(Id, Neighbors),
  storage_serve(M, Id, Neighbors, Storage, Table).

%% primary storage service function; handles
%% general communication and functionality.
storage_serve(M, Id, Neighbors, Storage, Table) ->
  GlobalName = getStorageProcessName(Id),
  % register(list_to_atom("StorageProcess" ++ integer_to_list(Id)), self()),
  receive 
    % ============================== STORE ====================================
    {Pid, Ref, store, Key, Value} ->
      println(""),
      println("~s> Received store command at key ~p of value ~p from ~p",
        [GlobalName, Key, Value, Pid]),
      HashValue = hash(Key, M),
      println("~s> Hashed value of the key: ~p", [GlobalName, HashValue]),
      case HashValue == Id of
        true ->
          % operation to be done at this process
          % save old value, replace it, and send message back
          case ets:lookup(Table, Key) of
            [] ->
              % this means there is no key before.
              ets:insert(Table, {Key, Value}),
              println("~s> {~p, ~p} stored. The key is brand new!",
                [GlobalName, Key, Value]),
              Pid ! {Ref, stored, no_value};

            [{_OldKey, OldValue}] ->
              println("~s> {~p, ~p} stored. The key existed before this store."
                +++ "The old value was ~p", [GlobalName, OldValue, Key, Value]),
              Pid ! {Ref, stored, OldValue};

            _ ->
              println("~s> We should not arrive at this stage! This can mean" ++
                "the table may be incorrectly set up to use multiset instead of set.")
            end;

        false ->
          % Pass on computation
          % determine the recipient to forward to -- see algorithm.pdf for
          % how we get this number.
          Diff = HashValue - Id,
          DiffPositive = case Diff < 0 of
            true -> Diff + round(math:pow(2, M));
            false -> Diff
          end,
          % if r = 2^{a_s} + 2^{a_{s - 1}} + ...  where a_s > _{s - 1} > ...
          % then we are trying to determine a_s, given r.
          MaxPowerOfTwo = floor(math:log(DiffPositive) / math:log(2)),
          ForwardedID = (Id + round(math:pow(2, MaxPowerOfTwo))) rem round(math:pow(2, M)),
          ForwardedRecipient = getStorageProcessName(ForwardedID),
          println("~s> The hash value does not match with this id. Forwarding the request to ~s...",
            [GlobalName, ForwardedRecipient]),
          % println("Check globally registered names: ~p", [global:registered_names()]),
          global:send(ForwardedRecipient, {Pid, Ref, store, Key, Value})
      end;
    % ============================== STORED ===================================
    {_Ref, stored, OldValue} -> 
      case OldValue == no_value of
        true -> 
          println("~s> No previously stored value. Store successful.", [GlobalName]);
        false ->
          println("~s> The old value was ~p. Store successful.", [GlobalName, OldValue])
      end;
    % ============================== RETRIEVE =================================
    {Pid, Ref, retrieve, Key} ->
      ok;
    % ============================== RETRIEVED ================================
    {Ref, retrieved, Value} -> ok;
    % ============================= FIRST KEY =================================
    {Pid, Ref, first_key} -> ok;
    % ============================== LAST KEY =================================
    {Pid, Ref, last_key} -> ok;
    % ============================== NUM KEYS =================================
    {Pid, Ref, num_keys} -> ok;
    % ============================== NODE LIST ================================
    {Pid, Ref, node_list} -> ok;
    {Ref, result, Result} -> ok;
    {Ref, failure} -> ok
    %{Name, "Hi there"} -> global:send(Name, {node(), "Yo"})
  end,
  storage_serve(M, Id, Neighbors, Storage, Table).

% floor function, taken from http://schemecookbook.org/Erlang/NumberRounding
floor(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T - 1;
        Pos when Pos > 0 -> T;
        _ -> T
    end.

%% hash function to uniformly distribute among 
%% storage processes.
hash(Str, M) when M >= 0 -> str_sum(Str) rem round(math:pow(2, M));
hash(_, _) -> -1.   %% error if no storage
      %% processes are open.

%% sum digits in string
str_sum([]) -> 0;
str_sum([X|XS]) -> X + str_sum(XS).

%% compute M from 2^M
compute_power2(N) when N < 2 -> 0;
compute_power2(N) -> 
  1 + compute_power2(math:pow(N, 1/2)).
