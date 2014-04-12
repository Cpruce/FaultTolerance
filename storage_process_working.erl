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

% calculate_forwarded_id/3
% given the current ID and target ID and M, calculate the neigboring id to forward to.
% see algorithm.pdf for how we get this number.
calculate_forwarded_id(Id, Target, M) ->
  Diff = Target - Id,
  DiffPositive = case Diff < 0 of
    true -> Diff + round(math:pow(2, M));
    false -> Diff
  end,
  % if r:= DiffPositive = 2^{a_s} + 2^{a_{s - 1}} + ...  where a_s > _{s - 1} > ...
  % then we are trying to determine a_s, given r.
  MaxPowerOfTwo = floor(math:log(DiffPositive) / math:log(2)),
  ForwardedID = (Id + round(math:pow(2, MaxPowerOfTwo))) rem round(math:pow(2, M)),
  ForwardedID.

%% primary storage service function; handles
%% general communication and functionality.
storage_serve(M, Id, Neighbors, Storage, Table) ->
  GlobalName = getStorageProcessName(Id),
  println(""),
  % register(list_to_atom("StorageProcess" ++ integer_to_list(Id)), self()),
  receive 
    % ============================== STORE ====================================
    {Pid, Ref, store, Key, Value} ->
      println("~s:~p> Received store command at key ~p of value ~p from ~p",
        [GlobalName, Ref, Key, Value, Pid]),
      HashValue = hash(Key, M),
      println("~s:~p> Hashed value of the key: ~p", [GlobalName, Ref, HashValue]),
      case HashValue == Id of
        true ->
          % operation to be done at this process
          % save old value, replace it, and send message back
          case ets:lookup(Table, Key) of
            [] ->
              % this means there is no key before.
              ets:insert(Table, {Key, Value}),
              println("~s:~p> {~p, ~p} stored. The key is brand new!",
                [GlobalName, Ref, Key, Value]),
              println("~s:~p> All of the key-value pairs stored by this storage process: ~p",
                [GlobalName, Ref, ets:match(Table, '$0')]),
              Pid ! {Ref, stored, no_value};

            [{_OldKey, OldValue}] ->
              println("~s:~p> {~p, ~p} stored. The key existed before this store. "
                ++ "The old value was ~p.", [GlobalName, Ref, OldValue, Key, Value]),
              Pid ! {Ref, stored, OldValue};

            _ ->
              println("~s:~p> We should not arrive at this stage! This can mean " ++
                "the table may be incorrectly set up to use multiset instead of set.",
                [GlobalName, Ref])
          end;

        false ->
          % Pass on computation
          % determine the recipient to forward to 
          ForwardedID = calculate_forwarded_id(Id, HashValue, M),
          ForwardedRecipient = getStorageProcessName(ForwardedID),
          println("~s:~p> The hash value does not match with this id. "
            ++ "Forwarding the store request to ~s...",
            [GlobalName, Ref, ForwardedRecipient]),
          % println("Check globally registered names: ~p", [global:registered_names()]),
          global:send(ForwardedRecipient, {Pid, Ref, store, Key, Value})
    end;
    % ============================== STORED ===================================
    {Ref, stored, OldValue} -> 
      case OldValue == no_value of
        true -> 
          println("~s:~p> No previously stored value. Store successful.",
            [GlobalName, Ref]);
        false ->
          println("~s:~p> The old value was ~p. Store successful.",
            [GlobalName, Ref, OldValue])
      end;
    % ============================== RETRIEVE =================================
    {Pid, Ref, retrieve, Key} ->
      println("~s:~p> Received retrieve command at key ~p from ~p",
        [GlobalName, Ref, Key, Pid]),
      HashValue = hash(Key, M),
      println("~s:~p> Hashed value of the key: ~p", [GlobalName, Ref, HashValue]),
      case HashValue == Id of
        true ->
          % operation to be done at this process
          % save old value, replace it, and send message back
          case ets:lookup(Table, Key) of
            [] ->
              % this means there is no key before.
              println("~s:~p> The key ~p did not exist in the system.",
                [GlobalName, Ref, Key]),
              Pid ! {Ref, retrieved, no_value};

            [{_OldKey, Value}] ->
              println("~s:~p> {~p, ~p} retrieved. The key existed in the system. "
                ++ "The value is ~p", [GlobalName, Ref, Value, Key, Value]),
              Pid ! {Ref, retrieved, Value};

            _ ->
              println("~s:~p> We should not arrive at this stage! This can mean" ++
                "the table may be incorrectly set up to use multiset instead of set.",
                [GlobalName, Ref])
          end;

        false ->
          % Pass on computation
          % determine the recipient to forward to 
          ForwardedID = calculate_forwarded_id(Id, HashValue, M),
          ForwardedRecipient = getStorageProcessName(ForwardedID),
          println("~s:~p> The hash value does not match with this id. "
            ++ "Forwarding the retrieve request to ~s...",
            [GlobalName, Ref, ForwardedRecipient]),
          % println("Check globally registered names: ~p", [global:registered_names()]),
          global:send(ForwardedRecipient, {Pid, Ref, retrieve, Key})
      end;
    % ============================== RETRIEVED ================================
    {Ref, retrieved, Value} ->
      case Value == no_value of
        true -> 
          println("~s:~p> The key does not exist.",
            [GlobalName, Ref]);
        false ->
          println("~s:~p> The value for the requested key is ~p.",
            [GlobalName, Ref, Value])
      end;
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
