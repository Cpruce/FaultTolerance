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

init_store(M, Id, Neighbors, _Dummy) ->
  Storage = ets:new(table, [ordered_set]),
  NodeName = "Bobby",
  % Backups = backup_neighbors(Id, Neighbors),
  storage_serve(M, NodeName, Id, Neighbors, Storage).

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
storage_serve(M, NodeName, Id, Neighbors, Storage) ->
  storage_serve_once(M, NodeName, Id, Neighbors, Storage),
  storage_serve(M, NodeName, Id, Neighbors, Storage).

storage_serve_once(M, NodeName, Id, Neighbors, Storage) ->
  GlobalName = getStorageProcessName(Id),
  TwoToTheM = round(math:pow(2, M)),
  println(""),
  % register(list_to_atom("StorageProcess" ++ integer_to_list(Id)), self()),
  receive 
    % =========================================================================
    % ============================== STORE ====================================
    % =========================================================================
    {Pid, Ref, store, Key, Value} ->
      println("~s:~p > Received store command at key ~p of value ~p from ~p",
        [GlobalName, Ref, Key, Value, Pid]),
      HashValue = hash(Key, M),
      println("~s:~p > Hashed value of the key: ~p", [GlobalName, Ref, HashValue]),
      case HashValue == Id of
        true ->
          % operation to be done at this process
          % save old value, replace it, and send message back
          case ets:lookup(Storage, Key) of
            [] ->
              % this means there is no key before.
              ets:insert(Storage, {Key, Value}),
              println("~s:~p > {~p, ~p} stored. The key is brand new!",
                [GlobalName, Ref, Key, Value]),
              println("~s:~p > All of the key-value pairs stored by this storage process: ~p",
                [GlobalName, Ref, ets:match(Storage, '$0')]),
              Pid ! {Ref, stored, no_value};

            [{_OldKey, OldValue}] ->
              ets:insert(Storage, {Key, Value}),
              println("~s:~p > {~p, ~p} stored. The key existed before this store. "
                ++ "The old value was ~p.", [GlobalName, Ref, Key, Value, OldValue]),
              println("~s:~p > All of the key-value pairs stored by this storage process: ~p",
                [GlobalName, Ref, ets:match(Storage, '$0')]),
              Pid ! {Ref, stored, OldValue};

            _ ->
              println("~s:~p > We should not arrive at this stage! This can mean " ++
                "the table may be incorrectly set up to use multiset instead of set.",
                [GlobalName, Ref])
          end;

        false ->
          % Pass on computation.
          % Determine the recipient to forward to.
          ForwardedID = calculate_forwarded_id(Id, HashValue, M),
          ForwardedRecipient = getStorageProcessName(ForwardedID),
          println("~s:~p > The hash value does not match with this id. "
            ++ "Forwarding the store request to ~s...",
            [GlobalName, Ref, ForwardedRecipient]),
          % println("Check globally registered names: ~p", [global:registered_names()]),
          global:send(ForwardedRecipient, {Pid, Ref, store, Key, Value})
    end;

    % =========================================================================
    % ============================== STORED ===================================
    % =========================================================================
    {Ref, stored, OldValue} -> 
      case OldValue == no_value of
        true ->
          println("~s:~p > No previously stored value. Store successful.",
            [GlobalName, Ref]);
        false ->
          println("~s:~p > The old value was ~p. Store successful.",
            [GlobalName, Ref, OldValue])
      end;

    % =========================================================================
    % ============================== RETRIEVE =================================
    % =========================================================================
    {Pid, Ref, retrieve, Key} ->
      println("~s:~p > Received retrieve command at key ~p from ~p",
        [GlobalName, Ref, Key, Pid]),
      HashValue = hash(Key, M),
      println("~s:~p > Hashed value of the key: ~p", [GlobalName, Ref, HashValue]),
      case HashValue == Id of
        true ->
          % operation to be done at this process
          % save old value, replace it, and send message back
          case ets:lookup(Storage, Key) of
            [] ->
              % this means there is no key before.
              println("~s:~p > The key ~p did not exist in the system.",
                [GlobalName, Ref, Key]),
              Pid ! {Ref, retrieved, no_value};

            [{_OldKey, Value}] ->
              println("~s:~p > {~p, ~p} retrieved. The key existed in the system. "
                ++ "The value is ~p", [GlobalName, Ref, Value, Key, Value]),
              Pid ! {Ref, retrieved, Value};

            _ ->
              println("~s:~p > We should not arrive at this stage! This can mean" ++
                "the table may be incorrectly set up to use multiset instead of set.",
                [GlobalName, Ref])
          end;

        false ->
          % Pass on computation
          % determine the recipient to forward to 
          ForwardedID = calculate_forwarded_id(Id, HashValue, M),
          ForwardedRecipient = getStorageProcessName(ForwardedID),
          println("~s:~p > The hash value does not match with this id. "
            ++ "Forwarding the retrieve request to ~s...",
            [GlobalName, Ref, ForwardedRecipient]),
          % println("Check globally registered names: ~p", [global:registered_names()]),
          global:send(ForwardedRecipient, {Pid, Ref, retrieve, Key})
      end;

    % =========================================================================
    % ============================== RETRIEVED ================================
    % =========================================================================
    {Ref, retrieved, Value} ->
      println("~s:~p > Received a retrieved message.", [GlobalName, Ref]),
      println("~s:~p > Should have got this command if I were the outside world. "
        ++ "But I'm also cool outputing it too.", [GlobalName, Ref]),
      case Value == no_value of
        true -> 
          println("~s:~p > The key does not exist.",
            [GlobalName, Ref]);
        false ->
          println("~s:~p > The value for the requested key is ~p.",
            [GlobalName, Ref, Value])
      end;

    % =========================================================================
    % ============================= FIRST KEY =================================
    % =========================================================================
    {Pid, Ref, first_key} ->
      println("~s:~p > Received first_key command.", [GlobalName, Ref]),
      println("~s:~p > Forwarding a request to a helper request on the same process...",
        [GlobalName, Ref]),
      self() ! {self(), Ref, first_key_for_the_next_k_processes_inclusive, TwoToTheM, M + 1},
      storage_serve_once(M, NodeName, Id, Neighbors, Storage),
      receive
        {_NewRef, first_key_result_for_the_next_k_processes_inclusive, Result} ->
          ListResult = case Result of
            '$end_of_table' ->
              [];
            _ ->
              [Result]
          end,
          Pid ! {Ref, result, ListResult}
      end;

    {Pid, Ref, first_key_for_the_next_k_processes_inclusive, LookAhead, NumLookAhead} ->
      println("~s:~p > Received first_key_for_the_next_k_processes_inclusive command "
        ++ "with lookahead (including self) of ~p and num lookahead of ~p.",
        [GlobalName, Ref, LookAhead, NumLookAhead]),
      Result = case NumLookAhead of
        1 ->
          ets:first(Storage);
        _ ->
          % The summary table is more like a list, but we use an 
          % ordered_set, duplicate_bag ets table for convenience.
          % each element will be a singleton tuple
          SummaryTable = ets:new(summary_table, [ordered_set, duplicate_bag]),
          % start with the first key from this process
          ets:insert(SummaryTable, {ets:first(Storage)}),
          NeighborsWithLookAhead = [
              {
                % a tuple of size 3
                (Id + round(math:pow(2, K))) rem TwoToTheM,
                round(math:pow(2, K)),
                % the number of processes to lookahead (including self)
                K + 1
              }
              % we already lookahead at itself. So we will look ahead using
              % the parameters [0, 1, 2, ..., NumLookAhead - 2],
              % which has the total number of things in it being NumLookAhead - 2.
              || K <- lists:seq(0, NumLookAhead - 2)
          ],
          println("~s:~p > Plan to send subcomputation requests to storage processes with id ~p",
            [
              GlobalName,
              Ref,
              lists:map(fun({A, _, _}) -> A end, NeighborsWithLookAhead)
            ]
          ),
          % send a request to compute first key for the next LookAhead processes
          lists:map(
            fun({ProcessId, ProcessLookAhead, NumProcessesLookAhead}) -> 
              TargetName = getStorageProcessName(ProcessId),
              println("~s:~p > Sending subcomputation for the first_key request "
                ++ "to ~p with lookahead (including self) of ~p and the number "
                ++ "of processes (including self) to lookahead of ~p",
                [GlobalName, Ref, TargetName, ProcessLookAhead, NumProcessesLookAhead]),
              global:send(
                TargetName,
                {self(), make_ref(), first_key_for_the_next_k_processes_inclusive,
                  ProcessLookAhead, NumProcessesLookAhead}
              )
            end,
            NeighborsWithLookAhead
          ),
          % expect the table to eventually have LookAhead elements
          wait_and_get_the_first_key(GlobalName, self(), Ref, SummaryTable, NumLookAhead)
      end,
      println(""),
      println("~s:~p > The first key for the next ~p processes starting from ~p is ~p",
        [GlobalName, Ref, LookAhead, GlobalName, Result]),
      Pid ! {Ref, first_key_result_for_the_next_k_processes_inclusive, Result};

    % =========================================================================
    % ============================== LAST KEY =================================
    % =========================================================================
    {Pid, Ref, last_key} ->
      println("~s:~p > Received last_key command.", [GlobalName, Ref]),
      println("~s:~p > Forwarding a request to a helper request on the same process...",
        [GlobalName, Ref]),
      self() ! {self(), Ref, last_key_for_the_next_k_processes_inclusive, TwoToTheM, M + 1},
      storage_serve_once(M, NodeName, Id, Neighbors, Storage),
      receive
        {_NewRef, last_key_result_for_the_next_k_processes_inclusive, Result} ->
          ListResult = case Result of
            '$end_of_table' ->
              [];
            _ ->
              [Result]
          end,
          Pid ! {Ref, result, ListResult}
      end;

    {Pid, Ref, last_key_for_the_next_k_processes_inclusive, LookAhead, NumLookAhead} ->
      println("~s:~p > Received last_key_for_the_next_k_processes_inclusive command "
        ++ "with lookahead (including self) of ~p and num lookahead of ~p.",
        [GlobalName, Ref, LookAhead, NumLookAhead]),
      Result = case NumLookAhead of
        1 ->
          ets:last(Storage);
        _ ->
          % The summary table is more like a list, but we use an 
          % ordered_set, duplicate_bag ets table for convenience.
          % each element will be a singleton tuple
          SummaryTable = ets:new(summary_table, [ordered_set, duplicate_bag]),
          % start with the last key from this process
          ets:insert(SummaryTable, {ets:last(Storage)}),
          NeighborsWithLookAhead = [
              {
                % a tuple of size 3
                (Id + round(math:pow(2, K))) rem TwoToTheM,
                round(math:pow(2, K)),
                % the number of processes to lookahead (including self)
                K + 1
              }
              % we already lookahead at itself. So we will look ahead using
              % the parameters [0, 1, 2, ..., NumLookAhead - 2],
              % which has the total number of things in it being NumLookAhead - 2.
              || K <- lists:seq(0, NumLookAhead - 2)
          ],
          println("~s:~p > Plan to send subcomputation requests to storage processes with id ~p",
            [
              GlobalName,
              Ref,
              lists:map(fun({A, _, _}) -> A end, NeighborsWithLookAhead)
            ]
          ),
          % send a request to compute last key for the next LookAhead processes
          lists:map(
            fun({ProcessId, ProcessLookAhead, NumProcessesLookAhead}) -> 
              TargetName = getStorageProcessName(ProcessId),
              println("~s:~p > Sending subcomputation for the last_key request "
                ++ "to ~p with lookahead (including self) of ~p and the number "
                ++ "of processes (including self) to lookahead of ~p",
                [GlobalName, Ref, TargetName, ProcessLookAhead, NumProcessesLookAhead]),
              global:send(
                TargetName,
                {self(), make_ref(), last_key_for_the_next_k_processes_inclusive,
                  ProcessLookAhead, NumProcessesLookAhead}
              )
            end,
            NeighborsWithLookAhead
          ),
          % expect the table to eventually have LookAhead elements
          wait_and_get_the_last_key(GlobalName, self(), Ref, SummaryTable, NumLookAhead)
      end,
      println(""),
      println("~s:~p > The last key for the next ~p processes starting from ~p is ~p",
        [GlobalName, Ref, LookAhead, GlobalName, Result]),
      Pid ! {Ref, last_key_result_for_the_next_k_processes_inclusive, Result};

    % =========================================================================
    % ============================== NUM KEYS =================================
    % =========================================================================
    {Pid, Ref, num_keys} ->
      println("~s:~p > Received num_keys command.", [GlobalName, Ref]),
      println("~s:~p > Forwarding a request to a helper request on the same process...",
        [GlobalName, Ref]),
      self() ! {self(), Ref, num_keys_for_the_next_k_processes_inclusive, TwoToTheM, M + 1},
      storage_serve_once(M, NodeName, Id, Neighbors, Storage),
      receive
        {_NewRef, num_keys_result_for_the_next_k_processes_inclusive, Result} ->
          Pid ! {Ref, result, Result}
      end;

    {Pid, Ref, num_keys_for_the_next_k_processes_inclusive, LookAhead, NumLookAhead} ->
      println("~s:~p > Received num_keys_for_the_next_k_processes_inclusive command "
        ++ "with lookahead (including self) of ~p and num lookahead of ~p.",
        [GlobalName, Ref, LookAhead, NumLookAhead]),
      Result = case NumLookAhead of
        1 ->
          length(ets:match(Storage, '$1'));
        _ ->
          % The summary table is more like a list, but we use an 
          % ordered_set, duplicate_bag ets table for convenience.
          % each element will be a singleton tuple
          SummaryTable = ets:new(summary_table, [ordered_set, duplicate_bag]),
          % start with the last key from this process
          ets:insert(SummaryTable, {length(ets:match(Storage, '$1'))}),
          NeighborsWithLookAhead = [
              {
                % a tuple of size 3
                (Id + round(math:pow(2, K))) rem TwoToTheM,
                round(math:pow(2, K)),
                % the number of processes to lookahead (including self)
                K + 1
              }
              % we already lookahead at itself. So we will look ahead using
              % the parameters [0, 1, 2, ..., NumLookAhead - 2],
              % which has the total number of things in it being NumLookAhead - 2.
              || K <- lists:seq(0, NumLookAhead - 2)
          ],
          println("~s:~p > Plan to send subcomputation requests to storage processes with id ~p",
            [
              GlobalName,
              Ref,
              lists:map(fun({A, _, _}) -> A end, NeighborsWithLookAhead)
            ]
          ),
          % send a request to compute last key for the next LookAhead processes
          lists:map(
            fun({ProcessId, ProcessLookAhead, NumProcessesLookAhead}) -> 
              TargetName = getStorageProcessName(ProcessId),
              println("~s:~p > Sending subcomputation for the num_keys request "
                ++ "to ~p with lookahead (including self) of ~p and the number "
                ++ "of processes (including self) to lookahead of ~p",
                [GlobalName, Ref, TargetName, ProcessLookAhead, NumProcessesLookAhead]),
              global:send(
                TargetName,
                {self(), make_ref(), num_keys_for_the_next_k_processes_inclusive,
                  ProcessLookAhead, NumProcessesLookAhead}
              )
            end,
            NeighborsWithLookAhead
          ),
          % expect the table to eventually have LookAhead elements
          wait_and_get_num_keys(GlobalName, self(), Ref, SummaryTable, NumLookAhead)
      end,
      println(""),
      println("~s:~p > The last key for the next ~p processes starting from ~p is ~p",
        [GlobalName, Ref, LookAhead, GlobalName, Result]),
      Pid ! {Ref, num_keys_result_for_the_next_k_processes_inclusive, Result};

    % =========================================================================
    % ============================== NODE LIST ================================
    % =========================================================================
    {Pid, Ref, node_list} ->
      println("~s:~p > Received node_list command.", [GlobalName, Ref]),
      println("~s:~p > Forwarding a request to a helper request on the same process...",
        [GlobalName, Ref]),
      self() ! {self(), Ref, node_list_for_the_next_k_processes_inclusive, TwoToTheM, M + 1},
      storage_serve_once(M, NodeName, Id, Neighbors, Storage),
      receive
        {_NewRef, node_list_result_for_the_next_k_processes_inclusive, Result} ->
          Pid ! {Ref, result, Result}
      end;

    {Pid, Ref, node_list_for_the_next_k_processes_inclusive, LookAhead, NumLookAhead} ->
      println("~s:~p > Received node_list_for_the_next_k_processes_inclusive command "
        ++ "with lookahead (including self) of ~p and num lookahead of ~p.",
        [GlobalName, Ref, LookAhead, NumLookAhead]),
      Result = case NumLookAhead of
        1 ->
          [NodeName];
        _ ->
          % The summary table is more like a list, but we use an 
          % ordered_set, duplicate_bag ets table for convenience.
          % each element will be a singleton tuple
          SummaryTable = ets:new(summary_table, [ordered_set, duplicate_bag]),
          % start with the last key from this process
          ets:insert(SummaryTable, {[NodeName]}),
          NeighborsWithLookAhead = [
              {
                % a tuple of size 3
                (Id + round(math:pow(2, K))) rem TwoToTheM,
                round(math:pow(2, K)),
                % the number of processes to lookahead (including self)
                K + 1
              }
              % we already lookahead at itself. So we will look ahead using
              % the parameters [0, 1, 2, ..., NumLookAhead - 2],
              % which has the total number of things in it being NumLookAhead - 2.
              || K <- lists:seq(0, NumLookAhead - 2)
          ],
          println("~s:~p > Plan to send subcomputation requests to storage processes with id ~p",
            [
              GlobalName,
              Ref,
              lists:map(fun({A, _, _}) -> A end, NeighborsWithLookAhead)
            ]
          ),
          % send a request to compute last key for the next LookAhead processes
          lists:map(
            fun({ProcessId, ProcessLookAhead, NumProcessesLookAhead}) -> 
              TargetName = getStorageProcessName(ProcessId),
              println("~s:~p > Sending subcomputation for the node_list request "
                ++ "to ~p with lookahead (including self) of ~p and the number "
                ++ "of processes (including self) to lookahead of ~p",
                [GlobalName, Ref, TargetName, ProcessLookAhead, NumProcessesLookAhead]),
              global:send(
                TargetName,
                {self(), make_ref(), node_list_for_the_next_k_processes_inclusive,
                  ProcessLookAhead, NumProcessesLookAhead}
              )
            end,
            NeighborsWithLookAhead
          ),
          % expect the table to eventually have LookAhead elements
          wait_and_get_node_list(GlobalName, self(), Ref, SummaryTable, NumLookAhead)
      end,
      println(""),
      println("~s:~p > The last key for the next ~p processes starting from ~p is ~p",
        [GlobalName, Ref, LookAhead, GlobalName, Result]),
      Pid ! {Ref, node_list_result_for_the_next_k_processes_inclusive, Result};
    % =========================================================================
    % =============================== RESULT ==================================
    % =========================================================================
    {Ref, result, Result} ->
      println("~s:~p > Received a result message.", [GlobalName, Ref]),
      println("~s:~p > Should have got this command if I were the outside world. "
        ++ "But I'm also cool outputing it too.", [GlobalName, Ref]),
      println("~s:~p > The result is ~p.", [GlobalName, Ref, Result]);

    % =========================================================================
    % ============================== FAILURE ==================================
    % =========================================================================
    {Ref, failure} ->
      println("~s:~p > Received a failure message.", [GlobalName, Ref]),
      println("~s:~p > Should have got this command if I were the outside world. "
        ++ "But I'm also cool outputing it too.", [GlobalName, Ref]),
      println("~s:~p > ============ FAILURE ============.", [GlobalName, Ref]);

    % =========================================================================
    % ================================ LEAVE ==================================
    % =========================================================================
    {Pid, Ref, leave} -> ok
  end.


wait_and_get_the_first_key(GlobalName, Pid, Ref, Storage, ExpectedLength) ->
  println(""),
  println("~s:~p > [first_key subcalculation] Waiting...", [GlobalName, Ref]),
  println("~s:~p > Current length of the results returned to this process: ~p",
    [GlobalName, Ref, length(ets:match(Storage, '$1'))]),
  println("~s:~p > Things in the table of results returned to this process: ~p",
    [GlobalName, Ref, ets:match(Storage, '$1')]),
  println("~s:~p > Expected length of the results returned to this process: ~p",
    [GlobalName, Ref, ExpectedLength]),

  case length(ets:match(Storage, '$1')) of
    ExpectedLength ->
      println("~s:~p > Quorum reached!", [GlobalName, Ref]),
      ets:delete(Storage, '$end_of_table'),
      println("~s:~p > After deleting '$end_of_table', the table looks like: ~p",
        [GlobalName, Ref, ets:match(Storage, '$1')]),
      % -- BEGIN: The following lines should not be necessary had ets:first worked properly ---
      AllElementsUnparsed = ets:match(Storage, '$1'),
      AllElementsParsed = lists:map(fun([{A}]) -> A end, AllElementsUnparsed),
      SortedList = lists:sort(AllElementsParsed),
      Result = case SortedList of
        [] ->
          '$end_of_table';
        _ ->
          hd(SortedList)
      end,
      % -- END: The lines above should not be necessary had ets:first worked properly ---
      % Result = ets:first(Storage),
      println("~s:~p > The first key for the next ~p processes starting from ~p is ~p",
        [GlobalName, Ref, ExpectedLength, GlobalName, Result]),
      Result;
    _ ->
      receive
        {NewRef, first_key_result_for_the_next_k_processes_inclusive, PartialResult} ->
          println("~s:~p > Received a partial result! It is ~p.",
            [GlobalName, NewRef, PartialResult]),
          ets:insert(Storage, {PartialResult}),
          wait_and_get_the_first_key(GlobalName, Pid, Ref, Storage, ExpectedLength);
        {NewRef, failure} ->
          % propagate failure
          Pid ! {NewRef, failure}
      end
  end.

wait_and_get_the_last_key(GlobalName, Pid, Ref, Storage, ExpectedLength) ->
  println(""),
  println("~s:~p > [last_key subcalculation] Waiting...", [GlobalName, Ref]),
  println("~s:~p > Current length of the results returned to this process: ~p",
    [GlobalName, Ref, length(ets:match(Storage, '$1'))]),
  println("~s:~p > Things in the table of results returned to this process: ~p",
    [GlobalName, Ref, ets:match(Storage, '$1')]),
  println("~s:~p > Expected length of the results returned to this process: ~p",
    [GlobalName, Ref, ExpectedLength]),

  case length(ets:match(Storage, '$1')) of
    ExpectedLength ->
      println("~s:~p > Quorum reached!", [GlobalName, Ref]),
      ets:delete(Storage, '$end_of_table'),
      println("~s:~p > After deleting '$end_of_table', the table looks like: ~p",
        [GlobalName, Ref, ets:match(Storage, '$1')]),
      % -- BEGIN: The following lines should not be necessary had ets:last worked properly ---
      AllElementsUnparsed = ets:match(Storage, '$1'),
      AllElementsParsed = lists:map(fun([{A}]) -> A end, AllElementsUnparsed),
      SortedList = lists:sort(AllElementsParsed),
      Result = case SortedList of
        [] ->
          '$end_of_table';
        _ ->
          lists:last(SortedList)
      end,
      % -- END: The lines above should not be necessary had ets:last worked properly ---
      % Result = ets:last(Storage),
      println("~s:~p > The last key for the next ~p processes starting from ~p is ~p",
        [GlobalName, Ref, ExpectedLength, GlobalName, Result]),
      Result;
    _ ->
      receive
        {NewRef, last_key_result_for_the_next_k_processes_inclusive, PartialResult} ->
          println("~s:~p > Received a partial result! It is ~p.",
            [GlobalName, NewRef, PartialResult]),
          ets:insert(Storage, {PartialResult}),
          wait_and_get_the_last_key(GlobalName, Pid, Ref, Storage, ExpectedLength);
        {NewRef, failure} ->
          % propagate failure
          Pid ! {NewRef, failure}
      end
  end.

wait_and_get_num_keys(GlobalName, Pid, Ref, Storage, ExpectedLength) ->
  println(""),
  println("~s:~p > [num_keys subcalculation] Waiting...", [GlobalName, Ref]),
  println("~s:~p > Current length of the results returned to this process: ~p",
    [GlobalName, Ref, length(ets:match(Storage, '$1'))]),
  println("~s:~p > Things in the table of results returned to this process: ~p",
    [GlobalName, Ref, ets:match(Storage, '$1')]),
  println("~s:~p > Expected length of the results returned to this process: ~p",
    [GlobalName, Ref, ExpectedLength]),

  case length(ets:match(Storage, '$1')) of
    ExpectedLength ->
      println("~s:~p > Quorum reached!", [GlobalName, Ref]),
      AllElementsUnparsed = ets:match(Storage, '$1'),
      AllElementsParsed = lists:map(fun([{A}]) -> A end, AllElementsUnparsed),
      Result = lists:sum(AllElementsParsed),
      println("~s:~p > The num keys for the next ~p processes starting from ~p is ~p",
        [GlobalName, Ref, ExpectedLength, GlobalName, Result]),
      Result;
    _ ->
      receive
        {NewRef, num_keys_result_for_the_next_k_processes_inclusive, PartialResult} ->
          println("~s:~p > Received a partial result! It is ~p.",
            [GlobalName, NewRef, PartialResult]),
          ets:insert(Storage, {PartialResult}),
          wait_and_get_num_keys(GlobalName, Pid, Ref, Storage, ExpectedLength);
        {NewRef, failure} ->
          % propagate failure
          Pid ! {NewRef, failure}
      end
  end.

wait_and_get_node_list(GlobalName, Pid, Ref, Storage, ExpectedLength) ->
  println(""),
  println("~s:~p > [node_list subcalculation] Waiting...", [GlobalName, Ref]),
  println("~s:~p > Current length of the results returned to this process: ~p",
    [GlobalName, Ref, length(ets:match(Storage, '$1'))]),
  println("~s:~p > Things in the table of results returned to this process: ~p",
    [GlobalName, Ref, ets:match(Storage, '$1')]),
  println("~s:~p > Expected length of the results returned to this process: ~p",
    [GlobalName, Ref, ExpectedLength]),

  case length(ets:match(Storage, '$1')) of
    ExpectedLength ->
      println("~s:~p > Quorum reached!", [GlobalName, Ref]),
      AllElementsUnparsed = ets:match(Storage, '$1'),
      AllElementsParsed = lists:map(fun([{A}]) -> A end, AllElementsUnparsed),
      Result = unique(lists:append(AllElementsParsed)),
      println("~s:~p > The node list for the next ~p processes starting from ~p is ~p",
        [GlobalName, Ref, ExpectedLength, GlobalName, Result]),
      Result;
    _ ->
      receive
        {NewRef, node_list_result_for_the_next_k_processes_inclusive, PartialResult} ->
          println("~s:~p > Received a partial result! It is ~p.",
            [GlobalName, NewRef, PartialResult]),
          ets:insert(Storage, {PartialResult}),
          wait_and_get_node_list(GlobalName, Pid, Ref, Storage, ExpectedLength);
        {NewRef, failure} ->
          % propagate failure
          Pid ! {NewRef, failure}
      end
  end.

% -------------------------------------------------------------------------
% ==================== Neighbors-Related Functions ========================
% -------------------------------------------------------------------------
% Constantly query neighbor processes to make sure that they are still
% there. If one is gone, delete fork to that philosopher and remove from
% neighbors list, sufficiently removing the edge. Otherwise, keep
% philosophizing.
check_neighbors([], _)-> ok;
check_neighbors([X|XS], ParentPid) ->
  spawn(fun() -> monitor_neighbor(X, ParentPid) end),
  check_neighbors(XS, ParentPid).

monitor_neighbor(Name, ParentPid) -> 
  erlang:monitor(process, {backup, Name}), %{RegName, Node}
  receive
    {'DOWN', _Ref, process, _Pid, normal} ->
      ParentPid ! {self(), check, Name};
    {'DOWN', _Ref, process, _Pid, _Reason} ->
      ParentPid ! {self(), missing, Name}
  end.

% -------------------------------------------------------------------------
% ======================= Other Helper Functions ==========================
% -------------------------------------------------------------------------
% given a list, return a list of unique elements
unique(L) ->
    unique_tail_recursion(L, []).

unique_tail_recursion([], Result) -> Result;
unique_tail_recursion([X|XS], Result) ->
  case lists:member(X, Result) of
      true -> unique_tail_recursion(XS, Result);
      false -> unique_tail_recursion(XS, [X|Result])
  end.

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
