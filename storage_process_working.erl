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
-export([storage_serve/4, hash/2]).
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

%% primary storage service function; handles
%% general communication and functionality.
storage_serve(M, Id, Neighbors, Storage) ->
  GlobalName = getStorageProcessName(Id),
  % register(list_to_atom("StorageProcess" ++ integer_to_list(Id)), self()),
  receive 
    {Pid, Ref, store, Key, Value} ->
      println("~s> Received store command at key ~p of value ~p from ~p~n", [GlobalName, Key, Value, Pid]),
      HashValue = hash(Key, M),
      println("Hashed value of the key: ~p", [HashValue]),
      case HashValue == Id of
        % operation to be done at this process
        true ->
          ok;
          % save old value, replace it, and send message back
          % Oldval = dict:fetch(Key, Storage),
          % NewStore = dict:store(Key, Value, Storage),
          % Pid ! {self(), stored, Oldval};
        % pass on computation
        false ->
          % determine the recipient to forward to -- see algorithm.pdf for how we get this number.
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
          % Recv = find_neighbor(Neighbors, Id),    %% ADD IN ONLY IF 'i + 2^k'
          % Recv ! {Pid, self(), store, Key, Value}
          % global:send(Name, {"StorageProcess" ++ integer_to_list(Id), "Yo"})
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
    %{Name, "Hi there"} -> global:send(Name, {node(), "Yo"})
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

%% compute M from 2^M
compute_power2(N) when N < 2 -> 0;
compute_power2(N) -> 
  1 + compute_power2(math:pow(N, 1/2)).
