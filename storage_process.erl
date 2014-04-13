% CSCI182E - Distributed Systems
%% Harvey TwoToTheMudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(storage_process).

-import(key_value_node, [println/1, println/2]).
-import(advertise_id, [init_snapshot/2]).
%% ====================================================================
%%                             Public API
%% ====================================================================
-export([init_store/4, x_store/6]).
%% ====================================================================
%%                             Constants
%% ====================================================================
%% ====================================================================
%%                            TwoToTheMain Function
%% ====================================================================

init_store(M, NodeName, Id, Neighbors)->
  	Storage = ets:new(table, [ordered_set]),
global:register_name(list_to_atom("StorageProcess" ++ integer_to_list(Id)), self()),
	println("Neighbors is ~p~n", [Neighbors]),
	storage_serve(M, NodeName, Id, Neighbors, Storage, []).%Backups). 

x_store(M, NodeName, Id, Neighbors, Storage, Backups)->
    Global = global:registered_names(),
    Regname =list_to_atom("StorageProcess" ++ integer_to_list(Id)), 
    case lists:member(Regname, Global) of
        true ->
            println("ev -> true"),
            timer:sleep(200);
        false ->
            continue
    end,
    println("Global = ~p", [Global]),
    global:register_name(Regname, self()),
    GlobalNow = global:registered_names(),
	println("Global is now = ~p", [GlobalNow]),
    println("Neighbors is ~p~n", [Neighbors]),
	storage_serve(M, NodeName, Id, Neighbors, Storage, Backups). 

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

% getStorageProcessName/1
% converts a storage process id to its globally registered name.
getStorageProcessName(Id) ->
  "StorageProcess" ++ integer_to_list(Id).

% floor function, taken from http://schemecookbook.org/Erlang/NumberRounding
floor(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T - 1;
        Pos when Pos > 0 -> T;
        _ -> T
    end.

%% backup neighbors in the ring
backup_neighbors(M, NodeName, _Id, [], _Storage, Backups) -> 
    println("Done backing up neighbors!"),
    [];
backup_neighbors(M, NodeName, Id, [IdN | Neighbors], Storage, Backups) -> 
	GlobalName = getStorageProcessName(Id),
    RecvNeigh = list_to_atom("StorageProcess"++integer_to_list(IdN)),
	println("Sending backup request to ~p~n", [RecvNeigh]),
    global:send(RecvNeigh, {self(), backup_request}),
	receive 
	 {Pid, backup_response, Backup} ->
		% create backup
		println("Backing up ~p~n", [RecvNeigh]),
		% monitor to see if backup needs to register
		monitor_neighbor(RecvNeigh, self()),
        backup_neighbors(M, NodeName, Id, Neighbors, Storage, Backups)++[Backup];

     {Pid, backup_request} ->
		% send storage back to be backed up
        global:send(Pid, {self(), backup_response, Storage}),
		backup_neighbors(M, NodeName, Id, Neighbors, Storage, Backups);
    
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
          case ets:lookup(Storage, Key) of
            [] ->
              % this means there is no key before.
              ets:insert(Storage, {Key, Value}),
              println("~s> {~p, ~p} stored. The key is brand new!",
                [GlobalName, Key, Value]),
              println("~s:~p> All of the key-value pairs stored by this storage process: ~p",
                [GlobalName, Ref, ets:match(Storage, '$0')]),
Pid ! {Ref, stored, no_value},
            backup_neighbors(M, NodeName, Id, [IdN | Neighbors], Storage,
                Backups);
              

            [{_OldKey, OldValue}] ->
              println("~s> {~p, ~p} stored. The key existed before this store."
                ++ "The old value was ~p", [GlobalName, OldValue, Key, Value]),
              Pid ! {Ref, stored, OldValue},
              backup_neighbors(M, NodeName, Id, [IdN | Neighbors], Storage, Backups);

            _ ->
              println("~s> We should not arrive at this stage! This can mean" ++
                "the table may be incorrectly set up to use multiset instead of
                set."),
                backup_neighbors(M, NodeName, Id, [IdN | Neighbors], Storage, Backups) end;

        false ->
          % Pass on computation.
          % Determine the recipient to forward to.
          ForwardedID = calculate_forwarded_id(Id, HashValue, M),
          ForwardedRecipient = getStorageProcessName(ForwardedID),
          println("~s> The hash value does not match with this id. Forwarding the request to ~s...",
            [GlobalName, ForwardedRecipient]),
          % println("Check globally registered names: ~p", [global:registered_names()]),
          global:send(ForwardedRecipient, {Pid, Ref, store, Key, Value}),
          backup_neighbors(M, NodeName, Id, [IdN | Neighbors], Storage, Backups)
end;
    % ============================== STORED ===================================
    {Ref, stored, OldValue} -> 
      case OldValue == no_value of
        true -> 
          println("~s:~p> No previously stored value. Store successful.",
              [GlobalName, Ref]),
          backup_neighbors(M, NodeName, Id, [IdN | Neighbors], Storage, Backups);
        false ->
          println("~s:~p> The old value was ~p. Store successful.",
              [GlobalName, Ref, OldValue]),
          backup_neighbors(M, NodeName, Id, [IdN | Neighbors], Storage, Backups)
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
          case ets:lookup(Storage, Key) of
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
      end,
      backup_neighbors(M, NodeName, Id, [IdN | Neighbors], Storage, Backups);
    % ============================== RETRIEVED ================================
    {Ref, retrieved, Value} ->  backup_neighbors(M, NodeName, Id, [IdN |
                Neighbors], Storage, Backups);    % ============================= FIRST KEY =================================
    {Pid, Ref, first_key} -> backup_neighbors(M, NodeName, Id, [IdN |
                Neighbors], Storage, Backups);
    % ============================== LAST KEY =================================
    {Pid, Ref, last_key} -> backup_neighbors(M, NodeName, Id, [IdN |
                Neighbors], Storage, Backups);    % ============================== NUM KEYS =================================
    {Pid, Ref, num_keys} -> backup_neighbors(M, NodeName, Id, [IdN |
                Neighbors], Storage, Backups);  
    %============================== NODE LIST ================================
    {Pid, rebalance} ->
			println("Received rebalance request from ~p~n", [Pid]),
        global:send(Pid, {self(), rebalance_response, Storage, Backups,
                Neighbors}),
        exit(normal);   
     
	 {_Ref, failure} ->
		println("Neighbor ~p crashed. Moving on.~n", [RecvNeigh]),
        backup_neighbors(M, NodeName, Id, Neighbors, Storage, Backups);

        _ ->
            println("Received something else"),
            backup_neighbors(M, NodeName, Id, Neighbors, Storage, Backups)

end.
%% primary storage service function; handles
%% general communication and functionality.
storage_serve(M, NodeName, Id, Neighbors, Storage, Backups) ->
    GlobalName = getStorageProcessName(Id),
    Rnd = crypto:rand_uniform(8000, 20000),
    println("~p is listening for ~p secs before backing up.", [Id, Rnd]),
    receive 
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
          case ets:lookup(Storage, Key) of
            [] ->
              % this means there is no key before.
              ets:insert(Storage, {Key, Value}),
              println("~s> {~p, ~p} stored. The key is brand new!",
                [GlobalName, Key, Value]),
              Pid ! {Ref, stored, no_value};

            [{_OldKey, OldValue}] ->
              println("~s> {~p, ~p} stored. The key existed before this store."
                ++ "The old value was ~p", [GlobalName, OldValue, Key, Value]),
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
    {Pid, rebalance} ->
		println("Received rebalance request from ~p, ~p moving to that node",
            [Pid, Id]),
        Pid ! {self(), rebalance_response, Storage, Backups, Neighbors},
        exit(normal);
	
	{Pid, backup_request} ->
		% send storage back to be backed up
		Pid ! {self(), backup_response, Storage},
		storage_serve(M, NodeName, Id, Neighbors, Storage, Backups);

	{_Ref, failure} -> 
		println("Node crashed during computation. failure.~n"),
	  	storage_serve(M, NodeName, Id, Neighbors, Storage, Backups)
    after 
      Rnd ->
	    NewBackups = backup_neighbors(M, NodeName,Id, Neighbors, Storage,
            Backups),
        storage_serve(M, NodeName, Id, Neighbors, Storage, NewBackups)
end,

storage_serve(M, NodeName, Id, Neighbors, Storage, Backups).

%%% remove highest numbered process from list.
%% tell it to exit.
select_hi_proc([], High)-> High;  
select_hi_proc([{IdN, PidN} | StorageProcs], {Id, Pid})->
	case IdN > Id of 
		true -> 
			select_hi_proc(StorageProcs, {IdN, PidN});
		false ->
			select_hi_proc(StorageProcs, {Id, Pid})
	end.
%% take the higher numbered processes to give
%lend_procs(StorageProcs, Lent, {Node, Id, Pid})->
	%Node ! lend down to the Id 

% hash function to uniformly distribute among 
%% storage processes.
hash(Str, M) when M >= 0 -> str_sum(Str) rem round((math:pow(2, M)));
hash(_, _) -> -1.   %% error if no storage
      %% processes are open.

% Constantly query neighbor philosophers to make sure that they are still
% there. If one is gone, delete fork to that philosopher and remove from
% neighbors list, sufficiently removing the edge. Otherwise, keep
% philosophizing.
check_neighbors([], _)-> ok;
check_neighbors([X|XS], ParentPid) ->
	    spawn(fun() -> monitor_neighbor(X, ParentPid) end),
	    check_neighbors(XS, ParentPid).
monitor_neighbor(Name, ParentPid) -> 
	erlang:monitor(process,{backup, Name}), %{RegName, Node}
	receive
		{'DOWN', _Ref, process, _Pid, normal} ->
			ParentPid ! {self(), check, Name};
		{'DOWN', _Ref, process, _Pid, _Reason} ->
			ParentPid ! {self(), missing, Name}
	end.
      
%% sum digits in string
str_sum([]) -> 0;
str_sum([X|XS]) -> X + str_sum(XS).

%% compute TwoToTheM from 2^TwoToTheM
compute_power2(N) when N < 2 -> 0;
compute_power2(N) -> 
  1 + compute_power2(math:pow(N, 1/2)).
