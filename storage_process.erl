%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(storage_process).

-import(key_value_node, [println/1, println/2]).
-import(advertise_id, [init_snapshot/2]).
%% ====================================================================
%%                             Public API
%% ====================================================================
-export([init_store/5]).
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

% floor function, taken from http://schemecookbook.org/Erlang/NumberRounding
floor(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T - 1;
        Pos when Pos > 0 -> T;
        _ -> T
    end.

init_store(M, NodeName, Id, Neighbors, Storage)->
  	global:register_name(list_to_atom("StorageProcess" ++ integer_to_list(Id)), self()),
	println("Neighbors is ~p~n", [Neighbors]),
	Backups = backup_neighbors(Id, Neighbors),
	storage_serve(M, NodeName, Id, Neighbors, Storage, []).%Backups). 

%% backup neighbors in the ring
backup_neighbors(_Id, []) -> [];
backup_neighbors(Id, [IdN | Neighbors]) -> 
	RecvNeigh = list_to_atom("StorageProcess"++integer_to_list(IdN)),
	println("Sending backup request to ~p~n", [RecvNeigh]),
    global:send(RecvNeigh, {self(), backup_request}),
	receive 
	 {_Ref, backup_response, Backup} ->
		% create backup
		println("Backing up ~p~n", [RecvNeigh]),
		% monitor to see if backup needs to register
		monitor_neighbor(RecvNeigh, self()), 
		backup_neighbors(Id, Neighbors)++[Backup];

	 {_Ref, failure} ->
		println("Neighbor ~p crashed. Moving on.~n", [IdN]),
		backup_neighbors(Id, Neighbors)
	end.

%% primary storage service function; handles
%% general communication and functionality.
storage_serve(M, NodeName, Id, Neighbors, Storage, Backups) ->
    GlobalName = getStorageProcessName(Id),
    receive 
    	{Pid, Ref, store, Key, Value} ->
      		println("Received store command at key ~p of value ~p from ~p~n", [Key, Value, Pid]),
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
          println("Check globally registered names: ~p", [global:registered_names()]),
          global:send(ForwardedRecipient, {Pid, Ref, store, Key, Value})
       end;
    	{_Ref, stored, Oldval} -> 
	       case Oldval == no_value of
		    true -> 
			  println("No previously stored value. Store successful~n"),
			  storage_serve(M, NodeName, Id, Neighbors, Storage, Backups);
		    false ->
			  println("Oldval is ~p~n. Store successful~n", [Oldval]), 
			  storage_serve(M, NodeName, Id, Neighbors, Storage, Backups)
	    end;
    
    	{Pid, _Ref, retrieve, Key} -> 
    		println("Received retrieve command at key ~p from ~p~n", [Key, Pid]),  
      		case hash(Key, M) == Id of
        	% operation to be done at this process
        		true ->
          			Val = dict:fetch(Key, Storage),
	  			println("Hash evaluated to Id, retrieved locally. Sending retrieved to ~p with the value ~p~n", [Pid, Val]),
	  			Pid ! {_Ref, retrieved, Val},
	  			storage_serve(M, NodeName, Id, Neighbors, Storage, Backups);
        		% pass on computation
        		false ->
          			% find and send to correct recipient
          			%Recv = find_neighbor(Neighbors, Id),
	  			%println("Passing retrieve message onto ~p~n", [Recv]), 
          			%Recv ! {Pid, _Ref, retrieve, Key},
       	  			storage_serve(M, NodeName, Id, Neighbors, Storage, Backups)
		end;
    
     	{_Ref, retrieved, Value} -> 
		case Value == no_value of
			true -> 
				println("No previously stored value. Retrieve unsuccessful~n"),
			  	storage_serve(M, NodeName, Id, Neighbors, Storage, Backups);
		    	false ->
				println("Value is ~p~n. Retrieve successful~n", [Value]), 
			  	storage_serve(M, NodeName, Id, Neighbors, Storage, Backups)
	    	end;
   
     	{Pid, _Ref, first_key} -> 
		println("Received first_key request from ~p~n", [Pid]),
	    	%% do leader election? or snapshot? (pg 5 of hw5.pdf) 
	    	storage_serve(M, NodeName, Id, Neighbors, Storage, Backups);
    
     	{Pid, _Ref, last_key} -> 
        	println("Received last_key request from ~p~n", [Pid]),
	    	%% do leader election? or snapshot? (pg 5 of hw5.pdf) 
	    	storage_serve(M, NodeName, Id, Neighbors, Storage, Backups); 
    
    	{Pid, _Ref, num_keys} -> 
		println("Received num_keys request from ~p~n", [Pid]),
	    	%% do leader election? or snapshot? (pg 5 of hw5.pdf)  
	    	storage_serve(M, NodeName, Id, Neighbors, Storage, Backups);
    
    	{Pid, _Ref, node_list} ->
		println("Received node_list request from ~p~n", [Pid]),
		NodeName ! {self(), node_list},
		storage_serve(M, NodeName, Id, Neighbors, Storage, Backups);
    
    	{_Ref, result, Result} -> 
            println("Obtained result ~p~n", [Result]),
            storage_serve(M, NodeName, Id, Neighbors, Storage, Backups);
    
    	{Pid, rebalance, {NewNode, NewId, NewPid}} ->
			println("Received rebalance request from ~p~n", [Pid]),
			%LentProcs = lend_procs(StorageProcs, {NewNode, NewId, NewPid}),
			%advertise(Id, NodeName, Neighbors++[{NewNode, NewId, NewPid}], StorageProcs--LentProcs, TwoToTheM),			
	ok;
	
	{Pid, backup_request} ->
		% send storage back to be backed up
		Pid ! {self(), backup_response, Storage},
		storage_serve(M, NodeName, Id, Neighbors, Storage, Backups);

	{_Ref, failure} -> 
		println("Node crashed during computation. failure.~n"),
	  	storage_serve(M, NodeName, Id, Neighbors, Storage, Backups)
  end.

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

%% compute M from 2^M
compute_power2(N) when N < 2 -> 0;
compute_power2(N) -> 
  1 + compute_power2(math:pow(N, 1/2)).
