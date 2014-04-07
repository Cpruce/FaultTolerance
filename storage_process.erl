%% CSCI182E - Distributed Systems
%% Harvey Mudd College
%% Fault tolerant key-value store distributed system
%% @author Cory Pruce, Tum Chaturapruek
%% @doc _D157R18U73_
-module(storage_process).

-import(key_value_node, [println/1, println/2]).
%% ====================================================================
%%                             Public API
%% ====================================================================
-export([init/4]).
%% ====================================================================
%%                             Constants
%% ====================================================================
%% ====================================================================
%%                            Main Function
%% ====================================================================

init(M, Id, Neighbors, Storage)->
  	register(list_to_atom("StorageProcess" ++ integer_to_list(Id)), self()),
	Backups = backup_neighbors(Neighbors),
	storage_serve(M, Id, Neighbors, Storage, Backups). 


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

%% backup neighbors in the ring
backup_neighbors([]) -> [];
backup_neighbors([IdN, Neighbors]) -> 
	RecvNeigh = list_to_atom("StorageProcess"++IdN),
	println("Sending backup request to ~p~n", [RecvNeigh]),
	RecvNeigh ! {self(), retrieve, IdN},
	receive 
	 {_Ref, retrieve, Value} ->
		println("Backing up ~p for ~p~n", [Value, RecvNeigh]),
		NewDict = dict:new(),
		RetDict = dict:store(IdN, Value, NewDict),
		backup_neighbors(Neighbors)++[RetDict];

	 {_Ref, failure} ->
		println("Neighbor ~p crashed. Moving on.~n", [IdN]),
		backup_neighbors(Neighbors)
	end.

%% primary storage service function; handles
%% general communication and functionality.
storage_serve(M, Id, Neighbors, Storage, Backups) ->

    receive 
    	{Pid, Ref, store, Key, Value} ->
      		println("Received store command at key ~p of value ~p from ~p~n", [Key, Value, Pid]),  
      		case hash(Key, M) == Id of
        	% operation to be done at this process
        		true ->
          			% save old value, replace it, and send message back
          			Oldval = dict:fetch(Key, Storage),
          			NewStore = dict:store(Key, Value, Storage),
	  			println("Hash evaluated to Id, stored locally. Sending stored to ~p~n", [Pid]),
	  			Pid ! {Ref, stored, Oldval},
	  			storage_serve(M, Id, Neighbors, NewStore, Backups);
        		% pass on computation
        		false ->
        			% find and send to correct recipient
          			Recv = find_neighbor(Neighbors, Id),
	  			println("Passing store message onto ~p~n", [Recv]), 
          			Recv ! {Pid, Ref, store, Key, Value},
       	  			storage_serve(M, Id, Neighbors, Storage, Backups)
		end;
    	{Ref, stored, Oldval} -> 
	       case Oldval == no_value of
		    true -> 
			  println("No previously stored value. Store successful~n"),
			  storage_serve(M, Id, Neighbors, Storage, Backups);
		    false ->
			  println("Oldval is ~p~n. Store successful~n", [Oldval]), 
			  storage_serve(M, Id, Neighbors, Storage, Backups)
	    end;
    
    	{Pid, Ref, retrieve, Key} -> 
    		println("Received retrieve command at key ~p from ~p~n", [Key, Pid]),  
      		case hash(Key, M) == Id of
        	% operation to be done at this process
        		true ->
          			Val = dict:fetch(Key, Storage),
	  			println("Hash evaluated to Id, retrieved locally. Sending retrieved to ~p with the value ~p~n", [Pid, Val]),
	  			Pid ! {Ref, retrieved, Val},
	  			storage_serve(M, Id, Neighbors, Storage, Backups);
        		% pass on computation
        		false ->
          			% find and send to correct recipient
          			Recv = find_neighbor(Neighbors, Id),
	  			println("Passing retrieve message onto ~p~n", [Recv]), 
          			Recv ! {Pid, Ref, retrieve, Key},
       	  			storage_serve(M, Id, Neighbors, Storage, Backups)
		end;
    
     	{Ref, retrieved, Value} -> 
		case Value == no_value of
			true -> 
				println("No previously stored value. Retrieve unsuccessful~n"),
			  	storage_serve(M, Id, Neighbors, Storage, Backups);
		    	false ->
				println("Value is ~p~n. Retrieve successful~n", [Value]), 
			  	storage_serve(M, Id, Neighbors, Storage, Backups)
	    	end;
   
     	{Pid, Ref, first_key} -> 
		println("Received first_key request from ~p~n", [Pid]),
	    	%% do leader election? or snapshot? (pg 5 of hw5.pdf) 
	    	storage_serve(M, Id, Neighbors, Storage, Backups);
    
     	{Pid, Ref, last_key} -> 
        	println("Received last_key request from ~p~n", [Pid]),
	    	%% do leader election? or snapshot? (pg 5 of hw5.pdf) 
	    	storage_serve(M, Id, Neighbors, Storage, Backups); 
    
    	{Pid, Ref, num_keys} -> 
		println("Received num_keys request from ~p~n", [Pid]),
	    	%% do leader election? or snapshot? (pg 5 of hw5.pdf)  
	    	storage_serve(M, Id, Neighbors, Storage, Backups);
    
    	{Pid, Ref, node_list} ->
		storage_serve(M, Id, Neighbors, Storage, Backups);
    
    	{Ref, result, Result} -> 
		storage_serve(M, Id, Neighbors, Storage, Backups);
    
    	{Ref, failure} -> 
		println("Node crashed during computation. failure.~n"),
	  	storage_serve(M, Id, Neighbors, Storage, Backups)
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
