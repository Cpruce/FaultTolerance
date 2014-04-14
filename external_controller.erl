-module(external_controller).

%% ====================================================================
-export([main/1]).

-import(key_value_node_working, [println/1, println/2]).


inialize(FullNodeName) ->
  net_kernel:start([external_controller, shortnames]),
  case net_kernel:connect(list_to_atom(FullNodeName)) of
    true ->
      println("Connected to ~p successfully.", [FullNodeName]);
    false ->
      println("Error: Cannot connect to ~p! Please try again", [FullNodeName]),
      halt()
  end,
  GlobalName = list_to_atom("ExternalController1"),
  global:register_name(GlobalName, self()),
  % sleep for 0.5 seconds -- we need to wait until it successfully registers
  timer:sleep(500).

send_store_request(Key, Value) ->
  GlobalName = "ExternalControllerStore1",
  Ref = make_ref(),
  RegisteredNames = global:registered_names(),
  % println("~p", [RegisteredNames]),
  StorageName = case length(RegisteredNames) of
    % pick one storage process 'randomly'
    0 ->
      halt("Empty globally registered names in send_store_request. "
        ++ "Something must have gone wrong!");
    1 ->
      halt("Only one globally registered names."
        ++ "We want to test when there are at least two.");
    _ ->
      hd(tl(RegisteredNames))
  end,
  % println("~s> Registered names: ~p", [GlobalName, RegisteredNames]),
  println("~s:~p > Sending a store request with {~p, ~p}...",
    [GlobalName, Ref, Key, Value]),
  global:send(StorageName, {self(), Ref, store, Key, Value}).


send_retrieve_request(Key) ->
  GlobalName = "ExternalControllerRetrieve1",
  Ref = make_ref(),
  RegisteredNames = global:registered_names(),
  StorageName = hd(tl(tl(tl(RegisteredNames)))), % pick one storage process 'randomly'
  % println("~s> Registered names: ~p", [GlobalName, RegisteredNames]),
  println("~s:~p > Sending a retrieve request with key ~p...",
    [GlobalName, Ref, Key]),
  global:send(StorageName, {self(), Ref, retrieve, Key}).


send_first_key_request() ->
  GlobalName = "ExternalControllerSendFirstKey1",
  Ref = make_ref(),
  RegisteredNames = global:registered_names(),
  StorageName = hd(RegisteredNames), % pick one storage process 'randomly'
  % println("~s> Registered names: ~p", [GlobalName, RegisteredNames]),
  println("~s:~p > Sending a first_key request...", [GlobalName, Ref]),
  global:send(StorageName, {self(), Ref, first_key}).


send_last_key_request() ->
  GlobalName = "ExternalControllerSendLastKey1",
  Ref = make_ref(),
  RegisteredNames = global:registered_names(),
  StorageName = hd(RegisteredNames), % pick one storage process 'randomly'
  % println("~s> Registered names: ~p", [GlobalName, RegisteredNames]),
  println("~s:~p > Sending a last_key request...", [GlobalName, Ref]),
  global:send(StorageName, {self(), Ref, last_key}).


send_num_keys_request() ->
  GlobalName = "ExternalControllerSendNumKeys1",
  Ref = make_ref(),
  RegisteredNames = global:registered_names(),
  StorageName = hd(RegisteredNames), % pick one storage process 'randomly'
  % println("~s> Registered names: ~p", [GlobalName, RegisteredNames]),
  println("~s:~p > Sending a num_keys request...", [GlobalName, Ref]),
  global:send(StorageName, {self(), Ref, num_keys}).


send_node_list_request() ->
  GlobalName = "ExternalControllerSendNodeList1",
  Ref = make_ref(),
  RegisteredNames = global:registered_names(),
  StorageName = hd(RegisteredNames), % pick one storage process 'randomly'
  % println("~s> Registered names: ~p", [GlobalName, RegisteredNames]),
  println("~s:~p > Sending a node_list request...", [GlobalName, Ref]),
  global:send(StorageName, {self(), Ref, node_list}).


loop_once() ->
  GlobalName = "ExternalControllerGeneral1",
  receive
    {Ref, stored, OldValue} ->
      case OldValue == no_value of
        true ->
          println("~s:~p > No previously stored value. Store successful", [GlobalName, Ref]);
        false ->
          println("~s:~p > Old value was ~p. Store successful.", [GlobalName, Ref, OldValue])
      end;
    {Ref, retrieved, Value} ->
      case Value == no_value of
        true ->
          println("~s:~p > The key does not exist.", [GlobalName, Ref]);
        false ->
          println("~s:~p > The value for the requested key is ~p.", [GlobalName, Ref, Value])
      end;
    {Ref, result, Result} ->
      println("~s:~p > The result is ~p", [GlobalName, Ref, Result]);
    _ ->
      println("~s > Error: Bad response!", [GlobalName])
  end,
  halt().

% loop() ->
%   loop_once(),
%   loop().

main(Params) ->
  FullNodeName = hd(Params),
  Action = hd(tl(Params)),
  TheRest = tl(tl(Params)),
  inialize(FullNodeName),
  case Action of
    "store" ->
      case length(TheRest) of
        2 ->
          Key =  hd(TheRest),
          Value =  hd(tl(TheRest)),
          send_store_request(Key, Value);
        _ ->
          halt("Error: There should be exactly two parameters after the keyword 'store'")
      end;
    "retrieve" ->
      case length(TheRest) of
        1 ->
          Key =  hd(TheRest),
          send_retrieve_request(Key);
        _ ->
          halt("Error: There should be exactly one parameter after the keyword 'retrieve'")
      end;
    "first_key" ->
      case length(TheRest) of
        0 ->
          send_first_key_request();
        _ ->
          halt("Error: There should be no parameters after the keyword 'first_key'")
      end;
    "last_key" ->
      case length(TheRest) of
        0 ->
          send_last_key_request();
        _ ->
          halt("Error: There should be no parameters after the keyword 'last_key'")
      end;
    "num_keys" ->
      case length(TheRest) of
        0 ->
          send_num_keys_request();
        _ ->
          halt("Error: There should be no parameters after the keyword 'num_keys'")
      end;
    "node_list" ->
      case length(TheRest) of
        0 ->
          send_node_list_request();
        _ ->
          halt("Error: There should be no parameters after the keyword 'node_list'")
      end;
    _ ->
      halt("Error: Not a valid action.")
  end,
  % if you want an infinite loop, use loop() instead.
  loop_once().