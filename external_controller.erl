-module(external_controller).

%% ====================================================================
-export([main/1]).

-import(key_value_node_working, [println/1, println/2]).


inialize(FullNodeName) ->
  net_kernel:start([external_controller, shortnames]),
  net_kernel:connect(list_to_atom(FullNodeName)),
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
  println("~s:~p> Sending a store request with {~p, ~p}...",
    [GlobalName, Ref, Key, Value]),
  global:send(StorageName, {self(), Ref, store, Key, Value}).

send_retrieve_request(Key) ->
  GlobalName = "ExternalControllerRetrieve1",
  Ref = make_ref(),
  RegisteredNames = global:registered_names(),
  StorageName = hd(tl(tl(tl(RegisteredNames)))), % pick one storage process 'randomly'
  % println("~s> Registered names: ~p", [GlobalName, RegisteredNames]),
  println("~s:~p> Sending a retrieve request with key ~p...",
    [GlobalName, Ref, Key]),
  global:send(StorageName, {self(), Ref, retrieve, Key}).

loop_once() ->
  GlobalName = "ExternalControllerGeneral1",
  receive
    {Ref, stored, OldValue} ->
      case OldValue == no_value of
        true -> 
          println("~s:~p> No previously stored value. Store successful", [GlobalName,
            Ref]);
        false ->
          println("~s:~p> Old value was ~p. Store successful.", [GlobalName, Ref, OldValue])
      end;
    {Ref, retrieved, Value} ->
      case Value == no_value of
        true -> 
          println("~s:~p> The key does not exist.", [GlobalName, Ref]);
        false ->
          println("~s:~p> The value for the requested key is ~p.", [GlobalName, Ref, Value])
      end;
    _ ->
      println("Bad response!")
  end,
  halt().

% loop() ->
%   loop_once(),
%   loop().

main(Params) ->
  FullNodeName = hd(Params),
  Action = hd(tl(Params)),
  inialize(FullNodeName),
  case Action of
    "store" ->
      Key =  hd(tl(tl(Params))),
      Value =  hd(tl(tl(tl(Params)))),
      send_store_request(Key,Value);
    "retrieve" ->
      Key =  hd(tl(tl(Params))),
      send_retrieve_request(Key);
    _ ->
      println("Not a valid action.")
  end,
  % if you want an infinite loop, use loop() instead.
  loop_once().