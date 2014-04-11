-module(external_controller).

%% ====================================================================
-export([main/0]).

-import(key_value_node_working, [println/1, println/2]).

send_store_request() ->
  GlobalName = "ExternalController1",
  Ref = make_ref(),
  RegisteredNames = global:registered_names(),
  StorageName = hd(tl(RegisteredNames)), % pick one storage process 'randomly'
  println("~s> Registered names: ~p", [GlobalName, RegisteredNames]),
  global:send(StorageName, {self(), Ref, store, "MyKey", "MyValue"}).

send_retrieve_request() ->
  GlobalName = "ExternalController1",
  Ref = make_ref(),
  RegisteredNames = global:registered_names(),
  StorageName = hd(tl(tl(tl(RegisteredNames))), % pick one storage process 'randomly'
  println("~s> Registered names: ~p", [GlobalName, RegisteredNames]),
  global:send(StorageName, {self(), Ref, retrieve, "MyKey"}).

loop() ->
  GlobalName = "ExternalController1",
  receive
    {_Ref, stored, OldValue} ->
      case OldValue == no_value of
        true -> 
          println("~s> No previously stored value. Store successful", [GlobalName]);
        false ->
          println("~s> Old value was ~p. Store successful.", [GlobalName, OldValue])
      end;
    _ ->
      println("Bad response!")
  end,
  loop().

main() ->
  net_kernel:start([external_controller, shortnames]),
  net_kernel:connect(list_to_atom("node1@wl-203-34")),
  % TODO: probably get rid of this global name registration.
  GlobalName = list_to_atom("ExternalController1"),
  global:register_name(GlobalName, self()),
  % sleep for 0.5 seconds -- we need to wait until it successfully registers
  timer:sleep(500),
  send_store_request(),
  loop().