-module(external_controller).

%% ====================================================================
-export([main/0]).

-import(key_value_node_working, [println/1, println/2]).

main() ->
  net_kernel:start([external_controller, shortnames]),
  println("~p", [net_kernel:connect(list_to_atom("node1@wl-203-34"))]),
  % TODO: probably get rid of this global name registration.
  GlobalName = list_to_atom("ExternalController1"),
  global:register_name(GlobalName, self()),
  % sleep for 0.5 seconds -- we need to wait until it successfully registers
  timer:sleep(500),
  RegisteredNames = global:registered_names(),
  println("~p", [RegisteredNames]),
  StorageName = hd(tl(RegisteredNames)),
  Ref = make_ref(),
  global:send(StorageName, {self(), Ref, store, "MyKey", "MyValue"}),
  receive
  	{Ref, stored, OldValue} ->
  		println("Stored! The old value was ~p!", [OldValue]);
  	_ ->
  		println("Bad response!")
  end.
