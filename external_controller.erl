-module(external_controller).

%% ====================================================================
-export([main/0]).

main() ->
  net_kernel:start([external_controller, shortnames]),
  io:format("~p~n", [net_kernel:connect(list_to_atom("node1@wl-203-34"))]),
  % TODO: probably get rid of this global name registration.
  GlobalName = list_to_atom("ExternalController1"),
  global:register_name(GlobalName, self()),
  % sleep for 0.5 seconds -- we need to wait until it successfully registers
  timer:sleep(500),
  RegisteredNames = global:registered_names(),
  io:format("~p~n", [RegisteredNames]),
  StorageName = hd(tl(RegisteredNames)),
  global:send(StorageName, {self(), make_ref(), store, "MyKey", "MyValue"}),
  receive
  	{Ref, stored, OldValue} ->
  		io:format("We got the Yo message from ~p that the old value is ~p!~n", [Ref, OldValue]);
  	_ ->
  		io:format("Bad response!~n")
  end.
