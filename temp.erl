-module(temp).

%% ====================================================================
-export([main/0]).

main() ->
  net_kernel:start([here, shortnames]),
  io:format("~p~n", [net_kernel:connect(list_to_atom("node1@wl-203-34"))]),
  GlobalName = list_to_atom("ExternalController1"),
  global:register_name(GlobalName, self()),
  % sleep for 0.5 seconds -- we need to wait until it successfully registers
  timer:sleep(500),
  RegisteredNames = global:registered_names(),
  io:format("~p~n", [RegisteredNames]),
  StorageName = hd(RegisteredNames),
  global:send(StorageName, {GlobalName, "Hi there"}),
  receive
  	{Name, "Yo"} ->
  		io:format("We got the Yo message from ~p!~n", [Name]);
  	_ ->
  		io:format("We did NOT get the Yo message!~n")
  end.