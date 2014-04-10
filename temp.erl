-module(temp).

%% ====================================================================
-export([main/0]).

main() ->
  net_kernel:start([here, shortnames]),
  io:format("~p", [net_kernel:connect(list_to_atom("node1@Js-MacBook-Pro-8"))]),
  % sleep for 0.5 seconds -- we need to wait until it successfully registers
  timer:sleep(500),
  io:format("~p", [global:registered_names()]).
