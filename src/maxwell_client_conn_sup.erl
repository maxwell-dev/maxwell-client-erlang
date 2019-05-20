%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Jun 2018 6:03 PM
%%%-------------------------------------------------------------------
-module(maxwell_client_conn_sup).
-behaviour(supervisor).

%% API
-export([
  start_link/0,
  start_link/1,
  start_child/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SUP_NAME, ?MODULE).
-define(SPEC(Module), #{
  id => Module,
  start => {Module, start_link, []},
  restart => temporary,
  shutdown => 100, % ms
  type => worker,
  modules => [Module]}
).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
  start_link([]).

start_link(Config) ->
  supervisor:start_link({local, ?SUP_NAME}, ?MODULE, [Config]).

start_child(Endpoint) ->
  supervisor:start_child(?SUP_NAME, [Endpoint]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([_Config]) ->
  SupFlags = #{strategy => simple_one_for_one, intensity => 0, period => 1},
  ChildSpecs = [?SPEC(maxwell_client_conn)],
  {ok, {SupFlags, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================