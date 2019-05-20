%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Mar 2018 6:00 PM
%%%-------------------------------------------------------------------
-module(maxwell_client_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SUP_NAME, ?MODULE).
-define(SPEC(Module, Type, Start), #{
  id => Module,
  start => Start,
  restart => permanent,
  shutdown => infinity,
  type => Type,
  modules => [Module]}
).
-define(SPEC0(Module, Type, Args),
  ?SPEC(Module, Type, {Module, start_link, [Args]})
).
-define(SPEC1(Module, Type),
  ?SPEC(Module, Type, {Module, start_link, []})
).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
  supervisor:start_link({local, ?SUP_NAME}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 60},
  ChildSpecs = [
    ?SPEC1(maxwell_client_registry, worker),
    ?SPEC1(maxwell_client_conn_sup, supervisor),
    ?SPEC1(maxwell_client_conn_mgr, worker),
    ?SPEC1(maxwell_client_puller_sup, supervisor)
  ],
  {ok, {SupFlags, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================