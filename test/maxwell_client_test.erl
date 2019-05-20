%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Jun 2018 8:47 PM
%%%-------------------------------------------------------------------
-module(maxwell_client_test).

-include_lib("eunit/include/eunit.hrl").

setup() ->
  RootDir = "/Users/xuchaoqian/codebase/hongjia/maxwell-client/data/basin/",
  LockFile = RootDir ++ "basin.lock",
  os:cmd("rm -rf " ++ RootDir ++ "/*"),
  application:set_env(basin, lock_file, LockFile),
  application:set_env(basin, root_dir, RootDir),
  application:set_env(basin, retention_age, 30),
  application:set_env(basin, clean_interval, 5),
  case application:ensure_all_started(maxwell_client) of
    {ok, _} -> ok;
    {error, {already_started, _}} -> ok;
    Error -> Error
  end.

teardown(_SetupResult) ->
  ok = application:stop(maxwell_client).

add_puller_test_() ->
  {setup, fun setup/0, fun teardown/1, fun add_puller/1}.

add_puller(_SetupResult) ->
  Topic = <<"topic_0">>,
  Endpoint = <<"localhost:8888">>,
  Result = maxwell_client_puller:ensure_started(Topic, Endpoint),
  [
    ?_assertMatch({ok, _}, Result)
  ].