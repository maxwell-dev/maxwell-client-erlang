%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Jun 2018 5:37 PM
%%%-------------------------------------------------------------------
-module(maxwell_client_conn_mgr).
-behaviour(gen_server).

%% API
-export([
  start_link/0,
  start_link/1,
  fetch/1
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {
  parallels_size,
  endpoint_conns_dict,
  endpoint_index_dict,
  ref_endpoint_dict
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
  start_link([]).

start_link(Config) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Config], []).

fetch(Endpoint) ->
  gen_server:call(?SERVER, {fetch, Endpoint}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Config]) ->
  State = init_state(Config),
  lager:info("Initializing ~p: pid: ~p", [?MODULE, self()]),
  {ok, State}.

handle_call({fetch, Endpoint}, _From, State) ->
  reply(fetch0(Endpoint, State));
handle_call(Request, _From, State) ->
  lager:info("Received unknown call: ~p", [Request]),
  reply({ok, State}).

handle_cast(Request, State) ->
  lager:info("Received unknown cast: ~p", [Request]),
  noreply(State).

handle_info({'DOWN', Ref, process, Pid, Reason}, State) ->
  lager:info(
    "Conn closed: ref: ~p, pid: ~p, reason: ~p",
    [Ref, Pid, Reason]
  ),
  noreply(on_conn_closed({Ref, Pid}, State));
handle_info(Info, State) ->
  lager:info("Received unknown info: ~p", [Info]),
  noreply(State).

terminate(Reason, _State) ->
  lager:info(
    "Terminating ~p: pid: ~p, reason: ~p",
    [?MODULE, self(), Reason]
  ),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
init_state(Config) ->
  Term = lists:keyfind(parallels_size, 1, Config),
  #state{
    parallels_size
    = case Term of
        {parallels_size, Size} -> Size;
        false -> 3
      end,
    endpoint_conns_dict = dict:new(),
    endpoint_index_dict = dict:new(),
    ref_endpoint_dict = dict:new()
  }.

fetch0(Endpoint, State) ->
  NewState = ensure_paralles_satisfied(Endpoint, State),
  {{Ref, Pid}, NewState1} = next_conn(Endpoint, NewState),
  NewState2 = ensure_conn_monitored(Endpoint, Ref, NewState1),
  {{ok, Pid}, NewState2}.

ensure_paralles_satisfied(Endpoint, State) ->
  NewConns = ensure_paralles_satisfied0(Endpoint, State),
  State#state{endpoint_conns_dict = dict:store(
    Endpoint, NewConns, State#state.endpoint_conns_dict
  )}.

ensure_paralles_satisfied0(Endpoint, State) ->
  case dict:find(Endpoint, State#state.endpoint_conns_dict) of
    {ok, Conns} ->
      ensure_paralles_satisfied1(
        Endpoint, Conns, State#state.parallels_size);
    error ->
      ensure_paralles_satisfied1(
        Endpoint, [], State#state.parallels_size)
  end.

ensure_paralles_satisfied1(_Endpoint, Conns, ParallelsSize)
  when erlang:length(Conns) == ParallelsSize ->
  Conns;
ensure_paralles_satisfied1(Endpoint, Conns, ParallelsSize)
  when erlang:length(Conns) < ParallelsSize ->
  {ok, Conn} = open(Endpoint),
  ensure_paralles_satisfied1(Endpoint, [Conn | Conns], ParallelsSize);
ensure_paralles_satisfied1(Endpoint, Conns, ParallelsSize)
  when erlang:length(Conns) > ParallelsSize ->
  [{_, Pid} | RestConns] = Conns,
  exit(Pid, close_redundant_conn),
  ensure_paralles_satisfied1(Endpoint, RestConns, ParallelsSize).

next_conn(Endpoint, State) ->
  {ok, Conns} = dict:find(Endpoint, State#state.endpoint_conns_dict),
  CurrIndex1 =
    case dict:find(Endpoint, State#state.endpoint_index_dict) of
      {ok, CurrIndex} -> CurrIndex;
      error -> 0
    end,
  NextIndex = CurrIndex1 + 1,
  NextIndex1
    = case NextIndex =< erlang:length(Conns) of
        true -> NextIndex;
        false -> 1
      end,
  {
    lists:nth(NextIndex1, Conns),
    State#state{endpoint_index_dict = dict:store(
      Endpoint, NextIndex1, State#state.endpoint_index_dict
    )}
  }.

ensure_conn_monitored(Endpoint, Ref, State) ->
  case dict:is_key(Ref, State#state.ref_endpoint_dict) of
    true -> State;
    false ->
      State#state{
        ref_endpoint_dict =
        dict:store(Ref, Endpoint, State#state.ref_endpoint_dict)
      }
  end.

open(Endpoint) ->
  {ok, Pid} = maxwell_client_conn_sup:start_child(Endpoint),
  Ref = erlang:monitor(process, Pid),
  {ok, {Ref, Pid}}.

on_conn_closed({Ref, _} = Conn, State) ->
  case dict:find(Ref, State#state.ref_endpoint_dict) of
    {ok, Endpoint} ->
      erase_conn(Endpoint, Conn, State);
    error ->
      State
  end.

erase_conn(Endpoint, {Ref, _} = Conn, State) ->
  State#state{
    endpoint_conns_dict =
    case dict:find(Endpoint, State#state.endpoint_conns_dict) of
      {ok, Conns} ->
        NewConns = lists:dropwhile(fun(Conn0) -> Conn0 == Conn end, Conns),
        dict:store(Endpoint, NewConns, State#state.endpoint_conns_dict);
      error ->
        State#state.endpoint_conns_dict
    end,
    ref_endpoint_dict = dict:erase(Ref, State#state.ref_endpoint_dict)
  }.

reply({Reply, State}) ->
  {reply, Reply, State}.

noreply(State) ->
  {noreply, State}.