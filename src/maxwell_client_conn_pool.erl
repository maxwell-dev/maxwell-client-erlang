%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Jun 2018 5:37 PM
%%%-------------------------------------------------------------------
-module(maxwell_client_conn_pool).
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
  capacity,
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
  lager:info(
    "Initializing ~p: capacity: ~p, pid: ~p", 
    [?MODULE, State#state.capacity, self()]
  ),
  {ok, State}.

handle_call({fetch, Endpoint}, _From, State) ->
  reply(fetch2(Endpoint, State));
handle_call(Request, _From, State) ->
  lager:error("Received unknown call: ~p", [Request]),
  reply({ok, State}).

handle_cast(Request, State) ->
  lager:error("Received unknown cast: ~p", [Request]),
  noreply(State).

handle_info({'DOWN', Ref, process, Pid, Reason}, State) ->
  lager:debug(
    "Conn closed: ref: ~p, pid: ~p, reason: ~p",
    [Ref, Pid, Reason]
  ),
  noreply(on_conn_closed({Ref, Pid}, State));
handle_info(Info, State) ->
  lager:error("Received unknown info: ~p", [Info]),
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
  Term = lists:keyfind(capacity, 1, Config),
  #state{
    capacity = case Term of
      {capacity, Size} -> Size;
      false -> 100
    end,
    endpoint_conns_dict = dict:new(),
    endpoint_index_dict = dict:new(),
    ref_endpoint_dict = dict:new()
  }.

fetch2(Endpoint, State) ->
  Conn2 = case dict:find(Endpoint, State#state.endpoint_conns_dict) of
    {ok, Conns} -> Conns;
    error -> []
  end,
  case erlang:length(Conn2) < State#state.capacity of
    true -> open_conn(Endpoint, State);
    false -> next_conn(Endpoint, State)
  end.

open_conn(Endpoint, State) ->
  {ok, Pid} = maxwell_client_conn_sup:start_child(Endpoint),
  Ref = erlang:monitor(process, Pid),

  State2 = State#state{
    endpoint_conns_dict = dict:update(
      Endpoint, 
      fun(Conns)-> [{Ref, Pid} | Conns] end, 
      [{Ref, Pid}], 
      State#state.endpoint_conns_dict
    ),
    endpoint_index_dict = dict:store(
      Endpoint, 0, State#state.endpoint_index_dict
    ), 
    ref_endpoint_dict = dict:store(
      Ref, Endpoint, State#state.ref_endpoint_dict
    )
  },

  {{ok, Pid}, State2}.

next_conn(Endpoint, State) ->
  {ok, Conns} = dict:find(Endpoint, State#state.endpoint_conns_dict),
  CurrIndex2 = case dict:find(Endpoint, State#state.endpoint_index_dict) of
    {ok, CurrIndex} -> CurrIndex;
    error -> 0
  end,
  NextIndex = CurrIndex2 + 1,
  NextIndex2 = case NextIndex =< erlang:length(Conns) of
    true -> NextIndex;
    false -> 1
  end,
  {_, Pid} = lists:nth(NextIndex2, Conns),
  {
    {ok, Pid},
    State#state{endpoint_index_dict = dict:store(
      Endpoint, NextIndex2, State#state.endpoint_index_dict
    )}
  }.

on_conn_closed({Ref, _} = Conn, State) ->
  case dict:find(Ref, State#state.ref_endpoint_dict) of
    {ok, Endpoint} -> erase_conn(Endpoint, Conn, State);
    error -> State
  end.

erase_conn(Endpoint, {Ref, _} = Conn, State) ->
  State#state{
    endpoint_conns_dict =
    case dict:find(Endpoint, State#state.endpoint_conns_dict) of
      {ok, Conns} ->
        Conns2 = lists:filter(fun(Conn2) -> Conn2 =/= Conn end, Conns),
        dict:store(Endpoint, Conns2, State#state.endpoint_conns_dict);
      error -> State#state.endpoint_conns_dict
    end,
    ref_endpoint_dict = dict:erase(Ref, State#state.ref_endpoint_dict)
  }.

reply({Reply, State}) ->
  {reply, Reply, State}.

noreply(State) ->
  {noreply, State}.