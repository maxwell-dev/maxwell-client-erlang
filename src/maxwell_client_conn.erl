%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Jun 2018 1:00 PM
%%%-------------------------------------------------------------------
-module(maxwell_client_conn).
-behaviour(gen_server).

-include_lib("maxwell_protocol/include/maxwell_protocol_pb.hrl").
-include("maxwell_client.hrl").

%% API
-export([
  start_link/1,
  stop/1,
  add_listener/2,
  delete_listener/2,
  send/3,
  send/4,
  async_send/3,
  async_send/4,
  get_status/1
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
-define(ON_ROUND_TIMEOUT_CMD(Ref), {'$on_round_timeout', Ref}).

-define(MAX_ROUND_REF, 600000).
-define(DELAY_QUEUE_CAPACITY, 128).

-record(state, {
  endpoint,
  gun_conn_ref,
  gun_conn_pid,
  timers,
  sources,
  delay_queue,
  listeners,
  last_ref,
  status
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Endpoint) ->
  gen_server:start_link(?MODULE, [Endpoint], []).

stop(ServerRef) ->
  gen_server:stop(ServerRef).

add_listener(ServerRef, ListenerPid) ->
  gen_server:call(ServerRef, {add_listener, ListenerPid}).

delete_listener(ServerRef, ListenerPid) ->
  gen_server:call(ServerRef, {delete_listener, ListenerPid}).

send(ServerRef, Msg, Timeout) ->
  send(ServerRef, Msg, Timeout, undefined).

send(ServerRef, Msg, Timeout, Callback) ->
  gen_server:call(ServerRef, {send, Msg, Timeout, Callback}, Timeout*2).

async_send(ServerRef, Msg, Timeout) ->
  async_send(ServerRef, Msg, Timeout, undefined).

async_send(ServerRef, Msg, Timeout, Callback) ->
  gen_server:call(ServerRef, {async_send, Msg, Timeout, Callback}, Timeout*2).

get_status(ServerRef) ->
  gen_server:call(ServerRef, get_status).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Endpoint]) ->
  State = open_gun_conn(#state{
    endpoint = Endpoint,
    gun_conn_ref = undefined,
    gun_conn_pid = undefined,
    timers = dict:new(),
    sources = dict:new(),
    delay_queue = orddict:new(),
    listeners = [],
    last_ref = 0,
    status = undefined
  }),
  lager:info("Initializing ~p: state: ~p", [?MODULE, State]),
  {ok, State}.

handle_call({add_listener, ListenerPid}, _From, State) ->
  {reply, ok, add_listener0(ListenerPid, State)};
handle_call({delete_listener, ListenerPid}, _From, State) ->
  {reply, ok, delete_listener0(ListenerPid, State)};
handle_call({send, Msg, Timeout, Callback}, From, State) ->
  {noreply, send0(Msg, Timeout, {From, Callback, sync}, State)};
handle_call({async_send, Msg, Timeout, Callback}, From, State) ->
  {reply, ok, send0(Msg, Timeout, {From, Callback, async}, State)};
handle_call(get_status, _From, State) ->
  {reply, State#state.status, State};
handle_call(Request, _From, State) ->
  lager:error("Recevied unknown call: ~p", [Request]),
  {reply, ok, State}.

handle_cast(Request, State) ->
  lager:error("Recevied unknown cast: ~p", [Request]),
  {noreply, State}.

handle_info(?ON_ROUND_TIMEOUT_CMD(Ref), State) ->
  {noreply, on_round_timeout(Ref, State)};
handle_info({gun_up, GunConnPid, Protocol}, State) ->
  lager:info(
    "Gun conn opened: endpoint: ~p, protocol: ~p",
    [State#state.endpoint, Protocol]
  ),
  gun:ws_upgrade(GunConnPid, "/"),
  {noreply, State};
handle_info(
    {gun_upgrade, _GunConnPid, _StreamRef, [<<"websocket">>], Headers}, State
) ->
  lager:info(
    "Gun conn upgraded: endpoint: ~p, headers: ~p",
    [State#state.endpoint, Headers]
  ),
  {noreply, on_gun_conn_connected(State)};
handle_info(
    {gun_response, _GunConnPid, _StreamRef, _IsFin, _Status, Headers}, State
) ->
  lager:info(
    "Failed to upgrade: endpoint: ~p, headers: ~p",
    [State#state.endpoint, Headers]
  ),
  {stop, {error, ws_upgrade_failed}, State};
handle_info({gun_error, _GunConnPid, _StreamRef, Reason}, State) ->
  lager:info(
    "Failed to upgrade(2): endpoint: ~p, reason: ~p",
    [State#state.endpoint, Reason]
  ),
  {stop, {error, Reason}, State};
handle_info({gun_error, _GunConnPid, Reason}, State) ->
  lager:info(
    "Error occured: endpoint: ~p, reason: ~p", [State#state.endpoint, Reason]
  ),
  {stop, {error, Reason}, State};
handle_info({gun_ws, _GunConnPid, _StreamRef, close}, State) ->
  lager:info("Gun conn closed: endpoint: ~p", [State#state.endpoint]),
  {stop, {shutdown, close_0}, on_gun_conn_disconnected(State)};
handle_info({gun_ws, _GunConnPid, _StreamRef, {close, Code, <<>>}}, State) ->
  lager:info(
    "Gun conn closed: endpoint: ~p, code: ~p", [State#state.endpoint, Code]
  ),
  {stop, {shutdown, close_1}, on_gun_conn_disconnected(State)};
handle_info({gun_ws, _GunConnPid, _StreamRef, Frame}, State) ->
  {noreply, recv0(Frame, State)};
handle_info({gun_down, _GunConnPid, Protocol, Reason,
  _KilledStreams, _UnprocessedStreams}, State) ->
  lager:info(
    "Gun conn down: endpoint: ~p, protocol: ~p, reason: ~p",
    [State#state.endpoint, Protocol, Reason]
  ),
  {stop, {shutdown, close_2}, on_gun_conn_disconnected(State)};
handle_info({'DOWN', _Ref, process, _GunConnPid, Reason}, State) ->
  lager:info(
    "Gun conn down(2): endpoint: ~p, reason: ~p",
    [State#state.endpoint, Reason]
  ),
  {stop, {shutdown, close_3}, on_gun_conn_disconnected(State)};
handle_info(Info, State) ->
  lager:error("Recevied unknown info: ~p", [Info]),
  {noreply, State}.

terminate(Reason, State) ->
  lager:info("Terminating: reason: ~p, state: ~p", [Reason, State]),
  close_gun_conn(State),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
open_gun_conn(State) ->
  {GunConnRef, GunConnPid} = open_gun_conn0(State#state.endpoint),
  State#state{
    gun_conn_ref = GunConnRef, 
    gun_conn_pid = GunConnPid,
    status = initial
  }.

open_gun_conn0(Endpoint) ->
  [HostBin, PortBin] = binary:split(Endpoint, <<":">>),
  Host = erlang:binary_to_list(HostBin),
  Port = erlang:binary_to_integer(PortBin),
  {ok, GunConnPid} = gun:open(Host, Port),
  GunConnRef = monitor(process, GunConnPid),
  {GunConnRef, GunConnPid}.

close_gun_conn(State) ->
  case State#state.gun_conn_ref of
    undefined -> ignore;
    _ -> demonitor(State#state.gun_conn_ref)
  end,
  case State#state.gun_conn_pid of
    undefined -> ignore;
    _ -> gun:shutdown(State#state.gun_conn_pid)
  end,
  State#state{
    gun_conn_ref = undefined,
    gun_conn_pid = undefined,
    status = closed
  }.

on_gun_conn_connected(State) ->
  State2 = send_delay_msgs(State),
  notify_and_clear(
    ?ON_CONNECTED_CMD(self()), State2#state{status = connected}
  ).

on_gun_conn_disconnected(State) ->
  close_gun_conn(
    notify_and_clear(
      ?ON_DISCONNECTED_CMD(self()), State#state{status = disconnected}
    )
  ).

add_listener0(ListenerPid, State) ->
  case lists:member(ListenerPid, State#state.listeners) of
    true -> State;
    false ->
      case State#state.status of
        connected -> ListenerPid ! ?ON_CONNECTED_CMD(self());
        disconnected -> ListenerPid ! ?ON_DISCONNECTED_CMD(self());
        _ -> ignore
      end,
      State#state{
        listeners = lists:append(State#state.listeners, [ListenerPid])
      }
  end.

delete_listener0(ListenerPid, State) ->
  State#state{
    listeners = lists:delete(ListenerPid, State#state.listeners)
  }.

notify_and_clear(Msg, State) ->
  NewListeners = lists:filter(
    fun(ListenerPid) -> erlang:is_process_alive(ListenerPid) end,
    State#state.listeners
  ),
  lists:foreach(
    fun(ListenerPid) -> ListenerPid ! Msg end, NewListeners
  ),
  State#state{listeners = NewListeners}.

send0(Msg, Timeout, Source, State) ->
  case maxwell_protocol:is_req(Msg) of
    true -> send1(Msg, Timeout, Source, State);
    false -> throw({unsupported_msg, Msg})
  end.

send1(Msg, Timeout, Source, State) ->
  State2 = next_ref(State),
  Ref = State2#state.last_ref,
  State3 = add_source(Ref, Source, State2),
  State4 = add_timer(Ref, Timeout, State3),
  Msg2 = set_ref_to_msg(Msg, Ref),
  send2(Msg2, State4).

send2(Msg, State) -> 
  case State#state.status =:= connected of
    true -> send3(Msg, State);
    false ->
      Size = orddict:size(State#state.delay_queue),
      lager:debug("Adding delay msg: msg: ~p, queue_size: ~p", [Msg, Size]),
      case Size < ?DELAY_QUEUE_CAPACITY of
        true -> add_delay_msg(Msg, State);
        false -> 
          lager:warning("Delay queue is full: ~p", [Size]),
          Ref = get_ref_from_msg(Msg),
          reply(Ref, {error, {10, delay_queue_is_full}, Ref}, State),
          State
      end
  end.

send3(Msg, State) ->
  lager:debug("Sending msg: ~p, to: ~p", [Msg, State#state.endpoint]),
  gun:ws_send(
    State#state.gun_conn_pid, {binary, maxwell_protocol:encode_msg(Msg)}
  ),
  State.

recv0(EncodedMsg, State) ->
  {binary, EncodedMsg2} = EncodedMsg,
  Msg = maxwell_protocol:decode_msg(EncodedMsg2),
  recv1(Msg, State).

recv1(Msg, State) ->
  case maxwell_protocol:is_rep(Msg) of
    true -> recv2(Msg, State);
    false -> throw({unsupported_msg, Msg})
  end.

recv2(Msg, State) ->
  lager:debug("Received msg: ~p: from: ~p, ", [Msg, State#state.endpoint]),
  Ref = get_ref_from_msg(Msg),
  reply(Ref, Msg, State),
  delete_source(Ref, delete_timer(Ref, State)).

on_round_timeout(Ref, State) ->
  reply(Ref, {error, {100, timeout}, Ref}, State),
  delete_delay_msg(Ref, delete_source(Ref, delete_timer(Ref, State))).

next_ref(State) ->
  NewRef = State#state.last_ref + 1,
  case NewRef > ?MAX_ROUND_REF of
    true -> State#state{last_ref = 1};
    false -> State#state{last_ref = NewRef}
  end.

set_ref_to_msg(#do_req_t{traces = [Trace | RestTraces]} = Msg, Ref) ->
  Msg#do_req_t{traces = [Trace#trace_t{ref = Ref} | RestTraces]};
set_ref_to_msg(Msg, Ref) -> setelement(size(Msg), Msg, Ref).

get_ref_from_msg(#do_rep_t{traces = [Trace | _]}) -> Trace#trace_t.ref;
get_ref_from_msg(#ok2_rep_t{traces = [Trace | _]}) -> Trace#trace_t.ref;
get_ref_from_msg(#error2_rep_t{traces = [Trace | _]}) -> Trace#trace_t.ref;
get_ref_from_msg(Msg) -> element(size(Msg), Msg).

add_source(Ref, Source, State) ->
  State#state{sources = dict:store(Ref, Source, State#state.sources)}.

delete_source(Ref, State) ->
  State#state{sources = dict:erase(Ref, State#state.sources)}.

add_timer(Ref, Delay, State) ->
  {ok, Timer} = timer:send_after(Delay, self(), ?ON_ROUND_TIMEOUT_CMD(Ref)),
  State#state{timers = dict:store(Ref, Timer, State#state.timers)}.

delete_timer(Ref, State) ->
  case dict:find(Ref, State#state.timers) of
    {ok, Timer} -> timer:cancel(Timer);
    error -> ignore
  end,
  State#state{timers = dict:erase(Ref, State#state.timers)}.

add_delay_msg(Msg, State) -> 
  State#state{delay_queue = 
    orddict:store(get_ref_from_msg(Msg), Msg, State#state.delay_queue)
  }.

delete_delay_msg(Ref, State) -> 
  State#state{delay_queue = orddict:erase(Ref, State#state.delay_queue)}.

send_delay_msgs(State) -> 
  State3 = orddict:fold(
    fun(_, Msg, State2) -> send3(Msg, State2) end, 
    State, State#state.delay_queue
  ),
  State3#state{delay_queue = orddict:new()}.

reply(Ref, Reply, State) ->
  case dict:find(Ref, State#state.sources) of
    {ok, {{Pid, _} = From, Callback, Mode}} -> 
      case Mode of 
        sync ->
          case Callback of
            undefined -> gen_server:reply(From, Reply);
            _ -> gen_server:reply(From, Callback(Reply))
          end;
        async ->
          case Callback of
            undefined -> Pid ! Reply;
            _ -> Pid ! Callback(Reply)
          end
      end;
    error -> 
      lager:info("Cannot find source: ref: ~p", [Ref]),  
      ignore
  end.