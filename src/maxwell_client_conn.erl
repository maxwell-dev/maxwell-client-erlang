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

-define(PING_CMD, '$ping').
-define(MAX_ROUND_REF, 600000).
-define(DELAY_QUEUE_CAPACITY, 128).

-record(state, {
  endpoint,
  gun_conn_ref,
  gun_conn_pid,
  timers,
  froms,
  delay_queue,
  listeners,
  last_ref,
  sending_time,
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
  gen_server:call(ServerRef, {send, Msg, Timeout}, infinity).

get_status(ServerRef) ->
  gen_server:call(ServerRef, get_status).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Endpoint]) ->
  State = #state{
    endpoint = Endpoint,
    gun_conn_ref = undefined,
    gun_conn_pid = undefined,
    timers = dict:new(),
    froms = dict:new(),
    delay_queue = orddict:new(),
    listeners = [],
    last_ref = 0,
    sending_time = 0,
    status = undefined
  },
  {ok, repeat_ping(open_gun_conn(State))}.

handle_call({add_listener, ListenerPid}, _From, State) ->
  {reply, ok, add_listener0(ListenerPid, State)};
handle_call({delete_listener, ListenerPid}, _From, State) ->
  {reply, ok, delete_listener0(ListenerPid, State)};
handle_call({send, Msg, Timeout}, From, State) ->
  {noreply, send0(Msg, Timeout, From, State)};
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
handle_info(?PING_CMD, State) ->
  {noreply, repeat_ping(State)};
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
  {noreply, on_gun_conn_disconnected(State)};
handle_info({gun_ws, _GunConnPid, _StreamRef, {close, Code, <<>>}}, State) ->
  lager:info(
    "Gun conn closed: endpoint: ~p, code: ~p", [State#state.endpoint, Code]
  ),
  {noreply, on_gun_conn_disconnected(State)};
handle_info({gun_ws, _GunConnPid, _StreamRef, Frame}, State) ->
  {noreply, recv0(Frame, State)};
handle_info({gun_down, _GunConnPid, Protocol, Reason,
  _KilledStreams, _UnprocessedStreams}, State) ->
  lager:info(
    "Gun conn down: endpoint: ~p, protocol: ~p, reason: ~p",
    [State#state.endpoint, Protocol, Reason]
  ),
  {noreply, on_gun_conn_disconnected(State)};
handle_info({'DOWN', _Ref, process, _GunConnPid, Reason}, State) ->
  lager:info(
    "Gun conn down(2): endpoint: ~p, reason: ~p",
    [State#state.endpoint, Reason]
  ),
  {noreply, on_gun_conn_disconnected(State)};
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
  open_gun_conn(
    close_gun_conn(
      notify_and_clear(
        ?ON_DISCONNECTED_CMD(self()), State#state{status = disconnected}
      )
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

send0(Msg, Timeout, From, State) ->
  case maxwell_protocol:is_req(Msg) of
    true -> send1(Msg, Timeout, From, State);
    false -> throw({unsupported_msg, Msg})
  end.

send1(Msg, Timeout, From, State) ->
  State2 = next_ref(State),
  Ref = State2#state.last_ref,
  State3 = add_from(Ref, From, State2),
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
          reply(
            get_ref_from_msg(Msg), {error, {100, delay_queue_is_full}}, State
          ),
          State
      end
  end.

send3(Msg, State) ->
  lager:debug("Sending msg: ~p, to: ~p", [Msg, State#state.endpoint]),
  gun:ws_send(
    State#state.gun_conn_pid, {binary, maxwell_protocol:encode_msg(Msg)}
  ),
  State#state{sending_time = get_current_timestamp()}.

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
  delete_from(Ref, delete_timer(Ref, State)).

on_round_timeout(Ref, State) ->
  reply(Ref, {error, {99, timeout}}, State),
  delete_delay_msg(Ref, delete_from(Ref, delete_timer(Ref, State))).

repeat_ping(State) ->
  case State#state.status of
    connected ->
      case should_ping(State#state.sending_time) of
        true -> send2(#ping_req_t{}, State);
        false -> ignore
      end;
    _ -> ignore
  end,
  send_cmd(?PING_CMD, 5000),
  State.

should_ping(SendingTime) ->
  get_current_timestamp() - SendingTime >= 5000.

next_ref(State) ->
  NewRef = State#state.last_ref + 1,
  case NewRef > ?MAX_ROUND_REF of
    true -> State#state{last_ref = 1};
    false -> State#state{last_ref = NewRef}
  end.

set_ref_to_msg(#do_req_t{traces = Traces} = Msg, Ref) ->
  Msg#do_req_t{traces = [#trace_t{ref = Ref} | Traces]};
set_ref_to_msg(Msg, Ref) -> setelement(size(Msg), Msg, Ref).

get_ref_from_msg(#do_rep_t{traces = [Trace | _]}) ->
  Trace#trace_t.ref;
get_ref_from_msg(Msg) -> element(size(Msg), Msg).

add_from(Ref, From, State) ->
  State#state{froms = dict:store(Ref, From, State#state.froms)}.

delete_from(Ref, State) ->
  State#state{froms = dict:erase(Ref, State#state.froms)}.

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
  orddict:fold(
    fun(Ref, Msg, State2) -> 
      State3 = send3(Msg, State2),
      State3#state{delay_queue = orddict:erase(Ref, State3#state.delay_queue)}
    end, 
    State,
    State#state.delay_queue
  ).

reply(Ref, Reply, State) ->
  case dict:find(Ref, State#state.froms) of
    {ok, From} -> gen_server:reply(From, Reply);
    error -> ignore
  end.

send_cmd(Cmd, DelayMS) ->
  erlang:send_after(DelayMS, self(), Cmd).

get_current_timestamp() ->
  {MegaSec, Sec, MicroSec} = os:timestamp(),
  1000000000 * MegaSec + Sec * 1000 + erlang:trunc(MicroSec / 1000).