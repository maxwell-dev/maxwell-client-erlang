%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Jun 2018 5:34 PM
%%%-------------------------------------------------------------------
-module(maxwell_client_puller).
-behaviour(gen_server).
-include_lib("maxwell_protocol/include/maxwell_protocol_pb.hrl").

%% API
-export([
  start_link/2,
  start_link/4,
  ensure_started/2,
  ensure_started/3,
  ensure_started/4,
  stop/1
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

-define(PROCESS_NAME(Topic), {puller, Topic}).
-define(VIA_PROCESS_NAME(Topic),
  {via, maxwell_client_registry, ?PROCESS_NAME(Topic)}
).
-define(PULL_CMD, '$pull').
-define(PULL_TIMEOUT, 10 * 1000). % millisecond as unit
-define(PULL_LIMIT, 128).

-record(state, {
  topic,
  endpoint,
  from_offset,
  to_offset,
  offset,
  basin_ref,
  basin_pid,
  conn_ref,
  conn_pid
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Topic, Endpoint) ->
  start_link(Topic, Endpoint, undefined, undefined).

start_link(Topic, Endpoint, FromOffset, ToOffset) ->
  gen_server:start_link(
    ?VIA_PROCESS_NAME(Topic), ?MODULE,
    [Topic, Endpoint, FromOffset, ToOffset], []
  ).

ensure_started(Topic, Endpoint) ->
  ensure_started(Topic, Endpoint, undefined, undefined).

ensure_started(Topic, Endpoint, FromOffset) ->
  ensure_started(Topic, Endpoint, FromOffset, undefined).

ensure_started(Topic, Endpoint, FromOffset, ToOffset) ->
  case maxwell_client_registry:whereis_name(
    ?PROCESS_NAME(Topic)) of
    undefined ->
      case maxwell_client_puller_sup:start_child(
        Topic, Endpoint, FromOffset, ToOffset) of
        {error, {already_started, Pid}} -> {ok, Pid};
        {ok, _} = Result -> Result
      end;
    Pid -> {ok, Pid}
  end.

stop(Ref) ->
  gen_server:cast(server_ref(Ref), stop).

server_ref(Ref) when is_pid(Ref) -> Ref;
server_ref(Topic) -> ?VIA_PROCESS_NAME(Topic).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Topic, Endpoint, FromOffset, ToOffset]) ->
  State = init_state(Topic, Endpoint, FromOffset, ToOffset),
  lager:info("Initializing ~p: state: ~p", [?MODULE, State]),
  send_cmd(?PULL_CMD, 100),
  {ok, State}.

handle_call(Request, _From, State) ->
  lager:error("Recevied unknown call: ~p", [Request]),
  {reply, ok, State}.

handle_cast(stop, State) ->
  {stop, {shutdown, unsubscribed}, State};
handle_cast(Request, State) ->
  lager:error("Recevied unknown cast: ~p", [Request]),
  {noreply, State}.

handle_info(?PULL_CMD, State) ->
  {noreply, pull(State)};
handle_info({'DOWN', BasinRef, process, _BasinPid, Reason},
    #state{basin_ref = BasinRef} = State) ->
  lager:info(
    "Basin was down: topic: ~p, basin_ref: ~p, reason: ~p",
    [State#state.topic, BasinRef, Reason]
  ),
  {noreply, init_basin(State)};
handle_info({'DOWN', ConnRef, process, _ConnPid, Reason},
    #state{conn_ref = ConnRef} = State) ->
  lager:info(
    "Conn was down: topic: ~p, conn_ref: ~p, reason: ~p",
    [State#state.topic, ConnRef, Reason]
  ),
  {noreply, pull(init_conn(State))};
handle_info(Info, State) ->
  lager:error("Recevied unknown info: ~p", [Info]),
  {noreply, State}.

terminate(Reason, State) ->
  lager:info(
    "Terminating ~p: topic: ~p, endpoint: ~p, reason: ~p",
    [?MODULE, State#state.topic, State#state.endpoint, Reason]
  ),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
init_state(Topic, Endpoint, FromOffset, ToOffset) ->
  State = #state{
    topic = Topic,
    endpoint = Endpoint,
    from_offset = FromOffset,
    to_offset = ToOffset
  },
  init_offset(init_conn(init_basin(State))).

init_basin(State) ->
  {BasinRef, BasinPid} = get_basin(State#state.topic),
  State#state{basin_ref = BasinRef, basin_pid = BasinPid}.

get_basin(Topic) ->
  {ok, Pid} = basin_topic_owner:ensure_started(Topic),
  Ref = erlang:monitor(process, Pid),
  {Ref, Pid}.

init_conn(State) ->
  {ConnRef, ConnPid} = get_conn(State#state.endpoint),
  State#state{conn_ref = ConnRef, conn_pid = ConnPid}.

get_conn(Endpoint) ->
  {ok, Pid} = maxwell_client_conn_mgr:fetch(Endpoint),
  Ref = erlang:monitor(process, Pid),
  {Ref, Pid}.

init_offset(State) ->
  Offset =
    case State#state.from_offset of
      undefined -> get_max_offset(State#state.basin_pid);
      Any -> Any
    end,
  State#state{offset = Offset}.

get_max_offset(BasinPid) ->
  case basin_topic_owner:seek_max_offset(BasinPid) of
    undefined -> -1;
    Offset -> Offset
  end.

pull(State) ->
  case State#state.to_offset =/= undefined
    andalso State#state.offset >= State#state.to_offset of
    true -> stop(self()), State;
    false -> pull1(State)
  end.

pull1(State) ->
  Req = #pull_req_t{
    topic = State#state.topic,
    offset = State#state.offset + 1,
    limit = ?PULL_LIMIT
  },
  Rep = maxwell_client_conn:send(
    State#state.conn_pid, Req, ?PULL_TIMEOUT
  ),
  end_pull1(Rep, State).

end_pull1(#pull_rep_t{msgs = Msgs}, State) ->
  {MaxOffset, Entries} = lists:foldl(
    fun(Msg, {MaxOffset0, Entries0}) ->
      MaxOffset1 = erlang:max(MaxOffset0, Msg#msg_t.offset),
      Entry = {
        Msg#msg_t.offset, Msg#msg_t.value, Msg#msg_t.timestamp
      },
      {MaxOffset1, [Entry | Entries0]}
    end, {0, []}, Msgs),
  basin_topic_owner:put_entries(
    State#state.basin_pid, lists:reverse(Entries)
  ),
  send_cmd(?PULL_CMD, 1),
  State#state{offset = MaxOffset};
end_pull1(#error_rep_t{} = Error, State) ->
  lager:warning("Error occured: ~p, will pull later...", [Error]),
  send_cmd(?PULL_CMD, 1000),
  State;
end_pull1({error, {100, timeout}} = Error, State) ->
  lager:debug("Timeout triggered: ~p, pull again...", [Error]),
  send_cmd(?PULL_CMD, 1),
  State.

send_cmd(Cmd, DelayMS) ->
  erlang:send_after(DelayMS, self(), Cmd).