%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(antidotec_pb_socket).

-include_lib("riak_pb/include/antidote_pb.hrl").

-behaviour(gen_server).

-define(FIRST_RECONNECT_INTERVAL, 100).
-define(TIMEOUT, 60000).

%% The TCP/IP host name or address of the Riak node
-type address() :: string() | atom() | inet:ip_address().
%% The TCP port number of the Riak node's Protocol Buffers interface
-type portnum() :: non_neg_integer().
-type msg_id() :: non_neg_integer().
-type rpb_req() :: {tunneled, msg_id(), binary()} | atom() | tuple().

-record(request, {ref :: reference(), msg :: rpb_req(), from, timeout :: timeout(),
                  tref :: reference() | undefined }).

-record(state, {
          address :: address(),    % address to connect to
          port :: portnum(),       % port to connect to
          sock :: port(),       % gen_tcp socket
          active :: #request{} | undefined,     % active request
          connect_timeout=infinity :: timeout(), % timeout of TCP connection
          keepalive = false :: boolean() % if true, enabled TCP keepalive for the socket
         }).

-export([start_link/2, start_link/3,
         start/2, start/3,
         stop/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         init/1,
         code_change/3,
         terminate/2]).

-export([
         general_tx/2,
         general_tx/3,
         certify/5,
         get_hash_fun/1,
         start_tx/2,
         single_up_req/4,
         single_read/4,
         read/4
        ]).

%% @private
init([Address, Port, _Options]) ->
    State = #state{address = Address, port = Port, active = undefined},
    case connect(State) of
        {error, Reason} ->
            {stop, {tcp, Reason}};
        Ok -> Ok
    end.

%% @doc Create a linked process to talk with the riak server on Address:Port
%%      Client id will be assigned by the server.
-spec start_link(address(), portnum()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port) ->
    start_link(Address, Port, []).

%% @doc Create a linked process to talk with the riak server on Address:Port with Options.
%%      Client id will be assigned by the server.
start_link(Address, Port, Options) when is_list(Options) ->
    gen_server:start_link(?MODULE, [Address, Port, Options], []).

%% @doc Create a process to talk with the riak server on Address:Port.
%%      Client id will be assigned by the server.
start(Address, Port) ->
    start(Address, Port, []).

%% @doc Create a process to talk with the riak server on Address:Port with Options.
start(Address, Port, Options) when is_list(Options) ->
    gen_server:start(?MODULE, [Address, Port, Options], []).

%% @doc Disconnect the socket and stop the process.
stop(Pid) ->
    call_infinity(Pid, stop).

%% @private Like `gen_server:call/3', but with the timeout hardcoded
%% to `infinity'.
call_infinity(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).

%% @private
handle_call({req, Msg, Timeout}, From, State) ->
    {noreply, send_request(new_request(Msg, From, Timeout), State)};

handle_call(stop, _From, State) ->
    _ = disconnect(State),
    {stop, normal, ok, State}.

%% @private
%% @todo handle timeout
handle_info({_Proto, Sock, Data}, State=#state{active = (Active = #request{})}) ->
    <<MsgCode:8, MsgData/binary>> = Data,
    Resp = riak_pb_codec:decode(MsgCode, MsgData),
    %%message handling
    Result = case decode_response(Resp) of
                 error -> error;
                 {error,Reason} -> {error, Reason};
                 ok -> ok;
                 {ok, Val} -> {ok, Val};
                 Val -> {ok, Val}
             end,
    cancel_req_timer(Active#request.tref),
    _ = send_caller(Result, Active),
    NewState = State#state{ active = undefined },
    ok = inet:setopts(Sock, [{active, once}]),
    {noreply, NewState};

handle_info({req_timeout, _Ref}, State=#state{active = Active}) ->
    cancel_req_timer(Active#request.tref),
    _ = send_caller({error, timeout}, Active),
    {noreply, State#state{ active = undefined }};

handle_info({tcp_closed, _Socket}, State) ->
    disconnect(State);

handle_info({_Proto, Sock, _Data}, State) ->
    ok = inet:setopts(Sock, [{active, once}]),
    {noreply, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) -> ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @private
%% Connect the socket if disconnected
connect(State) when State#state.sock =:= undefined ->
    #state{address = Address, port = Port} = State,
    case gen_tcp:connect(Address, Port,
                         [binary, {active, once}, {packet, 4},
                          {keepalive, State#state.keepalive}],
                         State#state.connect_timeout) of
        {ok, Sock} ->
            {ok, State#state{sock = Sock}};
        Error -> Error
    end.


disconnect(State) ->
    %% Tell any pending requests we've disconnected
    _ = case State#state.active of
            undefined ->
                ok;
            Request ->
                send_caller({error, disconnected}, Request)
        end,

    %% Make sure the connection is really closed
    case State#state.sock of
        undefined ->
            ok;
        Sock ->
            gen_tcp:close(Sock)
    end,
    NewState = State#state{sock = undefined, active = undefined},
    {stop, disconnected, NewState}.


%% @private
new_request(Msg, From, Timeout) ->
    Ref = make_ref(),
    #request{ref = Ref, msg = Msg, from = From, timeout = Timeout,
             tref = create_req_timer(Timeout, Ref)}.

%% @private
%% Create a request timer if desired, otherwise return undefined.
create_req_timer(infinity, _Ref) ->
    undefined;
create_req_timer(undefined, _Ref) ->
    undefined;
create_req_timer(Msecs, Ref) ->
    erlang:send_after(Msecs, self(), {req_timeout, Ref}).

%% Send a request to the server and prepare the state for the response
%% @private
send_request(Request0, State) when State#state.active =:= undefined  ->
    {Request, Pkt} = encode_request_message(Request0),
    case gen_tcp:send(State#state.sock, Pkt) of
        ok ->
            maybe_reply({noreply,State#state{active = Request}});
        {error, Reason} ->
            lager:warning("Socket error while sending riakc request: ~p.", [Reason]),
            gen_tcp:close(State#state.sock);
        Other ->
            lager:warning("Socket error while sending riakc request: ~p.", [Other]),
            gen_tcp:close(State#state.sock)
    end.

%% Unencoded Request (the normal PB client path)
encode_request_message(#request{msg=Msg}=Req) ->
    {Req, riak_pb_codec:encode(Msg)}.

%% maybe_reply({reply, Reply, State = #state{active = Request}}) ->
%%   NewRequest = send_caller(Reply, Request),
%%   State#state{active = NewRequest};

maybe_reply({noreply, State = #state{}}) ->
    State.

%% Replies the message and clears the requester id
send_caller(Msg, #request{from = From}=Request) when From /= undefined ->
    gen_server:reply(From, Msg),
    Request#request{from = undefined}.

%% @private
%% Cancel a request timer made by create_timer/2
cancel_req_timer(undefined) ->
    ok;
cancel_req_timer(Tref) ->
    _ = erlang:cancel_timer(Tref),
    ok.

single_up_req(Key, Value, PartitionId, Pid) ->
    Req = #fpbsingleupreq{key=list_to_binary(Key), value=term_to_binary(Value), partition_id=PartitionId},
    Result = call_infinity(Pid, {req, Req, ?TIMEOUT}),
    case Result of
        {ok, CommitTime} -> {ok, CommitTime};
        error -> error;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end.

start_tx(Clock, Pid) ->
    Req = #fpbstarttxnreq{clock=Clock},
    Result = call_infinity(Pid, {req, Req, ?TIMEOUT}),
    case Result of
        {ok, TxId} -> {ok, TxId};
        error -> error;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end.

get_hash_fun(Pid) ->
    Req = #fpbpartlistreq{noop=true},
    Result = call_infinity(Pid, {req, Req, ?TIMEOUT}),
    case Result of
        {ok, Value} -> {ok, Value};
        error -> error;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end.

certify(Pid, {Time, ProcId}, LocalUpdates, RemoteUpdates, MyId) -> 
    Req = #fpbpreptxnreq{txid=#fpbtxid{snapshot=Time, pid=term_to_binary(ProcId)}, 
            local_updates=encode_nodeups(LocalUpdates), 
            remote_updates=encode_nodeups(RemoteUpdates), threadid=MyId},
    Result = call_infinity(Pid, {req, Req, ?TIMEOUT}),
    case Result of
        {ok, Value} -> {ok, Value};
        error -> error;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end.

single_read(Pid, {Time, ProcId}, Key, PartitionId) ->
    Req = #fpbreadreq{key=integer_to_list(Key), txid=#fpbtxid{snapshot=Time, pid=term_to_binary(ProcId)},
            partition_id=PartitionId},
    Result = call_infinity(Pid, {req, Req, ?TIMEOUT}),
    case Result of
        {ok, Value} -> {ok, Value};
        error -> error;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end.

read(TxId, Key, PartitionId, Pid) ->
    Req = #fpbreadreq{key=list_to_binary(Key), txid=TxId, partition_id=PartitionId},
    Result = call_infinity(Pid, {req, Req, ?TIMEOUT}),
    case Result of
        {ok, Value} -> {ok, Value};
        error -> error;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end.

%% Atomically stores multiple CRDTs converting the object state to a
%% list of operations that will be appended to the log.
general_tx(Operations,Pid) ->
    general_tx(Operations, ignore, Pid).
-spec general_tx([term()], ignore|term(), pid()) -> {ok, term()} | error | {error, term()}.
general_tx(Operations, Clock, Pid) ->
    TxnRequest =  case Clock of 
                      ignore ->
                          #fpbtxnreq{clock=term_to_binary(ignore),ops=encode_general_txn(Operations)};
                      _ ->
                          #fpbtxnreq{clock=Clock,ops=encode_general_txn(Operations)}
                  end,
    Result = call_infinity(Pid, {req, TxnRequest, ?TIMEOUT}),
    case Result of
        {ok, CommitTime} -> {ok, CommitTime};
        error -> error;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end.

%% Encode Atomic store crdts request into the
%% pb request message structure to be serialized
encode_general_txn(Operations) ->
    lists:map(fun(Op) -> encode_general_txn_op(Op) end, Operations).

encode_general_txn_op({update, Key, Op, Param}) ->
    #fpbtxnop{type=0, key=Key, operation=get_op_id(Op), parameter=term_to_binary(Param)};
encode_general_txn_op({read, Key}) ->
    #fpbtxnop{type=1, key=Key}.

encode_nodeups(Updates) ->
    FoldUps = fun({Key, Value}, Acc) -> [#fpbupdate{key=integer_to_list(Key), 
                value=#fpbvalue{field=12, str_value=Value}}|Acc] end,
    #fpbnodeups{per_nodeup=lists:map(fun({NodeId, Part, Ups}) ->  
                #fpbpernodeup{node_id=NodeId, partition_id=Part, 
                    ups=lists:foldl(FoldUps, [], Ups)} end, Updates)}.    

%% Decode response of pb request
decode_response(#fpbtxnresp{success = Success, clock=Clock, results=Result}) ->
    case Success of
        true ->
            Res = lists:map(fun(X) -> decode_response(X) end, Result),
            {Clock, Res};
        _ ->
            {error, request_failed}
    end;
decode_response(#fpbpreptxnresp{success = Success, commit_time=CommitTime}) ->
    case Success of
        true ->
            {ok, CommitTime};
        _ ->
            {error, request_failed}
    end;
decode_response(#fpbpartlist{node_parts=NodeParts}) ->
    lists:map(fun(#fpbnodepart{ip=Ip, num_partitions=N}) -> {list_to_atom(Ip), N}  end ,  NodeParts); 
decode_response(#fpbvalue{field=12, str_value=Value}) ->
    {ok, Value};
decode_response(#fpbvalue{field=0}) ->
    {ok, []};
decode_response(#fpbtxid{}=TxId) ->
    {ok, TxId};
decode_response(Resp) ->
    lager:error("Unexpected Message ~p",[Resp]),
    error.

get_op_id(increment) ->
    0;
get_op_id(decrement) ->
    1;
get_op_id(assign) ->
    2;
get_op_id(add) ->
    3;
get_op_id(remove) ->
    4.

