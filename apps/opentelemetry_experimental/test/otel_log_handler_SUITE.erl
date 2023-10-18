-module(otel_log_handler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-define(OTEL_LOG_HANDLER, otel_log_handler).
-define(LOG_MSG, "otel_log_hanlder_SUITE test, please ignore it").
-define(TRACEPARENT, "00-0226551413cd73a554184b324c82ad51-b7ad6b71432023a2-01").

all() ->
    [crud_test,
     exporting_runner_timeout_test,
     check_table_size_max_queue_test,
     export_max_batch_success_test,
     scheduled_export_success_test,
     flush_on_terminate_test,
     sanity_end_to_end_test].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(exporting_runner_timeout_test = TC, Config) ->
    Config1 = common_testcase_init(Config),
    ExporterConf = {?MODULE, #{sleep => infinity}},
    HandlerConf = #{config => #{exporter => ExporterConf,
                                exporting_timeout_ms => 1,
                                scheduled_delay_ms => 1}},
    ok = logger:add_handler(TC, ?OTEL_LOG_HANDLER, HandlerConf),
    [{handler_id, TC} | Config1];
init_per_testcase(check_table_size_max_queue_test = TC, Config) ->
    Config1 = common_testcase_init(Config),
    ExporterConf = {?MODULE, #{success => true}},
    HandlerConf = #{config => #{exporter => ExporterConf,
                                exporting_timeout_ms => 1,
                                scheduled_delay_ms => timer:minutes(10),
                                check_table_size_ms => 1,
                                max_queue_size => 10}},
    ok = logger:add_handler(TC, ?OTEL_LOG_HANDLER, HandlerConf),
    [{handler_id, TC} | Config1];
init_per_testcase(export_max_batch_success_test = TC, Config) ->
    Config1 = common_testcase_init(Config),
    ExporterConf = {?MODULE, #{reply_to => self()}},
    HandlerConf = #{config => #{exporter => ExporterConf,
                                exporting_timeout_ms => 100,
                                scheduled_delay_ms => timer:minutes(10),
                                check_table_size_ms => 1,
                                max_queue_size => infinity,
                                max_export_batch_size => 10}},
    ok = logger:add_handler(TC, ?OTEL_LOG_HANDLER, HandlerConf),
    [{handler_id, TC} | Config1];
init_per_testcase(scheduled_export_success_test = TC, Config) ->
    Config1 = common_testcase_init(Config),
    ExporterConf = {?MODULE, #{reply_to => self()}},
    HandlerConf = #{config => #{exporter => ExporterConf,
                                exporting_timeout_ms => 10,
                                scheduled_delay_ms => 5,
                                check_table_size_ms => 1}},
    ok = logger:add_handler(TC, ?OTEL_LOG_HANDLER, HandlerConf),
    [{handler_id, TC} | Config1];
init_per_testcase(flush_on_terminate_test = TC, Config) ->
    Config1 = common_testcase_init(Config),
    ExporterConf = {?MODULE, #{reply_to => self()}},
    HandlerConf = #{config => #{exporter => ExporterConf,
                                exporting_timeout_ms => 10,
                                %% high values to make sure nothing is exported
                                %% before terminating the handler
                                scheduled_delay_ms => timer:minutes(10),
                                check_table_size_ms => timer:minutes(5),
                                max_export_batch_size => 1000,
                                max_queue_size => 10000}},
    ok = logger:add_handler(TC, ?OTEL_LOG_HANDLER, HandlerConf),
    [{handler_id, TC} | Config1];
init_per_testcase(sanity_end_to_end_test = TC, Config) ->
    Config1 = common_testcase_init(Config),
    ExporterConf = {?MODULE, #{otlp => true, reply_to => self()}},
    HandlerConf = #{config => #{exporter => ExporterConf,
                                exporting_timeout_ms => 1000,
                                %% setting scheduled delay high enough,
                                %% so that it doesn't occur during the test,
                                scheduled_delay_ms => timer:minutes(10),
                                %% the test will produce and expect 5 log events
                                max_export_batch_size => 5,
                                check_table_size_ms => 2}},
    ok = logger:add_handler(TC, ?OTEL_LOG_HANDLER, HandlerConf),
    [{handler_id, TC} | Config1];
init_per_testcase(_TC, Config) ->
    common_testcase_init(Config).

end_per_testcase(_TC, Config) ->
    case ?config(handler_id, Config) of
        undefined -> ok;
        Id -> _ = logger:remove_handler(Id)
    end,
    _ = common_testcase_cleanup(Config),
    ok.

crud_test(_Config) ->
    ExporterConf = {?MODULE, #{success => true}},
    HandlerConf = #{config => #{exporter => ExporterConf}},
    ok = logger:add_handler(otel_handler_test, ?OTEL_LOG_HANDLER, HandlerConf),
    ok = logger:add_handler(otel_handler_test1, ?OTEL_LOG_HANDLER, HandlerConf),
    ok = logger:remove_handler(otel_handler_test1),
    {ok, #{reg_name := RegName}} = logger:get_handler_config(otel_handler_test),
    true = erlang:is_process_alive(erlang:whereis(RegName)),

    InvalidHandlerConf = #{reg_name => new_reg_name},
    InvalidHandlerConf1 = #{config => #{exporter => {new_module, #{}}}},
    {error, reg_name_change_not_allowed} = logger:set_handler_config(otel_handler_test, InvalidHandlerConf),
    {error, exporter_change_not_allowed} = logger:set_handler_config(otel_handler_test, InvalidHandlerConf1),
    {error, reg_name_change_not_allowed} = logger:update_handler_config(otel_handler_test, InvalidHandlerConf),
    {error, exporter_change_not_allowed} = logger:update_handler_config(otel_handler_test, InvalidHandlerConf1),
    {error, exporter_change_not_allowed} = logger:update_handler_config(otel_handler_test, config,
                                                                        #{exporter => {new_module, #{}}}),

    NewValidConf = #{max_queue_size => infinity,
                     exporting_timeout_ms => 5000,
                     scheduled_delay_ms => 3000,
                     check_table_size_ms => 500},
    ok = logger:update_handler_config(otel_handler_test, #{config => NewValidConf}),
    ok = logger:update_handler_config(otel_handler_test, config, NewValidConf),
    ok = logger:set_handler_config(otel_handler_test, #{config => NewValidConf}),

    NewInvalidConf = #{max_queue_size => -100,
                     exporting_timeout_ms => atom,
                     scheduled_delay_ms => "string",
                     check_table_size_ms => #{}},
    {error, [_|_]} = logger:update_handler_config(otel_handler_test, #{config => NewInvalidConf}),
    {error, [_|_]} = logger:update_handler_config(otel_handler_test, config, NewInvalidConf),
    {error, [_|_]} = logger:set_handler_config(otel_handler_test, #{config => NewInvalidConf}),

    NewInvalidConf1 = #{unknown_opt => 100},
    {error, [_|_]} = logger:update_handler_config(otel_handler_test, #{config => NewInvalidConf1}),
    {error, [_|_]} = logger:update_handler_config(otel_handler_test, config, NewInvalidConf1),
    {error, [_|_]} = logger:set_handler_config(otel_handler_test, #{config => NewInvalidConf1}),

    NewInvalidConf2 = #{max_export_batch_size => 100, max_queue_size => 10},
    {error, [_|_]} = logger:update_handler_config(otel_handler_test, #{config => NewInvalidConf2}),
    {error, [_|_]} = logger:update_handler_config(otel_handler_test, config, NewInvalidConf2),
    {error, [_|_]} = logger:set_handler_config(otel_handler_test, #{config => NewInvalidConf2}),

    ok = logger:remove_handler(otel_handler_test).

exporting_runner_timeout_test(Config) ->
    HandlerId = ?config(handler_id, Config),
    {ok, #{reg_name := RegName} = HandlerConf} = logger:get_handler_config(HandlerId),

    Mon = erlang:monitor(process, RegName),

    %% Insert a few log events to make sure runner process will be spawned and killed
    %% because it hangs forever (see `init_per_testcase/2`
    %% and exporter behaviour defined in this module
    true = ?OTEL_LOG_HANDLER:log(log_event(), HandlerConf),
    true = ?OTEL_LOG_HANDLER:log(log_event(), HandlerConf),

    receive
        {'DOWN', Mon, process, _Pid, _} ->
            %% test is to ensure we don't hit this
            ct:fail(otel_log_handler_crash)
    after 500 ->
            ok
    end.

check_table_size_max_queue_test(Config) ->
    HandlerId = ?config(handler_id, Config),
    {ok, #{reg_name := RegName,
           config := #{max_queue_size := MaxQueueSize,
                       check_table_size_ms := CheckTableSizeMs}
          } = HandlerConf} = logger:get_handler_config(HandlerId),

    true = ?OTEL_LOG_HANDLER:log(log_event(), HandlerConf),
    true = ?OTEL_LOG_HANDLER:log(log_event(), HandlerConf),

    lists:foreach(fun(_) ->
                          ?OTEL_LOG_HANDLER:log(log_event(), HandlerConf)
                  end,
                  lists:seq(1, MaxQueueSize)),
    %% Wait for more than CheckTableSizeMs to be sure check timeout occurred
    timer:sleep(CheckTableSizeMs * 5),

    dropped = ?OTEL_LOG_HANDLER:log(log_event(), #{reg_name => RegName}).

export_max_batch_success_test(Config) ->
    HandlerId = ?config(handler_id, Config),
    {ok, #{config := #{max_export_batch_size := MaxBatchSize}}
     = HandlerConf} = logger:get_handler_config(HandlerId),

    erlang:spawn(fun() -> lists:foreach(
                            fun(_) -> ?OTEL_LOG_HANDLER:log(log_event(), HandlerConf) end,
                            lists:seq(1, MaxBatchSize))
                 end),
    %% Export must be triggered by reaching max_export_batch_size because
    %% scheduled_delay_ms is deliberately set to a high value
    receive
        {exported, Size} ->
            ?assertEqual(MaxBatchSize, Size)
    after 5000 ->
            ct:fail(otel_log_handler_exporter_failed)
    end.

scheduled_export_success_test(Config) ->
    HandlerId = ?config(handler_id, Config),
    {ok, HandlerConf} = logger:get_handler_config(HandlerId),

    LogsNum = 10,
    erlang:spawn(fun() -> lists:foreach(
                            fun(_) -> ?OTEL_LOG_HANDLER:log(log_event(), HandlerConf) end,
                            lists:seq(1, LogsNum))
                 end),
    receive
        {exported, Size} ->
            ?assertEqual(LogsNum, Size)
    after 5000 ->
            ct:fail(otel_log_handler_exporter_failed)
    end.

flush_on_terminate_test(Config) ->
    HandlerId = ?config(handler_id, Config),
    {ok, #{reg_name := RegName} = HandlerConf} = logger:get_handler_config(HandlerId),

    LogsNum = 15,
    lists:foreach(fun(_) -> ?OTEL_LOG_HANDLER:log(log_event(), HandlerConf) end,
                  lists:seq(1, LogsNum)),
    ?assert(erlang:is_pid(erlang:whereis(RegName))),
    ok = logger:remove_handler(HandlerId),
    receive
        {exported, Size} ->
            ?assertEqual(LogsNum, Size)
    after 5000 ->
            ct:fail(otel_log_handler_exporter_failed)
    end,
    ?assertEqual(undefined, erlang:whereis(RegName)).

sanity_end_to_end_test(Config) ->
    HandlerId = ?config(handler_id, Config),
    {ok, #{reg_name := RegName}} = logger:get_handler_config(HandlerId),
    Mon = erlang:monitor(process, RegName),

    otel_propagator_text_map:extract(otel_propagator_trace_context,[{"traceparent",?TRACEPARENT}]),
    [_, TraceId, SpanId, _] = string:split(?TRACEPARENT, "-", all),
    LoggerMeta = logger:get_process_metadata(),
    ?assertMatch(#{otel_span_id := SpanId, otel_trace_id := TraceId}, LoggerMeta),
    %% the number of log events must be 5, since init_per_testcase set max_export_batch_size = 5
    logger:warning(?LOG_MSG),
    logger:error(?LOG_MSG),
    logger:warning(#{msg => ?LOG_MSG, foo => [bar, baz]}),
    logger:error(#{msg => ?LOG_MSG, foo => {bar, [baz]}}),
    logger:warning(?LOG_MSG ++ "test term: ~p", [#{foo => bar}]),
    receive
        {exported, Data} ->
            #{resource_logs := [#{scope_logs := [#{log_records := LogRecords}]}]} = Data,
            ?assertEqual(5, length(LogRecords)),
            {{Y, M, D}, _} = calendar:universal_time(),
            lists:foreach(
              fun(#{time_unix_nano := T, observed_time_unix_nano := T,
                    trace_id := LogTraceId, span_id := LogSpanId}) ->
                      %% this is to check that timestamps unit is actually nanosecond,
                      %% as otel_otlp_logs relies on OTP logger producing microsecond timestamps,
                      %% if it's not nanosecond, calendar:system_time_to_universal_time/2
                      %% is expected to produce some unrealistic dates
                      {{LogY, LogM, LogD}, _} = calendar:system_time_to_universal_time(T, nanosecond),
                      ?assertEqual({Y, M, D}, {LogY, LogM, LogD}),
                      ?assertEqual(hex_str_to_bin(TraceId, 128), LogTraceId),
                      ?assertEqual(hex_str_to_bin(SpanId, 64), LogSpanId)
              end,
              LogRecords)
    after 5000 ->
            ct:fail(otel_log_handler_exporter_failed)
    end,

    %% No crash is expected during the test
    receive
        {'DOWN', Mon, process, _Pid, _} ->
            %% test is to ensure we don't hit this
            ct:fail(otel_log_handler_crash)
    after 100 ->
            ok
    end.

%% exporter behaviour

init(ExpConfig) ->
    {ok, ExpConfig}.

export(logs, {_Tab, _LogHandlerConfig}, _Resource, #{sleep := Time} =_State) ->
    timer:sleep(Time),
    ok;
export(logs, {_Tab, _LogHandlerConfig}, _Resource, #{success := true} =_State) ->
    ok;
export(logs, {Tab, LogHandlerConfig}, Resource, #{otlp := true, reply_to := Pid} = _State) ->
    Res = otel_otlp_logs:to_proto(Tab, Resource, LogHandlerConfig),
    Pid ! {exported, Res};
export(logs, {Tab, _LogHandlerConfig}, _Resource, #{reply_to := Pid} = _State) ->
    Pid ! {exported, ets:info(Tab, size)},
    ok.
shutdown(_) ->
    ok.

%% helpers

common_testcase_init(Config) ->
    {ok, _} = application:ensure_all_started(opentelemetry_experimental),
    Config.

common_testcase_cleanup(_Config) ->
    _ = application:stop(opentelemetry_experimental),
    ok.

log_event() ->
    #{level => warning,
      meta => #{gl => erlang:group_leader() ,pid => erlang:self(), time => os:system_time(microsecond)},
      msg => {string, ?LOG_MSG}}.

hex_str_to_bin(Str, Size) ->
    B = iolist_to_binary(Str),
    <<(binary_to_integer(B, 16)):Size>>.
