
function submit_lexed_rows!(parsing_queue, consume_ctx, parsing_ctx, row_num)
    task_size = estimate_task_size(parsing_ctx)
    ntasks = cld(length(parsing_ctx.eols), task_size)
    # Set the expected number of parsing tasks
    setup_tasks!(consume_ctx, parsing_ctx, ntasks)
    # Send task definitions (segment of `eols` to process) to the queue
    task_start = Int32(1)
    for task in Iterators.partition(eachindex(parsing_ctx.eols), task_size)
        task_end = Int32(last(task))
        put!(parsing_queue, (task_start, task_end, row_num, parsing_ctx.id == 1))
        row_num += Int(task_end - task_start)
        task_start = task_end
    end
    return row_num
end

function read_and_lex_task!(parsing_queue::Channel{T}, lexer::Lexer, parsing_ctx::ParsingContext, parsing_ctx_next::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options) where {T}
    limit_eols!(parsing_ctx, 1) && return
    uses_single_buffer = parsing_ctx_next.id == parsing_ctx.id
    row_num = submit_lexed_rows!(parsing_queue, consume_ctx, parsing_ctx, 1)
    @inbounds while true
        # Start parsing _next_ chunk of input
        if !lexer.done
            last_newline_at = Int(last(parsing_ctx.eols))
            uses_single_buffer || unsafe_copyto!(parsing_ctx_next.bytes, last_newline_at, parsing_ctx.bytes, last_newline_at, length(parsing_ctx.bytes) - last_newline_at + 1)
            read_and_lex!(lexer, parsing_ctx_next, last_newline_at)
            limit_eols!(parsing_ctx_next, row_num) && break
            row_num = submit_lexed_rows!(parsing_queue, consume_ctx, parsing_ctx_next, row_num)
        end
        # Wait for parsers to finish processing current chunk
        sync_tasks(consume_ctx, parsing_ctx)
        lexer.done && break
        # Switch contexts
        parsing_ctx, parsing_ctx_next = parsing_ctx_next, parsing_ctx
    end
end

function process_and_consume_task(worker_id, parsing_queue::Channel{T}, result_buffers::Vector{TaskResultBuffer{M}}, consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, parsing_ctx_next::ParsingContext, options::Parsers.Options) where {T,M}
    trace = get_parser_task_trace(worker_id)
    try
        @inbounds while true
            (task_start, task_end, row_num, use_current_context) = take!(parsing_queue)
            # We prepared 2 * nworkers result buffers, as there are might 2 chunks in flight and 
            # since the user might provide their own consume! methods which won't block like the default 
            # consume!, not separating the result buffers per chunk could lead to data corruption if 
            # the results from the 2nd chunk are ready before the 1st chunk is consumed.
            result_buf = result_buffers[worker_id + (tasks_per_chunk(parsing_ctx) * use_current_context)]
            push!(trace, time_ns())
            iszero(task_end) && break
            ctx = ifelse(use_current_context, parsing_ctx, parsing_ctx_next)
            _parse_rows_forloop!(result_buf, @view(ctx.eols.elements[task_start:task_end]), ctx.bytes, ctx.enum_schema, options, parsing_ctx.comment)
            consume!(consume_ctx, ctx, result_buf, row_num, task_start)
            task_done!(consume_ctx, ctx, result_buf)
            push!(trace, time_ns())
        end
    catch e
        ce = CapturedException(e, catch_backtrace())
        # If there was an exception, immediately stop processing the queue
        isopen(parsing_queue) && close(parsing_queue, ce)
        # if the io_task was waiting for work to finish, we'll interrupt it here
        Base.@lock parsing_ctx.cond.cond_wait begin
            isnothing(parsing_ctx.cond.exception) && (parsing_ctx.cond.exception = ce)
            notify(parsing_ctx.cond.cond_wait, ce, all=true, error=true)
        end

        Base.@lock parsing_ctx_next.cond.cond_wait begin
            isnothing(parsing_ctx_next.cond.exception) && (parsing_ctx_next.cond.exception = ce)
            notify(parsing_ctx_next.cond.cond_wait, ce, all=true, error=true)
        end
    end
end

function _parse_file_parallel(lexer::Lexer, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, ::Val{M}) where {M}
    parsing_queue = Channel{Tuple{Int32,Int32,Int,Bool}}(Inf)
    result_buffers = TaskResultBuffer{M}[TaskResultBuffer{M}(id, parsing_ctx.schema, cld(length(parsing_ctx.eols), tasks_per_chunk(parsing_ctx))) for id in 1:total_result_buffers_count(parsing_ctx)]
    if lexer.done
        parsing_ctx_next = parsing_ctx
    else
        parsing_ctx_next = ParsingContext(
            2,
            parsing_ctx.schema,
            parsing_ctx.enum_schema,
            parsing_ctx.header,
            Vector{UInt8}(undef, length(parsing_ctx.bytes)),
            BufferedVector{Int32}(Vector{Int32}(undef, length(parsing_ctx.eols)), 0),
            parsing_ctx.limit,
            parsing_ctx.nworkers,
            parsing_ctx.escapechar,
            TaskCondition(),
            parsing_ctx.comment,
        )
        unsafe_push!(parsing_ctx_next.eols, Int32(0))
    end
    parser_tasks = sizehint!(Task[], parsing_ctx.nworkers)
    for i in 1:parsing_ctx.nworkers
        t = Threads.@spawn process_and_consume_task($i, $parsing_queue, $result_buffers, $consume_ctx, $parsing_ctx, $parsing_ctx_next, $options)
        push!(parser_tasks, t)
    end

    try
        io_task = Threads.@spawn read_and_lex_task!($parsing_queue, $lexer, $parsing_ctx, $parsing_ctx_next, $consume_ctx, $options)
        wait(io_task)
    catch e
        isopen(parsing_queue) && close(parsing_queue, e)
        cleanup(consume_ctx, e)
        rethrow()
    end
    # Cleanup
    for _ in 1:parsing_ctx.nworkers
        put!(parsing_queue, (Int32(0), Int32(0), 0, true))
    end
    foreach(wait, parser_tasks)
    close(parsing_queue)
    return nothing
end
