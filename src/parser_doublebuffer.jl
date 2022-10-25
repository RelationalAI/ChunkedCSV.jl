# TODO: start processing the "next" buffer as soon as it is ready (now we wait for the previous one to be fully processed)
function read_and_lex_task!(parsing_queue::Channel{T}, lexer_state::LexerState{B}, parsing_ctx::ParsingContext, parsing_ctx_next::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options) where {T,B}
    row_num = UInt32(1)
    parsers_should_use_current_context = true
    done = lexer_state.done
    @inbounds while true
        limit_eols!(parsing_ctx, row_num) && break
        task_size = estimate_task_size(parsing_ctx)
        ntasks = cld(length(parsing_ctx.eols), task_size)
        # Set the expected number of parsing tasks
        setup_tasks!(consume_ctx, parsing_ctx, ntasks)
        # Send task definitions (segment of `eols` to process) to the queue
        task_start = UInt32(1)
        task_num = UInt32(1)
        for task in Iterators.partition(eachindex(parsing_ctx.eols), task_size)
            task_end = UInt32(last(task))
            put!(parsing_queue, (task_start, task_end, row_num, task_num, parsers_should_use_current_context))
            row_num += task_end - task_start
            task_start = task_end
            task_num += UInt32(1)
        end
        # Start parsing _next_ chunk of input
        if !done
            last_newline_at = lexer_state.last_newline_at
            unsafe_copyto!(parsing_ctx_next.bytes, last_newline_at, parsing_ctx.bytes, last_newline_at, UInt32(length(parsing_ctx.bytes)) - last_newline_at + 1)
            read_and_lex!(lexer_state, parsing_ctx_next, options)
        end
        # Wait for parsers to finish processing current chunk
        sync_tasks(consume_ctx, parsing_ctx, ntasks)
        done && break
        # Switch contexts
        parsing_ctx, parsing_ctx_next = parsing_ctx_next, parsing_ctx
        parsers_should_use_current_context = !parsers_should_use_current_context
        done = lexer_state.done
    end
end

function process_and_consume_task(parsing_queue::Channel{T}, result_buffers::Vector{TaskResultBuffer{N,M}}, consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, parsing_ctx_next::ParsingContext, options::Parsers.Options) where {T,N,M}
    try
        @inbounds while true
            (task_start, task_end, row_num, task_num, use_current_context) = take!(parsing_queue)
            iszero(task_end) && break
            result_buf = result_buffers[task_num]
            ctx = ifelse(use_current_context, parsing_ctx, parsing_ctx_next)
            _parse_rows_forloop!(result_buf, @view(ctx.eols.elements[task_start:task_end]), ctx.bytes, ctx.schema, options)
            consume!(consume_ctx, ctx, result_buf, row_num, task_start)
            task_done!(consume_ctx, ctx, result_buf)
        end
    catch e
        @error "Task failed" exception=(e, catch_backtrace())
        # If there was an exception, immediately stop processing the queue
        close(parsing_queue, e)
        # if the io_task was waiting for work to finish, we'll interrupt it here
        @lock parsing_ctx.cond.cond_wait begin
            notify(parsing_ctx.cond.cond_wait, e, all=true, error=true)
        end
        @lock parsing_ctx_next.cond.cond_wait begin
            notify(parsing_ctx_next.cond.cond_wait, e, all=true, error=true)
        end
    end
end

function _parse_file_doublebuffer(lexer_state::LexerState, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, ::Val{N}, ::Val{M}) where {N,M}
    parsing_queue = Channel{Tuple{UInt32,UInt32,UInt32,UInt32,Bool}}(Inf)
    result_buffers = TaskResultBuffer{N,M}[TaskResultBuffer{N,M}(id, parsing_ctx.schema, cld(length(parsing_ctx.eols), parsing_ctx.maxtasks)) for id in 1:parsing_ctx.nresults]
    parsing_ctx_next = ParsingContext(
        parsing_ctx.schema,
        parsing_ctx.header,
        Vector{UInt8}(undef, length(parsing_ctx.bytes)),
        BufferedVector{UInt32}(Vector{UInt32}(undef, length(parsing_ctx.eols)), 0),
        parsing_ctx.limit,
        parsing_ctx.nworkers,
        parsing_ctx.maxtasks,
        parsing_ctx.nresults,
        parsing_ctx.escapechar,
        TaskCondition(),
    )
    parser_tasks = Task[]
    for i in 1:parsing_ctx.nworkers
        t = Threads.@spawn process_and_consume_task(parsing_queue, result_buffers, consume_ctx, parsing_ctx, parsing_ctx_next, options)
        push!(parser_tasks, t)
        if i < parsing_ctx.nworkers
            consume_ctx = maybe_deepcopy(consume_ctx)
        end
    end

    try
        io_task = Threads.@spawn read_and_lex_task!(parsing_queue, lexer_state, parsing_ctx, parsing_ctx_next, consume_ctx, options)
        wait(io_task)
    catch e
        close(parsing_queue, e)
        rethrow()
    end
    # Cleanup
    for _ in 1:parsing_ctx.nworkers
        put!(parsing_queue, (UInt32(0), UInt32(0), UInt32(0), UInt32(0), true))
    end
    foreach(wait, parser_tasks)
    close(parsing_queue)
    return nothing
end
