function read_and_lex_task!(parsing_queue::Channel, io, parsing_ctx::ParsingContext, parsing_ctx_next::ParsingContext, options::Parsers.Options, byteset, last_newline_at, quoted, done)
    row_num = UInt32(1)
    parsers_should_use_current_context = true
    @inbounds while true
        limit_eols!(parsing_ctx, row_num) && break
        task_size = estimate_task_size(parsing_ctx)
        ntasks = cld(length(parsing_ctx.eols), task_size)

        # Set the expected number of parsing tasks
        @lock parsing_ctx.cond.cond_wait begin
            parsing_ctx.cond.ntasks = ntasks
        end

        # Send task definitions (segmenf of `eols` to process) to the queue
        task_start = UInt32(1)
        for task in Iterators.partition(parsing_ctx.eols, task_size)
            task_end = task_start + UInt32(length(task)) - UInt32(1)
            put!(parsing_queue, (task_start, task_end, row_num, parsers_should_use_current_context))
            row_num += UInt32(length(task) - 1)
            task_start = task_end + UInt32(1)
        end

        # Start parsing _next_ chunk of input
        if !done
            parsing_ctx_next.bytes[last_newline_at:end] .= parsing_ctx.bytes[last_newline_at:end]
            (last_newline_at, quoted, next_done) = read_and_lex!(io, parsing_ctx_next, options, byteset, last_newline_at, quoted)
        end
        # Wait for parsers to finish processing current chunk
        @lock parsing_ctx.cond.cond_wait begin
            while true
                parsing_ctx.cond.ntasks == 0 && break
                wait(parsing_ctx.cond.cond_wait)
            end
        end
        done && break

        # Switch contexts
        parsing_ctx, parsing_ctx_next = parsing_ctx_next, parsing_ctx
        parsers_should_use_current_context = !parsers_should_use_current_context
        done = next_done
    end
end

function process_and_consume_task(parsing_queue::Threads.Channel{T}, parsing_ctx::ParsingContext, parsing_ctx_next::ParsingContext, options::Parsers.Options, result_buf::TaskResultBuffer, consume_ctx::AbstractConsumeContext) where {T}
    try
        @inbounds while true
            task_start, task_end, row_num, use_current_context = take!(parsing_queue)::T
            ctx = use_current_context ? parsing_ctx : parsing_ctx_next
            iszero(task_end) && break
            _parse_rows_forloop!(result_buf, @view(ctx.eols.elements[task_start:task_end]), ctx.bytes, ctx.schema, options)
            consume!(result_buf, ctx, row_num, consume_ctx)
            @lock ctx.cond.cond_wait begin
                ctx.cond.ntasks -= 1
                notify(ctx.cond.cond_wait)
            end
        end
    catch e
        @error "Task failed" exception=(e, catch_backtrace())
        # If there was an exception, immediately stop processing the queue
        close(parsing_queue, e)

        # if the io_task was waiting for work to finish, it we'll interrupt it here
        @lock parsing_ctx.cond.cond_wait begin
            notify(parsing_ctx.cond.cond_wait, e, all=true, error=true)
        end
        @lock parsing_ctx_next.cond.cond_wait begin
            notify(parsing_ctx_next.cond.cond_wait, e, all=true, error=true)
        end
    end
end

function _parse_file_doublebuffer(io, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, last_newline_at::UInt32, quoted::Bool, done::Bool, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    parsing_queue = Channel{Tuple{UInt32,UInt32,UInt32,Bool}}(Inf)
    parsing_ctx_next = ParsingContext(
        parsing_ctx.schema,
        parsing_ctx.header,
        Vector{UInt8}(undef, length(parsing_ctx.bytes)),
        BufferedVector{UInt32}(Vector{UInt32}(undef, length(parsing_ctx.eols)), 0),
        parsing_ctx.limit,
        parsing_ctx.nworkers,
        parsing_ctx.maxtasks,
        TaskCondition(0, Threads.Condition(ReentrantLock())),
    )
    parser_tasks = Task[]
    for i in 1:parsing_ctx.nworkers
        result_buf = TaskResultBuffer{N}(parsing_ctx.schema, cld(length(parsing_ctx.eols), parsing_ctx.maxtasks))
        t = Threads.@spawn process_and_consume_task(parsing_queue, parsing_ctx, parsing_ctx_next, options, result_buf, consume_ctx)
        push!(parser_tasks, t)
        if i < parsing_ctx.nworkers
            consume_ctx = maybe_deepcopy(consume_ctx)
        end
    end
    try
        io_task = Threads.@spawn read_and_lex_task!(parsing_queue, io, parsing_ctx, parsing_ctx_next, options, byteset, last_newline_at, quoted, done)
        wait(io_task)
    catch e
        close(parsing_queue, e)
        rethrow()
    end
    # Cleanup
    for _ in 1:parsing_ctx.nworkers
        put!(parsing_queue, (UInt32(0), UInt32(0), UInt32(0), true))
    end
    foreach(wait, parser_tasks)
    close(parsing_queue)
    return nothing
end


