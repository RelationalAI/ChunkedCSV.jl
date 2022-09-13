function read_and_lex_task!(parsing_queue::Channel, io, parsing_ctx::ParsingContext, parsing_ctx_next::ParsingContext, options::Parsers.Options, byteset, last_chunk_newline_at, quoted, done)
    row_num = UInt32(1)
    parsers_should_use_current_context = true
    @inbounds while true
        # We start with a parsing_ctx that is already lexed from
        # initially from `parse_preamble`, later from from this function
        eols = parsing_ctx.eols[]
        task_size = max(1_000, cld(length(eols), parsing_ctx.maxtasks))
        task_start = UInt32(1)
        ntasks = cld(length(eols), task_size)

        # Spawn parsing tasks
        @lock parsing_ctx.cond.cond_wait begin
            parsing_ctx.cond.ntasks = ntasks
        end
        for task in Iterators.partition(eols, task_size)
            task_end = task_start + UInt32(length(task)) - UInt32(1)
            put!(parsing_queue, (task_start, task_end, row_num, parsers_should_use_current_context))
            row_num += UInt32(length(task))
            task_start = task_end + UInt32(1)
        end

        # Start parsing _next_ chunk of input
        if !done
            empty!(parsing_ctx_next.eols)
            push!(parsing_ctx_next.eols, UInt32(0))
            parsing_ctx_next.bytes[last_chunk_newline_at:end] .= parsing_ctx.bytes[last_chunk_newline_at:end]
            bytes_read_in = prepare_buffer!(io, parsing_ctx_next.bytes, last_chunk_newline_at)
            (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, parsing_ctx_next, options, byteset, bytes_read_in, quoted)
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
    end
end

function process_and_consume_task(parsing_queue::Threads.Channel{T}, parsing_ctx::ParsingContext, parsing_ctx_next::ParsingContext, options::Parsers.Options, result_buf::TaskResultBuffer, consume_ctx::AbstractConsumeContext) where {T}
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
end

function _parse_file_doublebuffer(io, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, last_chunk_newline_at::UInt32, quoted::Bool, done::Bool, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    parsing_queue = Channel{Tuple{UInt32,UInt32,UInt32,Bool}}(Inf)
    parsing_ctx_next = ParsingContext(
        parsing_ctx.schema,
        parsing_ctx.header,
        Vector{UInt8}(undef, length(parsing_ctx.bytes)),
        BufferedVector{UInt32}(Vector{UInt32}(undef, parsing_ctx.eols.occupied), 0),
        parsing_ctx.limit,
        parsing_ctx.nworkers,
        parsing_ctx.maxtasks,
        TaskCondition(0, Threads.Condition(ReentrantLock())),
    )
    parser_tasks = Task[]
    for _ in 1:parsing_ctx.nworkers
        result_buf = TaskResultBuffer{N}(parsing_ctx.schema, cld(length(parsing_ctx.eols), parsing_ctx.maxtasks))
        push!(parser_tasks, errormonitor(Threads.@spawn process_and_consume_task(parsing_queue, parsing_ctx, parsing_ctx_next, options, result_buf, consume_ctx)))
    end
    io_task = errormonitor(Threads.@spawn read_and_lex_task!(parsing_queue, io, parsing_ctx, parsing_ctx_next, options, byteset, last_chunk_newline_at, quoted, done))
    wait(io_task)
    for _ in 1:parsing_ctx.nworkers
        put!(parsing_queue, (UInt32(0), UInt32(0), UInt32(0), true))
    end
    foreach(wait, parser_tasks)
    close(parsing_queue)
    return nothing
end


