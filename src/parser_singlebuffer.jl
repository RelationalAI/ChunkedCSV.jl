function read_and_lex_task!(parsing_queue::Channel, io, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, byteset, last_newline_at, quoted, done)
    row_num = UInt32(1)
    @inbounds while true
        limit_eols!(parsing_ctx, row_num) && break
        task_size = estimate_task_size(parsing_ctx)
        ntasks = cld(length(parsing_ctx.eols), task_size)

        # Set the expected number of parsing tasks
        @lock parsing_ctx.cond.cond_wait begin
            parsing_ctx.cond.ntasks = ntasks
        end

        preconsume!(consume_ctx, parsing_ctx, ntasks)
        # Send task definitions (segmenf of `eols` to process) to the queue
        task_start = UInt32(1)
        for task in Iterators.partition(parsing_ctx.eols, task_size)
            task_end = task_start + UInt32(length(task)) - UInt32(1)
            put!(parsing_queue, (task_start, task_end, row_num))
            row_num += UInt32(length(task) - 1)
            task_start = task_end + UInt32(1)
        end

        # Wait for parsers to finish processing current chunk
        @lock parsing_ctx.cond.cond_wait begin
            while true
                parsing_ctx.cond.ntasks == 0 && break
                wait(parsing_ctx.cond.cond_wait)
            end
        end
        postconsume!(consume_ctx, parsing_ctx, ntasks)
        done && break
        (last_newline_at, quoted, done) = read_and_lex!(io, parsing_ctx, options, byteset, last_newline_at, quoted)
    end # while true
end

function process_and_consume_task(parsing_queue::Threads.Channel, parsing_ctx::ParsingContext, options::Parsers.Options, result_buf::TaskResultBuffer, consume_ctx::AbstractConsumeContext)
    try
        @inbounds while true
            task_start, task_end, row_num = take!(parsing_queue)::Tuple{UInt32,UInt32,UInt32}
            iszero(task_end) && break
            _parse_rows_forloop!(result_buf, @view(parsing_ctx.eols.elements[task_start:task_end]), parsing_ctx.bytes, parsing_ctx.schema, options)
            consume!(result_buf, parsing_ctx, row_num, consume_ctx)
            @lock parsing_ctx.cond.cond_wait begin
                parsing_ctx.cond.ntasks -= 1
                notify(parsing_ctx.cond.cond_wait)
            end
        end
    catch e
        @error "Task failed" exception=(e, catch_backtrace())
        # If there was an exception, immediately stop processing the queue
        close(parsing_queue, e)

        # if the io/lexing was waiting for work to finish, we'll interrupt it here
        @lock parsing_ctx.cond.cond_wait begin
            notify(parsing_ctx.cond.cond_wait, e, all=true, error=true)
        end
    end
end

function _parse_file_singlebuffer(io, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, last_newline_at::UInt32, quoted::Bool, done::Bool, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    parsing_queue = Channel{Tuple{UInt32,UInt32,UInt32}}(Inf)
    parser_tasks = Task[]
    for i in 1:parsing_ctx.nworkers
        result_buf = TaskResultBuffer{N}(parsing_ctx.schema, cld(length(parsing_ctx.eols), parsing_ctx.maxtasks))
        t = Threads.@spawn process_and_consume_task(parsing_queue, parsing_ctx, options, result_buf, consume_ctx)
        push!(parser_tasks, t)
        if i < parsing_ctx.nworkers
            consume_ctx = maybe_deepcopy(consume_ctx)
        end
    end
    try
        io_task = Threads.@spawn read_and_lex_task!(parsing_queue, io, parsing_ctx, consume_ctx, options, byteset, last_newline_at, quoted, done)
        wait(io_task)
    catch e
        close(parsing_queue, e)
        rethrow()
    end
    # Cleanup
    for _ in 1:parsing_ctx.nworkers
        put!(parsing_queue, (UInt32(0), UInt32(0), UInt32(0)))
    end
    foreach(wait, parser_tasks)
    close(parsing_queue)
    return nothing
end
