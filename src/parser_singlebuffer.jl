function read_and_lex_task!(parsing_queue::Channel{T}, lexer_state::LexerState{B}, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options) where {T,B}
    row_num = UInt32(1)
    @inbounds while true
        limit_eols!(parsing_ctx, row_num) && break
        task_size = estimate_task_size(parsing_ctx)
        ntasks = cld(length(parsing_ctx.eols), task_size)
        # Set the expected number of parsing tasks
        setup_tasks!(consume_ctx, parsing_ctx, ntasks)
        # Send task definitions (segmenf of `eols` to process) to the queue
        task_start = UInt32(1)
        task_num = UInt32(1)
        for task in Iterators.partition(eachindex(parsing_ctx.eols), task_size)
            task_end = UInt32(last(task))
            put!(parsing_queue, (task_start, task_end, row_num, task_num))
            row_num += task_end - task_start
            task_start = task_end
            task_num += UInt32(1)
        end
        # Wait for parsers to finish processing current chunk
        sync_tasks(consume_ctx, parsing_ctx, ntasks)
        lexer_state.done && break
        read_and_lex!(lexer_state, parsing_ctx, options)
    end # while true
end

function process_and_consume_task(parsing_queue::Channel{T}, result_buffers::Vector{TaskResultBuffer{N,M}}, consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, options::Parsers.Options) where {T,N,M}
    try
        @inbounds while true
            task_start, task_end, row_num, task_num = take!(parsing_queue)
            iszero(task_end) && break
            result_buf = result_buffers[task_num]
            _parse_rows_forloop!(result_buf, @view(parsing_ctx.eols.elements[task_start:task_end]), parsing_ctx.bytes, parsing_ctx.schema, options)
            consume!(consume_ctx, parsing_ctx, result_buf, row_num, task_start)
            task_done!(consume_ctx, parsing_ctx, result_buf)
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

function _parse_file_singlebuffer(lexer_state::LexerState{B}, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, ::Val{N}, ::Val{M}) where {B,N,M}
    parsing_queue = Channel{Tuple{UInt32,UInt32,UInt32,UInt32}}(Inf)
    result_buffers = TaskResultBuffer{N,M}[TaskResultBuffer{N,M}(id, parsing_ctx.schema, cld(length(parsing_ctx.eols), parsing_ctx.maxtasks)) for id in 1:parsing_ctx.nresults]
    parser_tasks = Task[]
    for i in 1:parsing_ctx.nworkers
        t = Threads.@spawn process_and_consume_task(parsing_queue, result_buffers, consume_ctx, parsing_ctx, options)
        push!(parser_tasks, t)
        if i < parsing_ctx.nworkers
            consume_ctx = maybe_deepcopy(consume_ctx)
        end
    end
    try
        io_task = Threads.@spawn read_and_lex_task!(parsing_queue, lexer_state, parsing_ctx, consume_ctx, options)
        wait(io_task)
    catch e
        close(parsing_queue, e)
        rethrow()
    end
    # Cleanup
    for _ in 1:parsing_ctx.nworkers
        put!(parsing_queue, (UInt32(0), UInt32(0), UInt32(0), UInt32(0)))
    end
    foreach(wait, parser_tasks)
    close(parsing_queue)
    return nothing
end
