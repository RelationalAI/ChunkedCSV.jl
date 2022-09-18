function _parse_file_singlebuffer(io, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, last_chunk_newline_at::UInt32, quoted::Bool, done::Bool, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    row_num = UInt32(1)
    queue = Channel{Tuple{UInt32,UInt32,UInt32}}(Inf)
    parser_tasks = Task[]
    for i in 1:parsing_ctx.nworkers
        result_buf = TaskResultBuffer{N}(parsing_ctx.schema, cld(length(parsing_ctx.eols), parsing_ctx.maxtasks))
        push!(parser_tasks, errormonitor(Threads.@spawn process_and_consume_task(queue, parsing_ctx, options, result_buf, consume_ctx)))
        if i < parsing_ctx.nworkers
            consume_ctx = maybe_deepcopy(consume_ctx)
        end
    end
    while true
        # Updates eols_buf with new newlines, byte buffer was updated either from initialization stage or at the end of the loop
        limit_eols!(parsing_ctx, row_num) && break
        eols = parsing_ctx.eols[]
        task_size = max(1_000, cld(length(eols), parsing_ctx.maxtasks)) # TODO: explicit parameters of for number of workers and number of tasks
        task_start = UInt32(1)
        ntasks = cld(length(eols), task_size)

        @lock parsing_ctx.cond.cond_wait begin
            parsing_ctx.cond.ntasks = ntasks
        end
        for task in Iterators.partition(eols, task_size)
            task_end = task_start + UInt32(length(task)) - UInt32(1)
            put!(queue, (task_start, task_end, row_num))
            row_num += UInt32(length(task) - 1)
            task_start = task_end + UInt32(1)
        end

        @lock parsing_ctx.cond.cond_wait begin
            while true
                parsing_ctx.cond.ntasks == 0 && break
                wait(parsing_ctx.cond.cond_wait)
            end
        end

        done && break
        empty!(parsing_ctx.eols)
        # We always end on a newline when processing a chunk, so we're inserting a dummy variable to
        # signal that. This works out even for the very first chunk.
        push!(parsing_ctx.eols, UInt32(0))
        bytes_read_in = prepare_buffer!(io, parsing_ctx.bytes, last_chunk_newline_at)
        (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, parsing_ctx, options, byteset, bytes_read_in, quoted)
    end # while !done
    # Cleanup
    for _ in 1:parsing_ctx.nworkers
        put!(queue, (UInt32(0), UInt32(0), UInt32(0)))
    end
    foreach(wait, parser_tasks)
    close(queue)
    return nothing
end

function process_and_consume_task(parsing_queue::Threads.Channel, parsing_ctx::ParsingContext, options::Parsers.Options, result_buf::TaskResultBuffer, consume_ctx::AbstractConsumeContext)
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
end
