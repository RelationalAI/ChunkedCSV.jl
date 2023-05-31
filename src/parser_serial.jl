function _parse_file_serial(lexer::Lexer, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options,::Type{CT}) where {CT}
    row_num = 1
    result_buf = TaskResultBuffer(0, parsing_ctx.schema, cld(length(parsing_ctx.eols), tasks_per_chunk(parsing_ctx)))
    try
        @inbounds while true
            limit_eols!(parsing_ctx, row_num) && break
            task_size = estimate_task_size(parsing_ctx)
            task_start = Int32(1)
            for task in Iterators.partition(eachindex(parsing_ctx.eols), task_size)
                setup_tasks!(consume_ctx, parsing_ctx, 1)
                task_end = Int32(last(task))
                _parse_rows_forloop!(result_buf, view(parsing_ctx.eols, task_start:task_end), parsing_ctx.bytes, parsing_ctx.schema, parsing_ctx.enum_schema, options, parsing_ctx.comment, CT)
                consume!(consume_ctx, parsing_ctx, result_buf, row_num, task_start)
                row_num += Int(task_end - task_start)
                task_start = task_end
                task_done!(consume_ctx, parsing_ctx)
                sync_tasks(consume_ctx, parsing_ctx)
            end
            lexer.done && break
            read_and_lex!(lexer, parsing_ctx)
        end # while true
    catch e
        close(parsing_ctx.counter, e)
        cleanup(consume_ctx, e)
        rethrow(e)
    end
    return nothing
end
