function _parse_file_serial(lexer_state::LexerState, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, ::Val{N}, ::Val{M}) where {N,M}
    row_num = UInt32(1)
    result_buf = TaskResultBuffer{N,M}(0, parsing_ctx.schema, cld(length(parsing_ctx.eols), parsing_ctx.maxtasks))
    while true
        limit_eols!(parsing_ctx, row_num) && break
        task_size = estimate_task_size(parsing_ctx)
        setup_tasks!(consume_ctx, parsing_ctx, 0)
        task_start = UInt32(1)
        task_num = UInt32(1)
        for task in Iterators.partition(eachindex(parsing_ctx.eols), task_size)
            task_end = UInt32(last(task))
            _parse_rows_forloop!(result_buf, view(parsing_ctx.eols, task_start:task_end), parsing_ctx.bytes, parsing_ctx.schema, options)
            consume!(consume_ctx, parsing_ctx, result_buf, row_num, UInt32(1))
            row_num += UInt32(length(task))
            task_start = task_end
            task_num += UInt32(1)
            task_done!(consume_ctx, parsing_ctx, result_buf)
        end
        sync_tasks(consume_ctx, parsing_ctx, 0)
        lexer_state.done && break
        read_and_lex!(lexer_state, parsing_ctx, options)
    end # while true
    return nothing
end