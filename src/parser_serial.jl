function _parse_file_serial(io, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, last_newline_at::UInt32, quoted::Bool, done::Bool, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    row_num = UInt32(1)
    limit_eols!(parsing_ctx, row_num)
    result_buf = TaskResultBuffer{N}(parsing_ctx.schema, cld(length(parsing_ctx.eols), parsing_ctx.maxtasks))
    while true
        task_size = estimate_task_size(parsing_ctx)
        for task in Iterators.partition(parsing_ctx.eols, task_size)
            _parse_rows_forloop!(result_buf, task, parsing_ctx.bytes, parsing_ctx.schema, options)
            consume!(result_buf, parsing_ctx, row_num, consume_ctx)
            row_num += UInt32(length(task) - 1)
        end
        done && break
        (last_newline_at, quoted, done) = read_and_lex!(io, parsing_ctx, options, byteset, last_newline_at, quoted)
        limit_eols!(parsing_ctx, row_num) && break
    end # while true
    return nothing
end