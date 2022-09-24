function _parse_file_serial(io, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, last_newline_at::UInt32, quoted::Bool, done::Bool, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    row_num = UInt32(1)
    schema = parsing_ctx.schema
    limit_eols!(parsing_ctx, row_num)
    result_buf = TaskResultBuffer{N}(schema, length(parsing_ctx.eols))
    while !done
        # Updates eols_buf with new newlines, byte buffer was updated either from initialization stage or at the end of the loop
        task = parsing_ctx.eols[]
        _parse_rows_forloop!(result_buf, task, parsing_ctx.bytes, schema, options)
        consume!(result_buf, parsing_ctx, row_num, consume_ctx)
        done && break
        row_num += UInt32(length(task) - 1)
        (last_newline_at, quoted, done) = read_and_lex!(io, parsing_ctx, options, byteset, last_newline_at, quoted)
        limit_eols!(parsing_ctx, row_num) && break
    end # while !done
    return nothing
end