function _parse_file_serial(io, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext, options::Parsers.Options, last_chunk_newline_at::UInt32, quoted::Bool, done::Bool, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    row_num = UInt32(1)
    schema = parsing_ctx.schema
    limit_eols!(parsing_ctx, row_num)
    result_buf = TaskResultBuffer{N}(schema, length(parsing_ctx.eols))
    while true
        # Updates eols_buf with new newlines, byte buffer was updated either from initialization stage or at the end of the loop
        task = parsing_ctx.eols[]
        _parse_rows_forloop!(result_buf, task, parsing_ctx.bytes, schema, options)
        consume!(result_buf, parsing_ctx, row_num, consume_ctx)
        done && break
        row_num += UInt32(length(task) - 1)
        empty!(parsing_ctx.eols)
        # We always end on a newline when processing a chunk, so we're inserting a dummy variable to
        # signal that. This works out even for the very first chunk.
        push!(parsing_ctx.eols, UInt32(0))
        bytes_read_in = prepare_buffer!(io, parsing_ctx.bytes, last_chunk_newline_at)
        (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, parsing_ctx, options, byteset, bytes_read_in, quoted)
        limit_eols!(parsing_ctx, row_num) && break
    end # while !done
    return nothing
end