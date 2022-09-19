
function hasBOM(bytes::Vector{UInt8})
    return @inbounds bytes[1] == 0xef && bytes[2] == 0xbb && bytes[3] == 0xbf
end

function init_parsing!(io::IO, settings::ParserSettings, options::Parsers.Options, byteset::Val{B}) where {B}
    header_provided = !isnothing(settings.header)
    schema_provided = !isnothing(settings.schema)
    should_parse_header = settings.hasheader
    schema = DataType[]
    parsing_ctx = ParsingContext(
        schema,
        copy(Symbol[]),
        Vector{UInt8}(undef, settings.buffersize),
        BufferedVector{UInt32}(),
        settings.limit,
        settings.nworkers,
        settings.maxtasks,
        TaskCondition(0, Threads.Condition(ReentrantLock()))
    )
    # We always end on a newline when processing a chunk, so we're inserting a dummy variable to
    # signal that. This works out even for the very first chunk.
    push!(parsing_ctx.eols, UInt32(0))

    bytes_read_in = prepare_buffer!(io, parsing_ctx.bytes, UInt32(0)) # fill the buffer for the first time
    if bytes_read_in > 2 && hasBOM(parsing_ctx.bytes)
        bytes_read_in -= prepare_buffer!(io, parsing_ctx.bytes, UInt32(3)) - UInt32(3)
    end

    # lex the entire buffer for newlines
    (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, parsing_ctx, options, byteset, bytes_read_in, false)

    skiprows = Int(settings.skiprows)
    while !done && skiprows >= length(parsing_ctx.eols) - 1
        skiprows -= length(parsing_ctx.eols) - 1
        empty!(parsing_ctx.eols)
        push!(parsing_ctx.eols, UInt32(0))
        bytes_read_in = prepare_buffer!(io, parsing_ctx.bytes, last_chunk_newline_at)
        (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, parsing_ctx, options, byteset, bytes_read_in, quoted)
        # TODO: Special path for the case where we skipped the entire file
        # done && (return (parsing_ctx, last_chunk_newline_at, quoted, done))
    end
    shiftleft!(parsing_ctx.eols, skiprows)

    @inbounds if schema_provided & header_provided
        append!(parsing_ctx.header, settings.header)
        append!(schema, settings.schema)
    elseif !schema_provided & header_provided
        append!(parsing_ctx.header, settings.header)
        resize!(schema, length(parsing_ctx.header))
        fill!(schema, String)
    elseif schema_provided & !header_provided
        append!(schema, settings.schema)
        if !should_parse_header
            for i in 1:length(settings.schema)
                push!(parsing_ctx.header, Symbol(string("COL_", i)))
            end
        else # should_parse_header
            s = parsing_ctx.eols[1]
            e = parsing_ctx.eols[2]
            v = @view parsing_ctx.bytes[s+1:e]
            pos = 1
            code = Parsers.OK
            for i in 1:length(settings.schema)
                (;val, tlen, code) = Parsers.xparse(String, v, pos, length(v), options)
                !Parsers.ok(code) && (close(io); error("Error parsing header for column $i at $(skiprows+1):$(pos)."))
                push!(parsing_ctx.header, Symbol(strip(String(v[val.pos:val.pos+val.len-1]))))
                pos += tlen
            end
            !(Parsers.eof(code) || Parsers.newline(code)) && (close(io); error("Error parsing header, there are more columns that provided types in schema"))
        end
    elseif !should_parse_header
        # infer the number of columns from the first data row
        s = parsing_ctx.eols[1]
        e = parsing_ctx.eols[2]
        v = @view parsing_ctx.bytes[s+1:e]
        pos = 1
        code = Parsers.OK
        i = 1
        while !(Parsers.eof(code) || Parsers.newline(code))
            (;val, tlen, code) = Parsers.xparse(String, v, pos, length(v), options)
            !Parsers.ok(code) && (close(io); error("Error parsing header for column $i at $(skiprows+1):$(pos)."))
            pos += tlen
            push!(parsing_ctx.header, Symbol(string("COL_", i)))
            i += 1
        end
        resize!(schema, length(parsing_ctx.header))
        fill!(schema, String)
    else
        #infer the number of columns from the header row
        s = parsing_ctx.eols[1]
        e = parsing_ctx.eols[2]
        v = view(parsing_ctx.bytes, s+1:e)
        pos = 1
        code = Parsers.OK
        while !(Parsers.eof(code) || Parsers.newline(code))
            (;val, tlen, code) = Parsers.xparse(String, v, pos, length(v), options)
            !Parsers.ok(code) && (close(io); error("Error parsing header for column $i at $(skiprows+1):$(pos)."))
            @inbounds push!(parsing_ctx.header, Symbol(strip(String(v[val.pos:val.pos+val.len-1]))))
            pos += tlen
        end

        resize!(schema, length(parsing_ctx.header))
        fill!(schema, String)
    end

    should_parse_header && shiftleft!(parsing_ctx.eols, 1)
    # Refill the buffer if if contained a single line and we consumed it to get the header
    if should_parse_header && length(parsing_ctx.eols) == 1
        empty!(parsing_ctx.eols)
        unsafe_push!(parsing_ctx.eols, UInt32(0))
        bytes_read_in = prepare_buffer!(io, parsing_ctx.bytes, last_chunk_newline_at)
        (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, parsing_ctx, options, byteset, bytes_read_in, quoted)
    end

    return (parsing_ctx, last_chunk_newline_at, quoted, done)
end
