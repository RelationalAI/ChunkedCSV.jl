
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
        settings.nresults,
        TaskCondition(),
    )
    # read and lex the entire buffer for the first time
    (last_newline_at, quoted, done) = read_and_lex!(io, parsing_ctx, options, byteset, UInt32(0), false)

    skiprows = Int(settings.skiprows)
    while !done && skiprows >= length(parsing_ctx.eols) - 1
        skiprows -= length(parsing_ctx.eols) - 1
        (last_newline_at, quoted, done) = read_and_lex!(io, parsing_ctx, options, byteset, last_newline_at, quoted)
        # TODO: Special path for the case where we skipped the entire file
        # done && (return (parsing_ctx, last_newline_at, quoted, done))
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
            v = @view parsing_ctx.bytes[s+1:e-1]
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
        v = @view parsing_ctx.bytes[s+1:e-1]
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
        v = view(parsing_ctx.bytes, s+1:e-1)
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
        (last_newline_at, quoted, done) = read_and_lex!(io, parsing_ctx, options, byteset, last_newline_at, quoted)
    end

    return (parsing_ctx, last_newline_at, quoted, done)
end
