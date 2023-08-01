struct HeaderParsingError <: Exception
    msg::String
end
Base.showerror(io::IO, e::HeaderParsingError) = print(io, e.msg)

function initial_read_and_lex_and_skip!(io, chunking_ctx, settings, escapechar, openquotechar, closequotechar, ignoreemptyrows)
    # First ingestion of raw bytes from io
    bytes_read_in = ChunkedBase.initial_read!(io, chunking_ctx)

    # We need to detect the newline first to construct the Lexer
    newline = settings.newlinechar
    if isnothing(newline)
        newline = ChunkedBase._detect_newline(chunking_ctx.bytes, 1, bytes_read_in)
    end

    lexer = settings.no_quoted_newlines ?
        Lexer(io, nothing, newline) :
        Lexer(io, escapechar, openquotechar, closequotechar, newline)

    # Find newlines
    ChunkedBase.initial_lex!(lexer, chunking_ctx, bytes_read_in)

    # First skip over commented lines, then jump to header / data row
    should_parse_header = settings.header_at > 0
    pre_header_skiprows = max(0, (should_parse_header ? settings.header_at : settings.data_at) - 1)
    lines_skipped_total = ChunkedBase.skip_rows_init!(lexer, chunking_ctx, pre_header_skiprows, ignoreemptyrows)

    return lexer, lines_skipped_total
end

function process_header_and_schema_and_finish_row_skip!(
    parsing_ctx::ParsingContext,
    chunking_ctx::ChunkingContext,
    lexer::Lexer,
    schema_input,
    settings::ParserSettings,
    lines_skipped_total::Int
)
    input_is_empty = length(chunking_ctx.newline_positions) == 1
    options = parsing_ctx.options
    ignorerepeated = options.ignorerepeated

    header_provided = !isnothing(settings.header)
    schema_provided = schema_input isa Vector{DataType}
    should_parse_header = settings.header_at > 0
    schema = parsing_ctx.schema
    schema_provided && validate_schema(schema_input)

    @inbounds if schema_provided & header_provided
        append!(parsing_ctx.header, settings.header)
        append!(schema, schema_input)
    elseif !schema_provided & header_provided
        append!(parsing_ctx.header, settings.header)
        _fill_schema!(schema, parsing_ctx.header, schema_input, settings)
    elseif schema_provided & !header_provided
        append!(schema, schema_input)
        if !should_parse_header || input_is_empty
            for i in 1:length(schema_input)
                push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
            end
        else # should_parse_header
            s = chunking_ctx.newline_positions[1]
            e = chunking_ctx.newline_positions[2]
            v = @view chunking_ctx.bytes[s+1:e-1]
            pos = 1
            ignorerepeated && (pos = Parsers.checkdelim!(v, pos, length(v), options))
            code = Parsers.OK
            for i in 1:length(schema_input)
                Parsers.eof(code) && throw(HeaderParsingError("Not enough columns in header, found $(i-1), provided schema implied $(length(schema_input)) at $(lines_skipped_total+1):$(pos) (row:pos)."))
                res = Parsers.xparse(String, v, pos, length(v), options, Parsers.PosLen31)
                (val, tlen, code) = res.val, res.tlen, res.code
                if Parsers.sentinel(code)
                    push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
                elseif !Parsers.ok(code)
                    throw(HeaderParsingError("Error parsing header for column $i at $(lines_skipped_total+1):$(pos) (row:pos)."))
                else
                    push!(parsing_ctx.header, Symbol(strip(Parsers.getstring(v, val, options.e))))
                end
                pos += tlen
            end
            if !(Parsers.eof(code) || Parsers.newline(code))
                # There are too many columns; calculate how many extra so we can inform the user.
                ncols = length(parsing_ctx.header)
                while !Parsers.eof(code)
                    res = Parsers.xparse(String, v, pos, length(v), options, Parsers.PosLen31)
                    (tlen, code) = res.tlen, res.code
                    pos += tlen
                    ncols += 1
                end
                throw(HeaderParsingError("Error parsing header, there are more columns ($ncols) than provided types in schema ($(length(schema_input))) at $(lines_skipped_total+1):$(pos) (row:pos)."))
            end
        end
    elseif !should_parse_header
        input_is_empty && return nothing
        # infer the number of columns from the first data row
        s = chunking_ctx.newline_positions[1]
        e = chunking_ctx.newline_positions[2]
        v = @view chunking_ctx.bytes[s+1:e-1]
        pos = 1
        ignorerepeated && (pos = Parsers.checkdelim!(v, pos, length(v), options))
        code = Parsers.OK
        i = 1
        while !(Parsers.eof(code) || Parsers.newline(code))
            res = Parsers.xparse(String, v, pos, length(v), options, Parsers.PosLen31)
            (tlen, code) = res.tlen, res.code
            !(Parsers.ok(code) || Parsers.sentinel(code)) && (throw(HeaderParsingError("Error parsing header for column $i at $(lines_skipped_total+1):$(pos) (row:pos).")))
            pos += tlen
            push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
            i += 1
        end
        _fill_schema!(schema, parsing_ctx.header, schema_input, settings)
    else
        input_is_empty && return nothing
        # infer the number of columns from the header row
        s = chunking_ctx.newline_positions[1]
        e = chunking_ctx.newline_positions[2]
        v = view(chunking_ctx.bytes, s+1:e-1)
        pos = 1
        ignorerepeated && (pos = Parsers.checkdelim!(v, pos, length(v), options))
        code = Parsers.OK
        i = 1
        while !((Parsers.eof(code) && !Parsers.delimited(code)) || Parsers.newline(code))
            res = Parsers.xparse(String, v, pos, length(v), options, Parsers.PosLen31)
            (val, tlen, code) = res.val, res.tlen, res.code
            if Parsers.sentinel(code)
                push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
            elseif !Parsers.ok(code)
                throw(HeaderParsingError("Error parsing header for column $i at $(lines_skipped_total+1):$(pos) (row:pos)."))
            else
                push!(parsing_ctx.header, Symbol(strip(Parsers.getstring(v, val, options.e))))
            end
            pos += tlen
            i += 1
        end
        _fill_schema!(schema, parsing_ctx.header, schema_input, settings)
    end
    !schema_provided && validate_schema(schema)

    should_parse_header && !input_is_empty && shiftleft!(chunking_ctx.newline_positions, 1) # remove the header row from eols
    # Refill the buffer if it contained a single line and we consumed it to get the header
    if should_parse_header && length(chunking_ctx.newline_positions) == 1 && !eof(lexer.io)
       ChunkedBase.read_and_lex!(lexer, chunking_ctx)
    end

    # Skip over commented lines, then jump to data row if needed
    post_header_skiprows = should_parse_header ? settings.data_at - settings.header_at - 1 : 0
    ChunkedBase.skip_rows_init!(lexer, chunking_ctx, post_header_skiprows)

    # This is where we create the enum'd counterpart of parsing_ctx.schema, parsing_ctx.enum_schema
    append!(parsing_ctx.enum_schema, map(Enums.to_enum, parsing_ctx.schema))
    for i in length(parsing_ctx.schema):-1:1 # remove Nothing types from schema (but keep SKIP in enum_schema)
        type = schema[i]
        if type === Nothing
            deleteat!(parsing_ctx.schema, i)
            deleteat!(parsing_ctx.header, i)
        else
            schema[i] = _translate_to_buffer_type(type)
        end
    end
    return nothing
end
