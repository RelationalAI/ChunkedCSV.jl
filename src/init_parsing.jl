
function hasBOM(bytes::Vector{UInt8})
    return @inbounds bytes[1] == 0xef && bytes[2] == 0xbb && bytes[3] == 0xbf
end

function apply_types_from_mapping!(schema, header, settings, header_provided)
    mapping = settings.schema::Dict{Symbol,DataType}
    if !(!settings.validate_type_map || header_provided || issubset(keys(mapping), header))
        throw(ArgumentError("Unknown columns from schema mapping: $(setdiff(keys(mapping), header)), parsed header: $(header), row $(settings.header_at)"))
    end
    @inbounds for (i, (colname, default_type)) in enumerate(zip(header, schema))
        schema[i] = get(mapping, colname, default_type)
    end
end

function _startswith(s::AbstractVector{UInt8}, soff::Integer, prefix::AbstractVector{UInt8})
    length(s) - soff < length(prefix) && return false
    @inbounds for i in eachindex(prefix)
        s[i + soff] == prefix[i] || return false
    end
    return true
end
_startswith(s::AbstractVector{UInt8}, prefix::AbstractVector{UInt8}) = _startswith(s, 0, prefix)
_startswith(s, soff, prefix::Nothing) = false
_startswith(s, prefix::Nothing) = false

function skip_rows_init!(lexer, parsing_ctx, rows_to_skip, comment)
    input_is_empty = last(parsing_ctx.eols) == Int32(0)
    lines_skipped_total = 0
    input_is_empty && return lines_skipped_total
    # To know where in the end-of-line buffer we are while deciding whether we can skip or
    # if we need to refill the buffer because we skipped everything in it.
    eol_index = 1
    @inbounds while true
        if eol_index == length(parsing_ctx.eols)
            if lexer.done
                break
            else
                read_and_lex!(lexer, parsing_ctx)
                eol_index = 1
            end
        end
        if !_startswith(parsing_ctx.bytes, parsing_ctx.eols[eol_index], comment)
            if rows_to_skip > 0
                rows_to_skip -= 1
            else
                break
            end
        else rows_to_skip > 0
            rows_to_skip -= 1
        end
        eol_index += 1
        lines_skipped_total += 1
    end
    shiftleft!(parsing_ctx.eols, eol_index-1)
    return lines_skipped_total
end

function skip_rows_init!(lexer, parsing_ctx, rows_to_skip, comment::Nothing)
    while !lexer.done && rows_to_skip >= length(parsing_ctx.eols) - 1
        rows_to_skip -= length(parsing_ctx.eols) - 1
        read_and_lex!(lexer, parsing_ctx)
    end
    rows_to_skip = max(rows_to_skip, 0)
    shiftleft!(parsing_ctx.eols, rows_to_skip)
    return rows_to_skip
end

function init_state_and_pre_header_skip(io, settings, escapechar, openquotechar, closequotechar)
    should_parse_header = settings.header_at > 0
    parsing_ctx = ParsingContext(
        1,
        DataType[],
        Enums.CSV_TYPE[],
        Symbol[],
        Vector{UInt8}(undef, settings.buffersize),
        BufferedVector(Int32[0]),
        settings.limit,
        settings.nworkers,
        escapechar,
        TaskCounter(),
        settings.comment,
    )

    # read and lex the entire buffer for the first time
    bytes_read_in = prepare_buffer!(io, parsing_ctx.bytes, 0)

    newline = settings.newlinechar
    if isnothing(newline)
        newline = _detect_newline(parsing_ctx.bytes, 1, bytes_read_in)
    end

    lexer = settings.no_quoted_newlines ?
        Lexer(io, nothing, newline) :
        Lexer(io, escapechar, openquotechar, closequotechar, newline)

    if bytes_read_in > 0
        # TRACING #  push!(LEXER_TASK_TIMES, time_ns())
        find_newlines!(lexer, parsing_ctx.bytes, parsing_ctx.eols, 1, bytes_read_in)
        handle_file_end!(lexer, parsing_ctx.eols, bytes_read_in)
        check_any_valid_rows(lexer, parsing_ctx)
        # TRACING #  push!(LEXER_TASK_TIMES, time_ns())
    end

    # First skip over commented lines, then jump to header / data row
    pre_header_skiprows = (should_parse_header ? settings.header_at : settings.data_at) - 1
    lines_skipped_total = skip_rows_init!(lexer, parsing_ctx, pre_header_skiprows, parsing_ctx.comment)

    return parsing_ctx, lexer, lines_skipped_total
end

function process_header_and_schema_and_finish_row_skip!(parsing_ctx::ParsingContext, lexer::Lexer, settings::ParserSettings, options::Parsers.Options, lines_skipped_total::Int)
    input_is_empty = last(parsing_ctx.eols) == Int32(0)

    header_provided = !isnothing(settings.header)
    schema_is_dict = isa(settings.schema, Dict)
    schema_provided = !isnothing(settings.schema) && !schema_is_dict
    should_parse_header = settings.header_at > 0
    schema = parsing_ctx.schema
    schema_provided && validate_schema(settings.schema)

    @inbounds if schema_provided & header_provided
        append!(parsing_ctx.header, settings.header)
        append!(schema, settings.schema)
    elseif !schema_provided & header_provided
        append!(parsing_ctx.header, settings.header)
        resize!(schema, length(parsing_ctx.header))
        fill!(schema, String)
        schema_is_dict && apply_types_from_mapping!(schema, parsing_ctx.header, settings, header_provided)
    elseif schema_provided & !header_provided
        append!(schema, settings.schema)
        if !should_parse_header || input_is_empty
            for i in 1:length(settings.schema)
                push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
            end
        else # should_parse_header
            s = parsing_ctx.eols[1]
            e = parsing_ctx.eols[2]
            v = @view parsing_ctx.bytes[s+1:e-1]
            pos = 1
            code = Parsers.OK
            for i in 1:length(settings.schema)
                res = Parsers.xparse(String, v, pos, length(v), options, Parsers.PosLen31)
                (val, tlen, code) = res.val, res.tlen, res.code
                if Parsers.sentinel(code)
                    push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
                elseif !Parsers.ok(code)
                    close(lexer.io);
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
                close(lexer.io)
                throw(HeaderParsingError("Error parsing header, there are more columns ($ncols) than provided types in schema ($(length(settings.schema))) at $(lines_skipped_total+1):$(pos) (row:pos)."))
            end
        end
    elseif !should_parse_header
        input_is_empty && return (parsing_ctx, lexer)
        # infer the number of columns from the first data row
        s = parsing_ctx.eols[1]
        e = parsing_ctx.eols[2]
        v = @view parsing_ctx.bytes[s+1:e-1]
        pos = 1
        code = Parsers.OK
        i = 1
        while !(Parsers.eof(code) || Parsers.newline(code))
            res = Parsers.xparse(String, v, pos, length(v), options, Parsers.PosLen31)
            (val, tlen, code) = res.val, res.tlen, res.code
            !Parsers.ok(code) && (close(lexer.io); throw(HeaderParsingError("Error parsing header for column $i at $(lines_skipped_total+1):$(pos) (row:pos).")))
            pos += tlen
            push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
            i += 1
        end
        resize!(schema, length(parsing_ctx.header))
        fill!(schema, String)
        schema_is_dict && apply_types_from_mapping!(schema, parsing_ctx.header, settings, header_provided)
    else
        input_is_empty && return (parsing_ctx, lexer)
        # infer the number of columns from the header row
        s = parsing_ctx.eols[1]
        e = parsing_ctx.eols[2]
        v = view(parsing_ctx.bytes, s+1:e-1)
        pos = 1
        code = Parsers.OK
        i = 1
        while !((Parsers.eof(code) && !Parsers.delimited(code)) || Parsers.newline(code))
            res = Parsers.xparse(String, v, pos, length(v), options, Parsers.PosLen31)
            (val, tlen, code) = res.val, res.tlen, res.code
            if Parsers.sentinel(code)
                push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
            elseif !Parsers.ok(code)
                close(lexer.io)
                throw(HeaderParsingError("Error parsing header for column $i at $(lines_skipped_total+1):$(pos) (row:pos)."))
            else
                @inbounds push!(parsing_ctx.header, Symbol(strip(Parsers.getstring(v, val, options.e))))
            end
            pos += tlen
            i += 1
        end

        resize!(schema, length(parsing_ctx.header))
        fill!(schema, String)
        schema_is_dict && apply_types_from_mapping!(schema, parsing_ctx.header, settings, header_provided)
    end
    !schema_provided && validate_schema(schema)

    should_parse_header && !input_is_empty && shiftleft!(parsing_ctx.eols, 1) # remove the header row from eols
    # Refill the buffer if it contained a single line and we consumed it to get the header
    if should_parse_header && length(parsing_ctx.eols) == 1 && !eof(lexer.io)
       read_and_lex!(lexer, parsing_ctx)
    end

    # Skip over commented lines, then jump to data row if needed
    post_header_skiprows = should_parse_header ? settings.data_at - settings.header_at - 1 : 0
    skip_rows_init!(lexer, parsing_ctx, post_header_skiprows, parsing_ctx.comment)

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
