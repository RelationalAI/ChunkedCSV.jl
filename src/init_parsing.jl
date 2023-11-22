struct HeaderParsingError <: Exception
    msg::String
end
Base.showerror(io::IO, e::HeaderParsingError) = print(io, "HeaderParsingError: ", e.msg)

# We might need to skip some rows to get to the header and then skip again to get to the data
# This function does the *first* skip and initializes the ChunkingContext and Lexer
# If the newline is not provided, we'll need to detect it first
function initial_read_and_lex_and_skip!(io, chunking_ctx, input_args, escapechar, openquotechar, closequotechar, ignoreemptyrows)
    # First ingestion of raw bytes from io
    bytes_read_in = ChunkedBase.initial_read!(io, chunking_ctx)

    # We need to detect the newline first to construct the Lexer
    newline = input_args.newlinechar
    if isnothing(newline)
        newline = ChunkedBase._detect_newline(chunking_ctx.bytes, 1, bytes_read_in)
    end

    lexer = input_args.no_quoted_newlines ?
        Lexer(io, nothing, newline) :
        Lexer(io, escapechar, openquotechar, closequotechar, newline)

    # Find newlines
    ChunkedBase.initial_lex!(lexer, chunking_ctx, bytes_read_in)

    # First skip over commented lines, then jump to header / data row
    should_parse_header = input_args.header_at > 0
    pre_header_skiprows = max(0, (should_parse_header ? input_args.header_at : input_args.data_at) - 1)
    lines_skipped_total = ChunkedBase.skip_rows_init!(lexer, chunking_ctx, pre_header_skiprows, ignoreemptyrows)

    return lexer, lines_skipped_total
end

# This function does the *second* skip and initializes the parsing context
# (see the function above for the first skip).
# We cannot construct the parsing context without knowing the schema and the header,
# we'll take inputs the user provided for schema and header and we'll reconcile it with the
# header row or at least with the number of columns in the file. We don't do type inference yet,
# so any unknown types will be filled with String if the user didn't give us a vector of types already.
function process_header_and_schema_and_finish_row_skip!(
    parsing_ctx::ParsingContext,
    chunking_ctx::ChunkingContext,
    lexer::Lexer,
    schema_input,
    input_args::InputArguments,
    lines_skipped_total::Int
)
    input_is_empty = length(chunking_ctx.newline_positions) == 1
    options = parsing_ctx.options
    ignorerepeated = options.ignorerepeated

    header_provided = !isnothing(input_args.header)
    schema_provided = schema_input isa Vector{DataType}
    should_parse_header = input_args.header_at > 0
    schema = parsing_ctx.schema
    schema_provided && validate_schema(schema_input)
    newlines = chunking_ctx.newline_positions

    if schema_provided & header_provided
        # The user actually provided a schema and a header, so we just give to the parsing context
        append!(parsing_ctx.header, input_args.header)
        append!(schema, schema_input)
    elseif !schema_provided & header_provided
        # The user provided a header but no schema, and since we don't infer types, we just fill
        # the schema with String
        append!(parsing_ctx.header, input_args.header)
        _fill_schema!(schema, parsing_ctx.header, schema_input, input_args)
    elseif schema_provided & !header_provided
        # The user provided a schema but no header, so we need to infer the header from the first row
        append!(schema, schema_input)
        if !should_parse_header || input_is_empty
            # The file doesn't have a header, so we just fill the header with default names
            for i in 1:length(schema_input)
                push!(parsing_ctx.header, Symbol(string(input_args.default_colname_prefix, i)))
            end
        else
            # The file has a header, so we parse it and fill the header with the parsed names
            header_row_start = newlines[1]
            header_row_end   = newlines[2]
            row_bytes = @view chunking_ctx.bytes[header_row_start+1:header_row_end-1]

            len = length(row_bytes)
            pos = 1
            # `ignorerepeated` is used to implement fixed width column parsing. We need to skip over the initial
            # delimiters to avoid getting an empty first value.
            ignorerepeated && (pos = Parsers.checkdelim!(row_bytes, pos, len, options))
            code = Parsers.OK
            ncols = length(schema_input)
            for i in 1:ncols
                Parsers.eof(code) && throw(HeaderParsingError("Not enough columns in header, found $(i-1), provided schema implied $ncols at $(lines_skipped_total+1):$pos (row:pos)."))
                res = Parsers.xparse(String, row_bytes, pos, len, options, Parsers.PosLen31)

                (val, tlen, code) = res.val, res.tlen, res.code

                if Parsers.sentinel(code)
                    push!(parsing_ctx.header, Symbol(string(input_args.default_colname_prefix, i)))
                elseif !Parsers.ok(code)
                    throw(HeaderParsingError("Error parsing header for column $i at $(lines_skipped_total+1):$(pos) (row:pos)."))
                else
                    identifier_s = strip(Parsers.getstring(row_bytes, val, options.e))
                    try
                        push!(parsing_ctx.header, Symbol(identifier_s))
                    catch
                        # defensively truncate identifier_s to 2k characters in case something is very cursed
                        throw(HeaderParsingError("Error parsing header for column $i ('$(first(identifier_s, 2000))') at " *
                            "$(lines_skipped_total+1):$pos (row:pos): presence of invalid non text bytes in the CSV snippet"))
                    end
                end
                pos += tlen
            end
            if !(Parsers.eof(code) || Parsers.newline(code))
                # There are too many columns; calculate how many extra so we can inform the user.
                ncols_actual = length(parsing_ctx.header)
                while !Parsers.eof(code)
                    res = Parsers.xparse(String, row_bytes, pos, len, options, Parsers.PosLen31)
                    (tlen, code) = res.tlen, res.code
                    pos += tlen
                    ncols_actual += 1
                end
                throw(HeaderParsingError("Error parsing header, there are more columns ($ncols_actual) than provided types in schema ($ncols) at $(lines_skipped_total+1):$(pos) (row:pos)."))
            end
        end
    elseif !should_parse_header
        # The user didn't provide a schema nor the header and the file *doesn't* have a header,
        # so we need to infer the number of columns from the first row and fill the schema with String.
        input_is_empty && return nothing
        # infer the number of columns from the first data row
        header_row_start = newlines[1]
        header_row_end   = newlines[2]
        row_bytes = @view chunking_ctx.bytes[header_row_start+1:header_row_end-1]

        len = length(row_bytes)
        pos = 1
        # `ignorerepeated` is used to implement fixed width column parsing. We need to skip over the initial
        # delimiters to avoid getting an empty first value.
        ignorerepeated && (pos = Parsers.checkdelim!(row_bytes, pos, len, options))
        code = Parsers.OK
        i = 1
        while !(Parsers.eof(code) || Parsers.newline(code))
            res = Parsers.xparse(String, row_bytes, pos, len, options, Parsers.PosLen31)
            (tlen, code) = res.tlen, res.code

            !(Parsers.ok(code) || Parsers.sentinel(code)) && (throw(HeaderParsingError("Error parsing header for column $i at $(lines_skipped_total+1):$(pos) (row:pos).")))

            push!(parsing_ctx.header, Symbol(string(input_args.default_colname_prefix, i)))
            pos += tlen
            i += 1
        end
        _fill_schema!(schema, parsing_ctx.header, schema_input, input_args)
    else
        # The user didn't provide a schema nor the header and the file *does* have a header,
        # so we need to infer the header from the first row and fill the schema with String.
        input_is_empty && return nothing
        # infer the number of columns from the header row
        header_row_start = newlines[1]
        header_row_end   = newlines[2]
        row_bytes = @view chunking_ctx.bytes[header_row_start+1:header_row_end-1]

        len = length(row_bytes)
        pos = 1
        # `ignorerepeated` is used to implement fixed width column parsing. We need to skip over the initial
        # delimiters to avoid getting an empty first value.
        ignorerepeated && (pos = Parsers.checkdelim!(row_bytes, pos, len, options))
        code = Parsers.OK
        i = 1
        while !((Parsers.eof(code) && !Parsers.delimited(code)) || Parsers.newline(code))
            res = Parsers.xparse(String, row_bytes, pos, len, options, Parsers.PosLen31)

            (val, tlen, code) = res.val, res.tlen, res.code

            if Parsers.sentinel(code)
                push!(parsing_ctx.header, Symbol(string(input_args.default_colname_prefix, i)))
            elseif !Parsers.ok(code)
                throw(HeaderParsingError("Error parsing header for column $i at $(lines_skipped_total+1):$pos (row:pos)."))
            else
                identifier_s = strip(Parsers.getstring(row_bytes, val, options.e))
                try
                    push!(parsing_ctx.header, Symbol(identifier_s))
                catch
                    # defensively truncate identifier_s to 2k characters in case something is very cursed
                    throw(HeaderParsingError("Error parsing header for column $i ('$(first(identifier_s, 2000))') at " *
                        "$(lines_skipped_total+1):$pos (row:pos): presence of invalid non text bytes in the CSV snippet"))
                end
            end
            pos += tlen
            i += 1
        end
        _fill_schema!(schema, parsing_ctx.header, schema_input, input_args)
    end
    !schema_provided && validate_schema(schema)

    # remove the header row from newlines so we don't have to worry about it in the
    # main parsing loop
    should_parse_header && !input_is_empty && shiftleft!(newlines, 1)
    # Refill the buffer if it contained a single line and we consumed it to get the header
    if should_parse_header && length(newlines) == 1 && !eof(lexer.io)
       ChunkedBase.read_and_lex!(lexer, chunking_ctx)
    end

    # Skip over commented lines, then jump to data row if needed
    post_header_skiprows = should_parse_header ? input_args.data_at - input_args.header_at - 1 : 0
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
