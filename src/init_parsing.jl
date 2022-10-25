
function hasBOM(bytes::Vector{UInt8})
    return @inbounds bytes[1] == 0xef && bytes[2] == 0xbb && bytes[3] == 0xbf
end

function apply_types_from_mapping!(schema, header, settings, header_provided)
    mapping = settings.schema::Dict{Symbol,DataType}
    @assert !settings.validate_type_map || header_provided || issubset(keys(mapping), header) "Unknown columns from schema mapping: $(setdiff(keys(mapping), header)), parsed header: $(header), row $(Int(settings.skiprows)+1)"
    @inbounds for (i, (colname, default_type)) in enumerate(zip(header, schema))
        schema[i] = get(mapping, colname, default_type)
    end
end

function init_parsing!(io::IO, settings::ParserSettings, options::Parsers.Options, byteset::Val{B}) where {B}
    header_provided = !isnothing(settings.header)
    schema_is_dict = isa(settings.schema, Dict)
    schema_provided = !isnothing(settings.schema) && !schema_is_dict
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
        options.e,
        TaskCondition(),
    )
    lexer_state = LexerState{byteset}(io)
    # read and lex the entire buffer for the first time
    read_and_lex!(lexer_state, parsing_ctx, options)
    input_is_empty = lexer_state.last_newline_at == UInt(0)

    skiprows = Int(settings.skiprows)
    while !lexer_state.done && skiprows >= length(parsing_ctx.eols) - 1
        skiprows -= length(parsing_ctx.eols) - 1
        read_and_lex!(lexer_state, parsing_ctx, options)
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
                (;val, tlen, code) = Parsers.xparse(String, v, pos, length(v), options)
                if Parsers.sentinel(code)
                    push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
                elseif !Parsers.ok(code)
                    close(io); 
                    error("Error parsing header for column $i at $(Int(settings.skiprows)+1):$(pos) (row:col).")
                else
                    push!(parsing_ctx.header, Symbol(strip(String(v[val.pos:val.pos+val.len-1]))))
                end
                pos += tlen
            end
            !(Parsers.eof(code) || Parsers.newline(code)) && (close(io); error("Error parsing header, there are more columns that provided types in schema"))
        end
    elseif !should_parse_header
        input_is_empty && return (parsing_ctx, lexer_state)
        # infer the number of columns from the first data row
        s = parsing_ctx.eols[1]
        e = parsing_ctx.eols[2]
        v = @view parsing_ctx.bytes[s+1:e-1]
        pos = 1
        code = Parsers.OK
        i = 1
        while !(Parsers.eof(code) || Parsers.newline(code))
            (;val, tlen, code) = Parsers.xparse(String, v, pos, length(v), options)
            !Parsers.ok(code) && (close(io); error("Error parsing header for column $i at $(Int(settings.skiprows)+1):$(pos) (row:col)."))
            pos += tlen
            push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
            i += 1
        end
        resize!(schema, length(parsing_ctx.header))
        fill!(schema, String)
        schema_is_dict && apply_types_from_mapping!(schema, parsing_ctx.header, settings, header_provided)
    else
        input_is_empty && return (parsing_ctx, lexer_state)
        #infer the number of columns from the header row
        s = parsing_ctx.eols[1]
        e = parsing_ctx.eols[2]
        v = view(parsing_ctx.bytes, s+1:e-1)
        pos = 1
        code = Parsers.OK
        i = 1
        while !((Parsers.eof(code) && !Parsers.delimited(code)) || Parsers.newline(code))
            (;val, tlen, code) = Parsers.xparse(String, v, pos, length(v), options)
            if Parsers.sentinel(code)
                push!(parsing_ctx.header, Symbol(string(settings.default_colname_prefix, i)))
            elseif !Parsers.ok(code)
                close(io) 
                error("Error parsing header for column $i at $(Int(settings.skiprows)+1):$(pos) (row:col).")
            else
                @inbounds push!(parsing_ctx.header, Symbol(strip(String(v[val.pos:val.pos+val.len-1]))))
            end
            pos += tlen
            i += 1
        end

        resize!(schema, length(parsing_ctx.header))
        fill!(schema, String)
        schema_is_dict && apply_types_from_mapping!(schema, parsing_ctx.header, settings, header_provided)
    end

    should_parse_header && !input_is_empty && shiftleft!(parsing_ctx.eols, 1)
    # Refill the buffer if if contained a single line and we consumed it to get the header
    if should_parse_header && !input_is_empty && !eof(io)
       read_and_lex!(lexer_state, parsing_ctx, options)
    end

    return (parsing_ctx, lexer_state)
end
