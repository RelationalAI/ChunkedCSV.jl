skip_row!(result_buf::TaskResultBuffer{M}, row_bytes, comment::Nothing) where {M} = false
function skip_row!(result_buf::TaskResultBuffer{M}, row_bytes, comment::Vector{UInt8}) where {M}
    if _startswith(row_bytes, comment)
        foreach(skip_element!, result_buf.cols)
        unsafe_push!(result_buf.row_statuses, RowStatus.HasColumnIndicators | RowStatus.SkippedRow)
        push!(result_buf.column_indicators::BufferedVector{M}, initflagset(M))
        return true
    end
    return false
end

_type_proxy(::Type{T}) where {T} = T
_type_proxy(::Type{FixedDecimal{T,F}}) where {T,F} = _FixedDecimal{T,F}

function _isemptyrow(prev_nl, next_nl, bytes)
    return prev_nl + 1 == next_nl || (prev_nl + 2 == next_nl && @inbounds(bytes[prev_nl+1]) == UInt8('\r'))
end

@inline function parsecustom!(::Type{customtypes}, row_bytes, pos, len, col_idx, cols, options, _type, row_status, column_indicators) where {customtypes}
    if @generated
        block = Expr(:block)
        push!(block.args, quote
            # TODO: Currently, we shouldn't ever hit this path as we either throw an error for unsupported types
            # or Parsers throw an error internally for weird custom Integer subtypes that don't have a parse method.
            row_status |= RowStatus.UnknownTypeError
            row_status |= RowStatus.HasColumnIndicators
            column_indicators = setflag(column_indicators, col_idx)
            skip_element!(cols[col_idx])
            res = Parsers.xparse(String, row_bytes, pos, len, options, Parsers.PosLen31)::Parsers.Result{Parsers.PosLen31}
            (val, tlen, code) = res.val, res.tlen, res.code
            return val, tlen, code, row_status, column_indicators
        end)
        for i = 1:fieldcount(customtypes)
            T = fieldtype(customtypes, i)
            pushfirst!(block.args, quote
                if type === $T
                    res = Parsers.xparse($(_type_proxy(T)), row_bytes, pos, len, options)::Parsers.Result{$(_type_proxy(T))}
                    (val, tlen, code) = res.val, res.tlen, res.code
                    unsafe_push!(cols[col_idx]::BufferedVector{$T}, val)
                    return val, tlen, code, row_status, column_indicators
                end
            end)
        end
        pushfirst!(block.args, :(type = _type))
        pushfirst!(block.args, Expr(:meta, :inline))
        # @show block
        return block
    else
        # println("generated function failed")
        res = Parsers.xparse(_type, row_bytes, pos, len, options)::Parsers.Result{_type}
        (val, tlen, code) = res.val, res.tlen, res.code
        unsafe_push!(cols[col_idx]::BufferedVector{_type}, val)
        return val, tlen, code, row_status, column_indicators
    end
end


function _parse_rows_forloop!(result_buf::TaskResultBuffer{M}, task::AbstractVector{Int32}, buf, schema, enum_schema, options, comment::Union{Nothing,Vector{UInt8}}, ::Type{CT}) where {M, CT}
    empty!(result_buf)
    N = length(schema)
    Base.ensureroom(result_buf, ceil(Int, length(task) * 1.01))
    ignorerepeated = options.ignorerepeated
    ignoreemptyrows = options.ignoreemptylines

    for chunk_row_idx in 2:length(task)
        @inbounds prev_newline = task[chunk_row_idx - 1]
        @inbounds curr_newline = task[chunk_row_idx]
        (ignoreemptyrows && _isemptyrow(prev_newline, curr_newline, buf)) && continue
        # +1 -1 to exclude newline chars
        @inbounds row_bytes = view(buf, prev_newline+Int32(1):curr_newline-Int32(1))
        skip_row!(result_buf, row_bytes, comment) && continue

        len = length(row_bytes)
        pos = 1
        # `ignorerepeated` is used to implement fixed width column parsing. We need to skip over the initial
        # delimiters to avoid getting an empty first value.
        if ignorerepeated
            pos = Parsers.checkdelim!(row_bytes, pos, len, options)
        end

        # Empty lines are treated as having too few columns
        code = (curr_newline - prev_newline) == 1 ? Parsers.EOF : Int16(0)
        row_status = RowStatus.Ok
        column_indicators = initflag(M)
        cols = result_buf.cols
        # Unlike `schema`, `enum_schema` also contains SKIP columns, so their lengths don't have to match
        # This way we know the schema of the TaskResultBuffers (schema) and the schema of the file (enum_schema)
        col_idx = 0
        @inbounds for type_enum in enum_schema
            if type_enum == Enums.SKIP
                Parsers.eof(code) && continue # we don't care if the skipped columns are not present on the line
                res = Parsers.xparse(String, row_bytes, pos, len, options, Parsers.PosLen31)::Parsers.Result{Parsers.PosLen31}
                pos += res.tlen
                code = res.code
                continue
            end

            col_idx += 1

            if Parsers.eof(code) && !(col_idx == N && Parsers.delimited(code))
                row_status |= RowStatus.TooFewColumns
                row_status |= RowStatus.HasColumnIndicators
                skip_element!(cols[col_idx])
                column_indicators = setflag(column_indicators, col_idx)
                for _col_idx in col_idx+1:N
                    skip_element!(getindex(cols, _col_idx))
                    column_indicators = setflag(column_indicators, _col_idx)
                end
                break
            end
            if type_enum == Enums.INT
                res = Parsers.xparse(Int, row_bytes, pos, len, options)::Parsers.Result{Int}
                (val, tlen, code) = res.val, res.tlen, res.code
                unsafe_push!(cols[col_idx]::BufferedVector{Int}, val)
            elseif type_enum == Enums.BOOL
                res = Parsers.xparse(Bool, row_bytes, pos, len, options)::Parsers.Result{Bool}
                (val, tlen, code) = res.val, res.tlen, res.code
                unsafe_push!(cols[col_idx]::BufferedVector{Bool}, val)
            elseif type_enum == Enums.FLOAT64
                res = Parsers.xparse(Float64, row_bytes, pos, len, options)::Parsers.Result{Float64}
                (val, tlen, code) = res.val, res.tlen, res.code
                unsafe_push!(cols[col_idx]::BufferedVector{Float64}, val)
            elseif type_enum == Enums.DATE
                res = Parsers.xparse(Dates.Date, row_bytes, pos, len, options)::Parsers.Result{Dates.Date}
                (val, tlen, code) = res.val, res.tlen, res.code
                unsafe_push!(cols[col_idx]::BufferedVector{Dates.Date}, val)
            elseif type_enum == Enums.GUESS_DATETIME
                res = Parsers.xparse(GuessDateTime, row_bytes, pos, len, options, Dates.DateTime)::Parsers.Result{Dates.DateTime}
                (val, tlen, code) = res.val, res.tlen, res.code
                unsafe_push!(cols[col_idx]::BufferedVector{Dates.DateTime}, val)
            elseif type_enum == Enums.DATETIME
                res = Parsers.xparse(DateTime, row_bytes, pos, len, options)::Parsers.Result{Dates.DateTime}
                (val, tlen, code) = res.val, res.tlen, res.code
                unsafe_push!(cols[col_idx]::BufferedVector{Dates.DateTime}, val)
            elseif type_enum == Enums.CHAR
                res = Parsers.xparse(Char, row_bytes, pos, len, options)::Parsers.Result{Char}
                (val, tlen, code) = res.val, res.tlen, res.code
                unsafe_push!(cols[col_idx]::BufferedVector{Char}, val)
            elseif type_enum == Enums.STRING
                res = Parsers.xparse(String, row_bytes, pos, len, options, Parsers.PosLen31)::Parsers.Result{Parsers.PosLen31}
                (val, tlen, code) = res.val, res.tlen, res.code
                unsafe_push!(cols[col_idx]::BufferedVector{Parsers.PosLen31}, Parsers.PosLen31(prev_newline+val.pos, val.len, val.missingvalue, val.escapedvalue))
            else
                (val, tlen, code, row_status, column_indicators) = parsecustom!(CT, row_bytes, pos, len, col_idx, cols, options, schema[col_idx], row_status, column_indicators)
            end
            if Parsers.sentinel(code)
                row_status |= RowStatus.HasColumnIndicators
                column_indicators = setflag(column_indicators, col_idx)
            elseif !Parsers.ok(code)
                row_status |= RowStatus.ValueParsingError
                row_status |= RowStatus.HasColumnIndicators
                column_indicators = setflag(column_indicators, col_idx)
            end
            pos += tlen
        end # for col_idx
        if !Parsers.eof(code)
            row_status |= RowStatus.TooManyColumns
        end
        unsafe_push!(result_buf.row_statuses, row_status)
        # TODO: replace anyflagset with a local variable?
        anyflagset(column_indicators) && push!(result_buf.column_indicators::BufferedVector{M}, column_indicators) # No inbounds as we're growing this buffer lazily
    end # for row_idx
    return nothing
end
