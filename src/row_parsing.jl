skip_row!(result_buf::TaskResultBuffer, row_bytes, comment::Nothing) = false
function skip_row!(result_buf::TaskResultBuffer, row_bytes, comment::Vector{UInt8})
    if _startswith(row_bytes, comment)
        foreach(skip_element!, result_buf.cols)
        unsafe_push!(result_buf.row_statuses, RowStatus.HasColumnIndicators | RowStatus.SkippedRow)
        addrows!(result_buf.column_indicators, 1, true)
        return true
    end
    return false
end

_type_proxy(::Type{T}) where {T} = T
_type_proxy(::Type{FixedDecimal{T,F}}) where {T,F} = _FixedDecimal{T,F}

function _isemptyrow(prev_nl, next_nl, bytes)
    return prev_nl + 1 == next_nl || (prev_nl + 2 == next_nl && @inbounds(bytes[prev_nl+1]) == UInt8('\r'))
end

@inline function parsecustom!(::Type{customtypes}, row_bytes, pos, len, col_idx, cols, options, _type, row_status, colinds) where {customtypes}
    if @generated
        block = Expr(:block)
        push!(block.args, quote
            # TODO: Currently, we shouldn't ever hit this path as we either throw an error for unsupported types
            # or Parsers throw an error internally for weird custom Integer subtypes that don't have a parse method.
            row_status |= RowStatus.UnknownTypeError
            row_status |= RowStatus.HasColumnIndicators
            colinds = setflag(colinds, col_idx)
            skip_element!(cols[col_idx])
            res = Parsers.xparse(String, row_bytes, pos, len, options, Parsers.PosLen31)::Parsers.Result{Parsers.PosLen31}
            (val, tlen, code) = res.val, res.tlen, res.code
            return val, tlen, code, row_status, colinds
        end)
        for i = 1:fieldcount(customtypes)
            T = fieldtype(customtypes, i)
            pushfirst!(block.args, quote
                if type === $T
                    res = Parsers.xparse($(_type_proxy(T)), row_bytes, pos, len, options)::Parsers.Result{$(_type_proxy(T))}
                    (val, tlen, code) = res.val, res.tlen, res.code
                    unsafe_push!(cols[col_idx]::BufferedVector{$T}, val)
                    return val, tlen, code, row_status, colinds
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
        return val, tlen, code, row_status, colinds
    end
end

function mark_missing!(colinds, colinds_row_idx, col_idx)
    row_diff = colinds_row_idx - size(colinds, 1)
    @assert row_diff == 0 || row_diff == 1
    row_diff == 1 && addrows!(colinds)
    @inbounds colinds[colinds_row_idx, col_idx] = true
    return
end


function _parse_rows_forloop!(result_buf::TaskResultBuffer, task::AbstractVector{Int32}, buf, schema, enum_schema, options, comment::Union{Nothing,Vector{UInt8}}, ::Type{CT}) where {CT}
    empty!(result_buf)
    colinds_row_idx = 1

    Base.ensureroom(result_buf, ceil(Int, length(task) * 1.01))

    ignorerepeated = options.ignorerepeated
    ignoreemptyrows = options.ignoreemptylines
    colinds = result_buf.column_indicators

    N = length(schema)
    for row_idx in 2:length(task)
        @inbounds prev_newline = task[row_idx - 1]
        @inbounds curr_newline = task[row_idx]
        isemptyrow = _isemptyrow(prev_newline, curr_newline, buf)
        (ignoreemptyrows && isemptyrow) && continue
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
        code = isemptyrow ? Parsers.EOF : Int16(0)
        row_status = RowStatus.Ok
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
                for _col_idx in col_idx:N
                    skip_element!(cols[_col_idx])
                    mark_missing!(colinds, colinds_row_idx, _col_idx)
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
                (val, tlen, code, row_status, colinds) = parsecustom!(CT, row_bytes, pos, len, col_idx, cols, options, schema[col_idx], row_status, colinds)
            end
            if Parsers.sentinel(code)
                row_status |= RowStatus.HasColumnIndicators
                mark_missing!(colinds, colinds_row_idx, col_idx)
            elseif !Parsers.ok(code)
                row_status |= RowStatus.ValueParsingError
                row_status |= RowStatus.HasColumnIndicators
                mark_missing!(colinds, colinds_row_idx, col_idx)
            end
            pos += tlen
        end # for col_idx
        if !Parsers.eof(code)
            row_status |= RowStatus.TooManyColumns
        end
        unsafe_push!(result_buf.row_statuses, row_status)
        colinds_row_idx += (row_status & RowStatus.HasColumnIndicators) > 0
    end # for row_idx
    return nothing
end
