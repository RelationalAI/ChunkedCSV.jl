skip_commented_row!(result_buf::TaskResultBuffer{M}, row_bytes, comment::Nothing) where {M} = false
function skip_commented_row!(result_buf::TaskResultBuffer{M}, row_bytes, comment::Vector{UInt8}) where {M}
    if _startswith(row_bytes, comment)
        foreach(skip_element!, result_buf.cols)
        unsafe_push!(result_buf.row_statuses, RowStatus.SkippedRow)
        push!(result_buf.column_indicators::BufferedVector{M}, initflagset(M))
        return true
    end
    return false
end

function _parse_rows_forloop!(result_buf::TaskResultBuffer{M}, task::AbstractVector{Int32}, buf, enum_schema, options, comment::Union{Nothing,Vector{UInt8}}) where {M}
    empty!(result_buf)
    N = length(enum_schema)
    Base.ensureroom(result_buf, ceil(Int, length(task) * 1.01))
    for chunk_row_idx in 2:length(task)
        @inbounds prev_newline = task[chunk_row_idx - 1]
        @inbounds curr_newline = task[chunk_row_idx]
        # +1 -1 to exclude newline chars
        @inbounds row_bytes = view(buf, prev_newline+Int32(1):curr_newline-Int32(1))
        skip_commented_row!(result_buf, row_bytes, comment) && continue

        pos = 1
        len = length(row_bytes)
        # Empty lines are treated as having too few columns
        code = (curr_newline - prev_newline) == 1 ? Parsers.EOF : Int16(0)
        row_status = RowStatus.Ok
        column_indicators = initflag(M)
        cols = result_buf.cols
        @inbounds for col_idx in 1:N
            type_enum = enum_schema[col_idx]::Enums.CSV_TYPE
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
                (;val, tlen, code) = Parsers.xparse(Int, row_bytes, pos, len, options)::Parsers.Result{Int}
                unsafe_push!(cols[col_idx]::BufferedVector{Int}, val)
            elseif type_enum == Enums.BOOL
                (;val, tlen, code) = Parsers.xparse(Bool, row_bytes, pos, len, options)::Parsers.Result{Bool}
                unsafe_push!(cols[col_idx]::BufferedVector{Bool}, val)
            elseif type_enum == Enums.FLOAT64
                (;val, tlen, code) = Parsers.xparse(Float64, row_bytes, pos, len, options)::Parsers.Result{Float64}
                unsafe_push!(cols[col_idx]::BufferedVector{Float64}, val)
            elseif type_enum == Enums.DATE
                (;val, tlen, code) = Parsers.xparse(Dates.Date, row_bytes, pos, len, options)::Parsers.Result{Dates.Date}
                unsafe_push!(cols[col_idx]::BufferedVector{Dates.Date}, val)
            elseif type_enum == Enums.DATETIME
                (;val, tlen, code) = Parsers.xparse(_GuessDateTime, row_bytes, pos, len, options, Dates.DateTime)::Parsers.Result{Dates.DateTime}
                unsafe_push!(cols[col_idx]::BufferedVector{Dates.DateTime}, val)
            elseif type_enum == Enums.CHAR
                (;val, tlen, code) = Parsers.xparse(Char, row_bytes, pos, len, options)::Parsers.Result{Char}
                unsafe_push!(cols[col_idx]::BufferedVector{Char}, val)
            elseif type_enum == Enums.STRING
                (;val, tlen, code) = Parsers.xparse(String, row_bytes, pos, len, options, Parsers.PosLen31)::Parsers.Result{Parsers.PosLen31}
                unsafe_push!(cols[col_idx]::BufferedVector{Parsers.PosLen31}, Parsers.PosLen31(prev_newline+val.pos, val.len, val.missingvalue, val.escapedvalue))
            # TODO: We currently support only 8 digits after decimal point. We need to update Parsers.jl to accept runtime params, then we'd provide `f`
            #       param at runtime, and we'll only have to unroll on T (Int8,Int16,Int32,Int64,Int128)
            elseif type_enum == Enums.FIXEDDECIMAL_INT8_0
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int8,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int8,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int8,0}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT8_1
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int8,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int8,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int8,1}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT8_2
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int8,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int8,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int8,2}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT16_0
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int16,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int16,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int16,0}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT16_1
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int16,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int16,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int16,1}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT16_2
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int16,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int16,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int16,2}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT16_3
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int16,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int16,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int16,3}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT16_4
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int16,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int16,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int16,4}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT32_0
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,0}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT32_1
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,1}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT32_2
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,2}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT32_3
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,3}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT32_4
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,4}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT32_5
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,5}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT32_6
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,6}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT32_7
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,7}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT32_8
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,8}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT64_0
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,0}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT64_1
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,1}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT64_2
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,2}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT64_3
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,3}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT64_4
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,4}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT64_5
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,5}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT64_6
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,6}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT64_7
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,7}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT64_8
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,8}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT128_0
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,0}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT128_1
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,1}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT128_2
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,2}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT128_3
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,3}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT128_4
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,4}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT128_5
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,5}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT128_6
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,6}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT128_7
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,7}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_INT128_8
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,8}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT8_0
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt8,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt8,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt8,0}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT8_1
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt8,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt8,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt8,1}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT8_2
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt8,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt8,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt8,2}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT16_0
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt16,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt16,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt16,0}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT16_1
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt16,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt16,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt16,1}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT16_2
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt16,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt16,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt16,2}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT16_3
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt16,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt16,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt16,3}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT16_4
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt16,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt16,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt16,4}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT32_0
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,0}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT32_1
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,1}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT32_2
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,2}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT32_3
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,3}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT32_4
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,4}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT32_5
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,5}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT32_6
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,6}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT32_7
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,7}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT32_8
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,8}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT64_0
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,0}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT64_1
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,1}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT64_2
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,2}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT64_3
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,3}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT64_4
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,4}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT64_5
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,5}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT64_6
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,6}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT64_7
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,7}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT64_8
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,8}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT128_0
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,0}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT128_1
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,1}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT128_2
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,2}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT128_3
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,3}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT128_4
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,4}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT128_5
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,5}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT128_6
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,6}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT128_7
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,7}}, val.x)
            elseif type_enum == Enums.FIXEDDECIMAL_UINT128_8
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,8}}, val.x)
            else
                row_status |= RowStatus.UnknownTypeError
                row_status |= RowStatus.HasColumnIndicators
                column_indicators = setflag(column_indicators, col_idx)
                skip_element!(cols[col_idx])
                (;val, tlen, code) = Parsers.xparse(String, row_bytes, pos, len, options, Parsers.PosLen31)::Parsers.Result{Parsers.PosLen31}
                # NOTE: Trying out parsing as much as possible now
                # for _col_idx in col_idx+1:N
                #     skip_element!(getindex(cols, _col_idx))
                #     column_indicators = setflag(column_indicators, _col_idx)
                # end
                # break
            end
            if Parsers.sentinel(code)
                row_status |= RowStatus.HasColumnIndicators
                column_indicators = setflag(column_indicators, col_idx)
            elseif !Parsers.ok(code)
                row_status |= RowStatus.ValueParsingError
                row_status |= RowStatus.HasColumnIndicators
                column_indicators = setflag(column_indicators, col_idx)
                # NOTE: Trying out parsing as much as possible now
                # for _col_idx in col_idx+1:N
                #     skip_element!(getindex(cols, _col_idx))
                #     column_indicators = setflag(column_indicators, _col_idx)
                # end
                # break
            end
            pos += tlen
        end # for col_idx
        if !Parsers.eof(code)
            row_status |= RowStatus.TooManyColumns
            row_status |= RowStatus.HasColumnIndicators
            column_indicators = setflag(column_indicators, N)
        end
        unsafe_push!(result_buf.row_statuses, row_status)
        # TODO: replace anyflagset with a local variable?
        anyflagset(column_indicators) && push!(result_buf.column_indicators::BufferedVector{M}, column_indicators) # No inbounds as we're growing this buffer lazily
    end # for row_idx
    return nothing
end
