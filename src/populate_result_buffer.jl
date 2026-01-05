# What we need to forward to `ChunkedBase.populate_result_buffer!`
# Knowing the schema allows us to unroll on column types in a type
# stable way without specializations.
struct ParsingContext <: AbstractParsingContext
    # The schema of the TaskResultBuffer.cols
    schema::Vector{DataType}
    # The schema of the file as enums from src/Enums.jl.
    # This includes even skipped columns and is easier for the compiler to unroll on.
    enum_schema::Vector{Enums.CSV_TYPE}
    # The names of the columns in the TaskResultBuffer.cols.
    header::Vector{Symbol}
    # The escape character for the quoted strings in the file.
    escapechar::UInt8
    # The sets up how Parsers.xparse parses data
    options::Parsers.Options
end

skip_row!(result_buf::AbstractResultBuffer, row_bytes, comment::Nothing) = false
function skip_row!(result_buf::AbstractResultBuffer, row_bytes, comment::Vector{UInt8})
    return ChunkedBase._startswith(row_bytes, comment) && skip_row!(result_buf)
end
function skip_row!(result_buf::AbstractResultBuffer)
    foreach(skip_element!, result_buf.cols)
    unsafe_push!(result_buf.row_statuses, RowStatus.MissingValues | RowStatus.SkippedRow)
    addrows!(result_buf.missing_values, 1, true)
    return true
end

# This generated function unrolls on the "custom types" in the schema. Custom types are those that we
# don't unroll manually in populate_result_buffer.jl and for which we have corresponding Enum.jl values.
# Unrolling means generating type stable if-else branches which check the type in the conditional and then
# type assert in the body of the branch, it is a form of manual dispatch. This is necessary because otherwise,
# we'd hit dynamic dispatch on custom types which is bad for performance.
# This code has been adapted from CSV.jl
@inline function parsecustom!(::Type{customtypes}, row_bytes, pos, len, col_idx, cols, options, _type) where {customtypes}
    if @generated
        block = Expr(:block)
        pushfirst!(block.args, :(error(lazy"Unreachable: type not matched")))
        for i = 1:fieldcount(customtypes)
            T = fieldtype(customtypes, i)
            pushfirst!(block.args, quote
                if type === $T
                    res = Parsers.xparse($T, row_bytes, pos, len, options)::Parsers.Result{$T}
                    (val, tlen, code) = res.val, res.tlen, res.code
                    unsafe_push!(cols[col_idx]::BufferedVector{$T}, val)
                    return tlen, code
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
        return tlen, code
    end
end

function ChunkedBase.populate_result_buffer!(
    result_buf::AbstractResultBuffer,
    newlines_segment::AbstractVector{Int32},
    parsing_ctx::ParsingContext,
    buf::Vector{UInt8},
    comment::Union{Nothing,Vector{UInt8}}=nothing,
    ::Type{CT}=Tuple{}
) where {CT}
    empty!(result_buf)
    enum_schema = parsing_ctx.enum_schema
    schema = parsing_ctx.schema
    missing_idx = 1
    errored_idx = 1
    options = parsing_ctx.options

    # If we need to grow the buffer, we add a bit of extra room to avoid having to reallocate too often
    capacity(result_buf) < (length(newlines_segment)-1) && Base.ensureroom(result_buf, ceil(Int, length(newlines_segment) * 1.01))

    ignorerepeated = options.ignorerepeated::Bool
    ignoreemptyrows = options.ignoreemptylines::Bool
    missing_values = result_buf.missing_values
    errored_values = result_buf.errored_values
    cols = result_buf.cols

    N = length(schema)
    for row_idx in 2:length(newlines_segment)
        # We only grow the column indicators when we need to, this flag tacks whether we
        # already added one for this row
        added_missing_row = false
        added_errored_row = false
        @inbounds prev_newline = newlines_segment[row_idx - 1]
        @inbounds curr_newline = newlines_segment[row_idx]
        isemptyrow = ChunkedBase._isemptyrow(prev_newline, curr_newline, buf)
        (ignoreemptyrows && isemptyrow) && skip_row!(result_buf) && (missing_idx += 1; continue)
        # +1 -1 to exclude newline chars
        @inbounds row_bytes = view(buf, prev_newline+Int32(1):curr_newline-Int32(1))
        skip_row!(result_buf, row_bytes, comment) && (missing_idx += 1; continue)

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
                added_errored_row || (added_errored_row = true; addrows!(errored_values))
                for _col_idx in col_idx:N
                    skip_element!(cols[_col_idx])
                    errored_values[errored_idx, _col_idx] = true
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
                (tlen, code) = parsecustom!(CT, row_bytes, pos, len, col_idx, cols, options, schema[col_idx])
            end
            if Parsers.sentinel(code)
                row_status |= RowStatus.MissingValues
                added_missing_row || (added_missing_row = true; addrows!(missing_values))
                @inbounds missing_values[missing_idx, col_idx] = true
            elseif !Parsers.ok(code)
                row_status |= RowStatus.ValueParsingError
                added_errored_row || (added_errored_row = true; addrows!(errored_values))
                @inbounds errored_values[errored_idx, col_idx] = true
            end
            pos += tlen
        end # for col_idx
        if !Parsers.eof(code)
            row_status |= RowStatus.TooManyColumns
        end
        unsafe_push!(result_buf.row_statuses, row_status)
        missing_idx += added_missing_row
        errored_idx += added_errored_row
    end # for row_idx
    return nothing
end
