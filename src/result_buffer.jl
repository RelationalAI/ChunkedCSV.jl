#
# RowStatus
#

module RowStatus
    const T = UInt8

    # TODO: currently we set `HasColumnIndicators` for all `TooFewColumns`, `TooManyColumns`, `ValueParsingError`, `UnknownTypeError`, `SkippedRow`
    #       but for `TooManyColumns` and `SkippedRow` we don't need them.
    const Ok                  = 0x00 # All ok
    const HasColumnIndicators = 0x01 # Some fields have missing values
    const TooFewColumns       = 0x02 # Some fields have missing values due field count mismatch with the schema
    const TooManyColumns      = 0x04 # We have a valid record according to schema, but we didn't parse some fields due to missing schema info
    const ValueParsingError   = 0x08 # We couldn't parse some fields because we don't know how to parse that particular instance of that type
    const UnknownTypeError    = 0x10 # We couldn't parse some fields because we don't know how to parse any instance of that type
    const SkippedRow          = 0x20 # The row contains no valid values

    const Marks = ('✓', '?', '<', '>', '!', 'T', '#')
    const Names = ("Ok", "HasColumnIndicators", "TooFewColumns", "TooManyColumns", "ValueParsingError", "UnknownTypeError", "SkippedRow")
    const Flags = (0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20)
end

mutable struct BitSetMatrix <: AbstractMatrix{Bool}
    data::BitVector
    nrows::Int
    ncolumns::Int
end
BitSetMatrix(nrows, ncolumns) = BitSetMatrix(falses(ncolumns * nrows), nrows, ncolumns)

Base.@propagate_inbounds Base.setindex!(bs::BitSetMatrix, v, r::Integer, c::Integer) = setindex!(bs.data, v, ((r - 1) * bs.ncolumns) + c)
Base.@propagate_inbounds Base.getindex(bs::BitSetMatrix, r::Integer, c::Integer) = getindex(bs.data, ((r - 1) * bs.ncolumns) + c)
Base.@propagate_inbounds Base.getindex(bs::BitSetMatrix, r::Integer, ::Colon) = getindex(bs.data, ((r - 1) * bs.ncolumns) .+ (1:bs.ncolumns))

Base.size(bs::BitSetMatrix) = (bs.nrows, bs.ncolumns)
function Base.empty!(bs::BitSetMatrix)
    empty!(bs.data)
    bs.nrows = 0
    return bs
end

function addrows!(bs::BitSetMatrix, n::Integer=1, v::Bool=false)
    n < 0 && ArgumentError("n must be >= 0")
    n == 0 && return bs.nrows
    resize!(bs.data, bs.ncolumns * (bs.nrows + n))
    @inbounds bs.data[end-n*bs.ncolumns+1:end] .= v
    return bs.nrows += n
end

function addcols!(bs::BitSetMatrix, n::Integer=1)
    n < 0 && ArgumentError("n must be >= 0")
    n == 0 && return bs.ncolumns
    new_column_count = bs.ncolumns + n
    resize!(bs.data, new_column_count * bs.nrows)
    @inbounds bs.data[end-n*bs.nrows+1:end] .= false
    # Shift the old data to respect the new column count
    for r in bs.nrows-1:-1:0
        for c in new_column_count:-1:bs.ncolumns+1
            @inbounds bs.data[(r * new_column_count) + c] = false
        end
        for c in bs.ncolumns:-1:1
            @inbounds bs.data[(r * new_column_count) + c] = bs.data[(r * bs.ncolumns) + c]
        end
    end
    return bs.ncolumns += n
end

#
# TaskResultBuffer
#

"""
    TaskResultBuffer

Holds the parsed results in columnar buffers.

# Fields
- `id::Int`: The unique identifier of the buffer object, in range of 1 to two times `nworkers` arg to the `parse_file` function.
- `cols::Vector{BufferedVector}`: A vector of vectors, each corresponding to a column in the CSV file. Note this field is abstractly typed.
- `row_statuses::BufferedVector{RowStatus.T}`: Contains a $(RowStatus.T) status flag for each row.
- `column_indicators::BitSetMatrix`: a special type of `BitMatrix` where each row is a bitset signalling missing column values. Number of rows corresponds to the number of row statuses where `HasColumnIndicators` flag is set.

Note: strings are stored lazily as `Parsers.PosLen31` pointers to the underlying byte buffer (available in the `bytes` field of `ParsingContext`.).

# Example:

The following shows the structure of a `TaskResultBuffer` storing results for a messy CSV file which
we parsed expecting 3 `Int` columns and while skipping over comments:
```
+-------------------------+-------------------------------------------------------------------------------+
|       INPUT CSV         |                               TASK_RESULT_BUFFER                              |
+-------------------------+---------------------------+--------------------+----------+---------+---------+
| head,er,row             |        row_statuses       | column_indicators  |  cols[1] | cols[2] | cols[3] |
+-------------------------+---------------------------+--------------------+----------+---------+---------+
| 1,1,1                   | Ok                        | No value           |     1    |    1    |    1    |
| 2,,2                    | HasCI                     |   0 1 0  #=[1,:]=# |     2    |  undef  |    2    |
| 2,,                     | HasCI                     |   0 1 1  #=[2,:]=# |     2    |  undef  |  undef  |
| 3,3                     | HasCI | TooFewColumns     |   0 0 1  #=[3,:]=# |     3    |    3    |  undef  |
| 3                       | HasCI | TooFewColumns     |   0 1 1  #=[4,:]=# |     3    |  undef  |  undef  |
| 4,4,4,4                 | TooManyColumns            | No value           |     4    |    4    |    4    |
| 4,4,4,4,4               | TooManyColumns            | No value           |     4    |    4    |    4    |
| garbage,garbage,garbage | HasCI | ValueParsingError |   1 1 1  #=[5,:]=# |   undef  |  undef  |  undef  |
| garbage,5,garbage       | HasCI | ValueParsingError |   1 0 1  #=[6,:]=# |   undef  |    5    |  undef  |
| garbage,,garbage        | HasCI | ValueParsingError |   1 1 1  #=[7,:]=# |   undef  |  undef  |  undef  |
| # comment               | HasCI | SkippedRow        |   1 1 1  #=[8,:]=# |   undef  |  undef  |  undef  |
+-------------------------+---------------------------+--------------------+----------+---------+---------+
HasCI = HasColumnIndicators
```
"""
struct TaskResultBuffer <: AbstractResultBuffer
    id::Int
    cols::Vector{BufferedVector}
    row_statuses::BufferedVector{RowStatus.T}
    column_indicators::BitSetMatrix
end

_translate_to_buffer_type(::Type{String}) = Parsers.PosLen31
_translate_to_buffer_type(::Type{GuessDateTime}) = Dates.DateTime
_translate_to_buffer_type(::Type{T}) where {T} = T

TaskResultBuffer(id, schema) = TaskResultBuffer(id, schema, 0)
# Assumes `schema` has been `_translate_to_buffer_type`'d
function TaskResultBuffer(id, schema::Vector{DataType}, n::Int)
    TaskResultBuffer(
        id,
        BufferedVector[
            BufferedVector{T}(Vector{T}(undef, n), 0)
            for T
            in schema
            if T !== Nothing
        ],
        BufferedVector{RowStatus.T}(Vector{RowStatus.T}(undef, n), 0),
        BitSetMatrix(0, count(x->x !== Nothing, schema)),
    )
end

# Assumes `schema` has been `_translate_to_buffer_type`'d
function _make_result_buffers(num_buffers::Integer, schema, n)
    out = Vector{TaskResultBuffer}(undef, num_buffers)
    for i in 1:num_buffers
        @inbounds out[i] = TaskResultBuffer(
            i,
            Vector{BufferedVector}(undef, length(schema)),
            BufferedVector{RowStatus.T}(Vector{RowStatus.T}(undef, n), 0),
            BitSetMatrix(0, count(x->x !== Nothing, schema)),
        )
    end
    for (j, T) in enumerate(schema)
        _push_buffers!(T, out, j, n)
    end
    return out
end

function _push_buffers!(::Type{T}, out, i, n) where {T}
    @inbounds for b in out
        b.cols[i] = BufferedVector{T}(Vector{T}(undef, n), 0)
    end
end

Base.length(buf::TaskResultBuffer) = length(buf.row_statuses)

function Base.empty!(buf::TaskResultBuffer)
    foreach(empty!, buf.cols)
    empty!(buf.row_statuses)
    empty!(buf.column_indicators)
    return nothing
end

function Base.ensureroom(buf::TaskResultBuffer, n)
    foreach(x->Base.ensureroom(x, n), buf.cols)
    Base.ensureroom(buf.row_statuses, n)
    return nothing
end

"""
    ColumnIterator{T}

Iterate over a column of a `TaskResultBuffer`. The iterator yields values of type `ParsedField{T}`,
which is a struct containing the parsed value, a flag indicating whether the row was invalid, and a flag indicating whether the value was missing.
"""
struct ColumnIterator{T}
    x::BufferedVector{T}
    idx::Int
    statuses::BufferedVector{RowStatus.T}
    colinds::BitSetMatrix
end
function ColumnIterator{T}(buf::TaskResultBuffer, column_position::Int) where {T}
    col = (buf.cols[column_position])::BufferedVector{T}
    return ColumnIterator{T}(col, column_position, buf.row_statuses, buf.column_indicators)
end

Base.length(itr::ColumnIterator) = length(itr.statuses)

struct ParsedField{T}
    value::T
    isinvalidrow::Bool
    ismissingvalue::Bool
end
Base.iterate(t::ParsedField, iter=1) = iter > nfields(t) ? nothing : (getfield(t, iter), iter + 1)

function Base.iterate(itr::ColumnIterator{T}, state=(row=1, indicator_idx=0)) where {T}
    row, indicator_idx = state.row, state.indicator_idx
    if row > length(itr.x)
        return nothing
    end
    s = @inbounds itr.statuses[row]
    value = @inbounds itr.x[row]::T
    isinvalidrow = s > RowStatus.HasColumnIndicators

    has_indicators = (s & RowStatus.HasColumnIndicators) != 0
    indicator_idx += has_indicators
    ismissingvalue = has_indicators && @inbounds(itr.colinds[indicator_idx, itr.idx])
    row += 1

    return ParsedField(value, isinvalidrow, ismissingvalue), (; row, indicator_idx)
end
