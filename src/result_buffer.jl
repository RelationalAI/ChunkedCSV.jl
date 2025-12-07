#
# RowStatus
#

"""
    RowStatus

A module implementing a bitflag type used to indicate the status of a row in a `TaskResultBuffer`.

- `0x00` -- `Ok`: All fields were parsed successfully.
- `0x01` -- `MissingValues`: Some fields have missing values.
- `0x02` -- `TooFewColumns`: The row has fewer fields than expected according to the schema. Implies `MissingValues`.
- `0x04` -- `TooManyColumns`: The row has more fields than expected according to the schema.
- `0x08` -- `ValueParsingError`: Some fields could not be parsed due to an unknown instance of a particular type. Implies `MissingValues`.
- `0x10` -- `SkippedRow`: The row contains no valid values, e.g. it was a comment. Implies `MissingValues`.

Multiple flags can be set at the same time, e.g. `MissingValues | TooFewColumns` means that at least column in the row does not have a known value and that there were not enough fields in this row.
If a row has `MissingValues` flag set, then the `missing_values` field of the `TaskResultBuffer` will contain a bitset indicating which columns have missing values.

# See also:
- [`TaskResultBuffer`](#TaskResultBuffer)
"""
module RowStatus
    const T = UInt8                  # Type of the row status flags

    const Ok                  = 0x00 # All ok
    const MissingValues       = 0x01 # Some fields have missing values
    const TooFewColumns       = 0x02 # Some fields have missing values due field count mismatch with the schema
    const TooManyColumns      = 0x04 # We have a valid record according to schema, but we didn't parse some fields due to missing schema info
    const ValueParsingError   = 0x08 # We couldn't parse some fields because we don't know how to parse that particular instance of that type
    const SkippedRow          = 0x10 # The row contains no valid values

    # Used in DebugContext
    const Marks = ('✓', '?', '<', '>', '!', '#')
    const Names = ("Ok", "MissingValues", "TooFewColumns", "TooManyColumns", "ValueParsingError", "SkippedRow")
    const Flags = (0x00, 0x01, 0x02, 0x04, 0x08, 0x10)
end

"""
    BitSetMatrix <: AbstractMatrix{Bool}

A matrix representing the missingness of values in the result buffer.
The number of rows in the matrix is equal the number of rows with at least one missing value in the result buffer.
The number of columns in the matrix is equal to the number of columns in the results buffer.

When consuming a `TaskResultBuffer` it is this recommended to iterate it from start to finish
and note the `RowStatus` for the `MissingValues` which signals that the row contains missing values.
Using `ColumnIterator`s is the easiest way to do this. For example:

```julia
# The first column has type T
for (value, isinvalidrow, ismissingvalue) for ColumnIterator{T}(result_buffer, 1)
    if isinvalidrow
        # The row didn't match the schema, so we better discard it
        continue
    end
    if ismissingvalue
        # The value is missing, so we can't use it
        continue
    end
    # Use the value
end
```

## Indexing
- `bs[r, c]`: Get the value at row `r` and column `c` of the matrix.
- `bs[r, :]`: Get the values in row `r` of the matrix.

## See also:
- [`TaskResultBuffer`](@ref), [`RowStatus`](@ref), [`ColumnIterator`](@ref)
"""
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
- `missing_values::BitSetMatrix`: a special type of `BitMatrix` where each row is a bitset signalling missing column values. Number of rows corresponds to the number of row statuses where `MissingValues` or `SkippedRow` flag is set.
- `errored_values::BitSetMatrix`: a special type of `BitMatrix` where each row is a bitset signalling errored column values. Number of rows corresponds to the number of row statuses where `TooFewColumns or `ValueParsingError` flag is set.

# Notes
- Each column in the `cols` field is a `BufferedVector` of the same type as the corresponding column in the `ParsingContext` schema.
- The `row_statuses` vector has the same length as each of the `cols` vectors.
- Strings are stored lazily as `Parsers.PosLen31` pointers to the underlying byte buffer (available in the `bytes` field of `ParsingContext`).
- When the file was parsed with `ignoreemptyrows=true` and/or a non-default `comment` argument, the `row_statuses` field might contain `SkippedRow` flags for all rows that were skipped.

# Example:

The following shows the structure of a `TaskResultBuffer` storing results for a messy CSV file which
we parsed expecting 3 `Int` columns and while skipping over comments:
```
+-------------------------+------------------------------------------------------------------------------+
|       INPUT CSV         |                             TASK_RESULT_BUFFER                               |
+-------------------------+--------------------------+----------+----------+---------+---------+---------+
| head,er,row             |        row_statuses      | missing  | errored  | cols[1] | cols[2] | cols[3] |
+-------------------------+--------------------------+----------+----------+---------+---------+---------+
| 1,1,1                   | Ok                       | No value | No value |    1    |    1    |    1    |
| 2,,2                    | Miss                     |  0 1 0   | No value |    2    |  undef  |    2    |
| 2,,                     | Miss                     |  0 1 1   | No value |    2    |  undef  |  undef  |
| 3,3                     | TooFewColumns            | No value |  0 0 1   |    3    |    3    |  undef  |
| 3                       | TooFewColumns            | No value |  0 1 1   |    3    |  undef  |  undef  |
| 4,4,4,4                 | TooManyColumns           | No value | No value |    4    |    4    |    4    |
| 4,4,4,4,4               | TooManyColumns           | No value | No value |    4    |    4    |    4    |
| garbage,garbage,garbage | ValueParsingError        | No value |  1 1 1   |  undef  |  undef  |  undef  |
| garbage,5,garbage       | ValueParsingError        | No value |  1 0 1   |  undef  |    5    |  undef  |
| garbage,,garbage        | Miss | ValueParsingError |  0 1 0   |  1 0 1   |  undef  |  undef  |  undef  |
| # comment               | Miss | SkippedRow        |  1 1 1   | No value |  undef  |  undef  |  undef  |
+-------------------------+--------------------------+----------+----------+---------+---------+---------+
Miss = MissingValues
```
"""
struct TaskResultBuffer <: AbstractResultBuffer
    id::Int
    cols::Vector{BufferedVector}
    row_statuses::BufferedVector{RowStatus.T}
    missing_values::BitSetMatrix
    errored_values::BitSetMatrix
end

# Since the chunk size if always <= 2GiB, we can never never overflow a PosLen31
# which uses 31 bits for the position and 31 bits for the length
_translate_to_buffer_type(::Type{String}) = Parsers.PosLen31
# GuessDateTime is just a DateTime parser that instead of parsing a specific format string,
# tries a bit harder to handle multiple ISO8601 compatible formats, including time zones.
_translate_to_buffer_type(::Type{GuessDateTime}) = Dates.DateTime
_translate_to_buffer_type(::Type{T}) where {T} = T

TaskResultBuffer(id, schema) = TaskResultBuffer(id, schema, 0)
# Assumes `schema` has been `_translate_to_buffer_type`'d
function TaskResultBuffer(id, schema::Vector{DataType}, n::Int)
    ncols = count(x->x !== Nothing, schema)
    TaskResultBuffer(
        id,
        BufferedVector[
            BufferedVector{T}(Vector{T}(undef, n), 0)
            for T
            in schema
            if T !== Nothing
        ],
        BufferedVector{RowStatus.T}(Vector{RowStatus.T}(undef, n), 0),
        BitSetMatrix(0, ncols),
        BitSetMatrix(0, ncols),
    )
end

# Assumes `schema` has been `_translate_to_buffer_type`'d
function _make_result_buffers(num_buffers::Integer, schema, n)
    out = Vector{TaskResultBuffer}(undef, num_buffers)
    ncols = count(x->x !== Nothing, schema)
    for i in 1:num_buffers
        @inbounds out[i] = TaskResultBuffer(
            i,
            Vector{BufferedVector}(undef, length(schema)),
            BufferedVector{RowStatus.T}(Vector{RowStatus.T}(undef, n), 0),
            BitSetMatrix(0, ncols),
            BitSetMatrix(0, ncols),
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
capacity(buf::TaskResultBuffer) = length(buf.row_statuses.elements)

function Base.empty!(buf::TaskResultBuffer)
    foreach(empty!, buf.cols)
    empty!(buf.row_statuses)
    empty!(buf.missing_values)
    empty!(buf.errored_values)
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
    missing_values::BitSetMatrix
    errored_values::BitSetMatrix
end
function ColumnIterator{T}(buf::TaskResultBuffer, column_position::Int) where {T}
    col = (buf.cols[column_position])::BufferedVector{T}
    return ColumnIterator{T}(col, column_position, buf.row_statuses, buf.missing_values, buf.errored_values)
end

Base.length(itr::ColumnIterator) = length(itr.statuses)

struct ParsedField{T}
    value::T             # The parsed value, garbage if `ismissingvalue` is true
    isinvalidrow::Bool   # True if the row didn't match the schema
    iserroredvalue::Bool # True if the value is invalid
    ismissingvalue::Bool # True if the value is missing
end
Base.iterate(t::ParsedField, iter=1) = iter > nfields(t) ? nothing : (getfield(t, iter), iter + 1)

function Base.iterate(itr::ColumnIterator{T}, state=(row=1, missing_idx=0, errored_idx=0)) where {T}
    row, missing_idx, errored_idx = state.row, state.missing_idx, state.errored_idx
    if row > length(itr.x)
        return nothing
    end
    s = @inbounds itr.statuses[row]
    value = @inbounds itr.x[row]::T
    has_errored_values_row = (s & (RowStatus.TooFewColumns | RowStatus.ValueParsingError)) != 0
    isinvalidrow = has_errored_values_row | ((s & RowStatus.TooManyColumns) != 0)
    errored_idx += has_errored_values_row

    has_missing_values_row = (s & RowStatus.MissingValues) != 0 # All SkippedRows have the MissingValues tag
    missing_idx += has_missing_values_row
    ismissingvalue = has_missing_values_row && @inbounds(itr.missing_values[missing_idx, itr.idx])
    iserroredvalue = has_errored_values_row && @inbounds(itr.errored_values[errored_idx, itr.idx])
    row += 1

    return ParsedField(value, isinvalidrow, iserroredvalue, ismissingvalue), (; row, missing_idx, errored_idx)
end
