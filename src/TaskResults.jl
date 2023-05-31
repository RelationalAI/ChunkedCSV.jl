"""TaskResultBuffer accumulates results produced by a single task"""

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

# TODO: a AbstractResultBuffer so we can generalize type signatures for ChunkedJSONL.
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
struct TaskResultBuffer
    id::Int
    cols::Vector{BufferedVector}
    row_statuses::BufferedVector{RowStatus.T}
    column_indicators::BitSetMatrix
end

_translate_to_buffer_type(::Type{String}, lazystrings=true) = lazystrings ? Parsers.PosLen31 : String
_translate_to_buffer_type(::Type{GuessDateTime}, lazystrings=true) = Dates.DateTime
_translate_to_buffer_type(::Type{T}, lazystring=true) where {T} = T

TaskResultBuffer(id, schema) = TaskResultBuffer(id, schema, 0)
TaskResultBuffer(id, schema::Vector{DataType}, n::Int) = TaskResultBuffer(
    id,
    BufferedVector[
        BufferedVector{_translate_to_buffer_type(T)}(Vector{_translate_to_buffer_type(T)}(undef, n), 0)
        for T
        in schema
        if T !== Nothing
    ],
    BufferedVector{RowStatus.T}(Vector{RowStatus.T}(undef, n), 0),
    BitSetMatrix(0, count(x->x !== Nothing, schema)),
)

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
