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

# TODO: a AbstractResultBuffer so we can generalize type signatures for ChunkedJSONL.
"""
    TaskResultBuffer{M}

Holds the parsed results in columnar buffers.

# Fields
- `id::Int`: The unique identifier of the buffer object, in range of 1 to two times `nworkers` arg to the `parse_file` function.
- `cols::Vector{BufferedVector}`: A vector of vectors, each corresponding to a column in the CSV file. Note this field is abstractly typed.
- `row_statuses::BufferedVector{RowStatus.T}`: Contains a $(RowStatus.T) status flag for each row.
- `column_indicators::BufferedVector{M}`: Contains bitsets of type `M` with set bits corresponding to missing or errored columns. Length is the number of row statuses where `HasColumnIndicators` flag is set.

Note: strings are stored lazily as `Parsers.PosLen31` pointers to the underlying byte buffer (available in the `bytes` field of `ParsingContext`.).

# Example:

The following shows the structure of a `TaskResultBuffer` storing results for a messy CSV file which
we parsed expecting 3 `Int` columns and while skipping over comments:
```
+-------------------------+------------------------------------------------------------------------------+
|       INPUT CSV         |                               TASK_RESULT_BUFFER                             |
+-------------------------+---------------------------+-------------------+----------+---------+---------+
| head,er,row             |        row_statuses       | column_indicators |  cols[1] | cols[2] | cols[3] |
+-------------------------+---------------------------+-------------------+----------+---------+---------+
| 1,1,1                   | Ok                        |      No value     |     1    |    1    |    1    |
| 2,,2                    | HasCI                     |  0 0 0 0 0 0 1 0  |     2    |  undef  |    2    |
| 2,,                     | HasCI                     |  0 0 0 0 0 1 1 0  |     2    |  undef  |  undef  |
| 3,3                     | HasCI | TooFewColumns     |  0 0 0 0 0 1 0 0  |     3    |    3    |  undef  |
| 3                       | HasCI | TooFewColumns     |  0 0 0 0 0 1 1 0  |     3    |  undef  |  undef  |
| 4,4,4,4                 | TooManyColumns            |      No value     |     4    |    4    |    4    |
| 4,4,4,4,4               | TooManyColumns            |      No value     |     4    |    4    |    4    |
| garbage,garbage,garbage | HasCI | ValueParsingError |  0 0 0 0 0 1 1 1  |   undef  |  undef  |  undef  |
| garbage,5,garbage       | HasCI | ValueParsingError |  0 0 0 0 0 1 0 1  |   undef  |    5    |  undef  |
| # comment               | HasCI | SkippedRow        |  1 1 1 1 1 1 1 1  |   undef  |  undef  |  undef  |
+-------------------------+---------------------------+-------------------+----------+---------+---------+
HasCI = HasColumnIndicators
```
"""
struct TaskResultBuffer{M}
    id::Int
    cols::Vector{BufferedVector}
    row_statuses::BufferedVector{RowStatus.T}
    column_indicators::BufferedVector{M}

    function TaskResultBuffer{M}(id, cols, row_statuses, column_indicators) where {M}
        @assert M isa DataType
        return new{M}(id, cols, row_statuses, column_indicators)
    end
end

@inline function _bounding_flag_type(N::Integer)
    return N > 128 ? NTuple{1+((N-1)>>6),UInt64} :
        N > 64 ? UInt128 :
        N > 32 ? UInt64 :
        N > 16 ? UInt32 :
        N > 8 ? UInt16 : UInt8
end
@inline function _bounding_flag_type(schema::Vector{DataType})
    N = count(x->x !== Nothing, schema)
    return _bounding_flag_type(N)
end
@inline function _bounding_flag_type(schema::Vector{Enums.CSV_TYPE})
    N = count(x->x != Enums.SKIP, schema)
    return _bounding_flag_type(N)
end
_translate_to_buffer_type(::Type{String}, lazystrings=true) = lazystrings ? Parsers.PosLen31 : String
_translate_to_buffer_type(::Type{GuessDateTime}, lazystrings=true) = Dates.DateTime
_translate_to_buffer_type(::Type{T}, lazystring=true) where {T} = T

TaskResultBuffer(id, schema) = TaskResultBuffer{_bounding_flag_type(schema)}(id, schema, 0)
# Prealocate BufferedVectors with `n` values
TaskResultBuffer(id, schema, n) = TaskResultBuffer{_bounding_flag_type(schema)}(id, schema, n)
TaskResultBuffer{M}(id, schema::Vector{DataType}, n::Int) where {M} = TaskResultBuffer{M}(
    id,
    BufferedVector[
        BufferedVector{_translate_to_buffer_type(T)}(Vector{_translate_to_buffer_type(T)}(undef, n), 0)
        for T
        in schema
        if T !== Nothing
    ],
    BufferedVector{RowStatus.T}(Vector{RowStatus.T}(undef, n), 0),
    BufferedVector{M}(),
)

function Base.empty!(buf::TaskResultBuffer)
    foreach(empty!, buf.cols)
    empty!(buf.row_statuses)
    empty!(buf.column_indicators)
end

function Base.ensureroom(buf::TaskResultBuffer, n)
    foreach(x->Base.ensureroom(x, n), buf.cols)
    Base.ensureroom(buf.row_statuses, n)
end

initflag(::Type{T}) where {T<:Unsigned} = zero(T)
initflag(::Type{NTuple{N,T}}) where {N,T<:Unsigned} = ntuple(_->zero(T), N)

initflagset(::Type{T}) where {T<:Unsigned} = ~zero(T)
initflagset(::Type{NTuple{N,T}}) where {N,T<:Unsigned} = ntuple(_->~zero(T), N)

function isflagset(x::NTuple{N,T}, n) where {T,N}
    n <= 0 && return false
    d, r = fldmod1(n, 8sizeof(T))
    (N >= d) && (((x[N - d + 1] >> (r - 1)) & one(T)) == one(T))
end
isflagset(x::UInt128, n) = (UInt32((x >> (n - 1)) % UInt32) & UInt32(1)) == UInt32(1)
isflagset(x::T, n) where {T<:Union{UInt64, UInt32, UInt16, UInt8}} = ((x >> (n - 1)) & T(1)) == T(1)

setflag(x::T, n) where {T<:Unsigned} = x | (one(T) << (n - 1))
function setflag(x::NTuple{N,T}, n) where {T,N}
    d, r = fldmod1(n, 8sizeof(T))
    j = N - d + 1
    j < 1 && return x
    return ntuple(i -> @inbounds(i == j ? setflag(x[i], r) : x[i]), N)
end

anyflagset(x::Unsigned) = !iszero(x)
anyflagset(x::NTuple{N,UInt64}) where {N} = any(anyflagset, x)

flagpadding(ncolumns) = 8sizeof(_bounding_flag_type(ncolumns)) - ncolumns

firstflagset(x::Unsigned) = 8sizeof(typeof(x)) - leading_zeros(x)
lastflagset(x::Unsigned) = trailing_zeros(x) + 1
function firstflagset(xs::NTuple{N,UInt64}) where {N}
    out = 8sizeof(typeof(xs))
    @inbounds for i in 1:N
        x = xs[i]
        out -= leading_zeros(x)
        x > 0 && break
    end
    return out
end
function lastflagset(xs::NTuple{N,UInt64}) where {N}
    out = 0
    @inbounds for i in N:-1:1
        x = xs[i]
        out += trailing_zeros(x)
        x > 0 && break
    end
    return out + 1
end
