"""TaskResultBuffer accumulates results produced by a single task"""

module RowStatus
    const T = UInt8

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

@inline _bounding_flag_type(N) = N > 128 ? NTuple{1+((N-1)>>6),UInt64} :
    N > 64 ? UInt128 :
    N > 32 ? UInt64 :
    N > 16 ? UInt32 :
    N > 8 ? UInt16 : UInt8
_translate_to_buffer_type(::Type{String}) = Parsers.PosLen31
_translate_to_buffer_type(::Type{T}) where {T} = T

TaskResultBuffer(id, schema) = TaskResultBuffer{_bounding_flag_type(length(schema))}(id, schema, 0)
# Prealocate BufferedVectors with `n` values
TaskResultBuffer(id, schema, n) = TaskResultBuffer{_bounding_flag_type(length(schema))}(id, schema, n)
TaskResultBuffer{M}(id, schema::Vector{DataType}, n::Int) where {M} = TaskResultBuffer{M}(
    id,
    BufferedVector[BufferedVector{_translate_to_buffer_type(schema[i])}(Vector{_translate_to_buffer_type(schema[i])}(undef, n), 0) for i in 1:length(schema)],
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

flagpadding(ncolumns) = 8sizeof(ChunkedCSV._bounding_flag_type(ncolumns)) - ncolumns

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