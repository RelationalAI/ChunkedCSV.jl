""" A Vector that doesn't call `_growend!` quite as often (copy-pasted from ProtoBuf.jl) """
mutable struct BufferedVector{T}
    elements::Vector{T}
    occupied::Int
end
BufferedVector{T}() where {T} = BufferedVector(T[], 0)
BufferedVector(v::Vector{T}) where {T} = BufferedVector{T}(v, length(v))
Base.getindex(x::BufferedVector) = resize!(x.elements, x.occupied)

Base.length(x::BufferedVector) = x.occupied
Base.size(x::BufferedVector) = (x.occupied,)
Base.@propagate_inbounds function Base.last(x::BufferedVector)
    x.elements[x.occupied]
end
@inline function Base.first(x::BufferedVector)
    @boundscheck x.occupied > 0
    @inbounds x.elements[1]
end
@inline function Base.getindex(x::BufferedVector, i::Int)
    @boundscheck x.occupied >= i && i > 0
    @inbounds x.elements[i]
end
Base.ndims(x::BufferedVector) = 1
Base.empty!(x::BufferedVector) = x.occupied = 0
Base.isempty(x::BufferedVector) = x.occupied == 0
Base.IndexStyle(::BufferedVector) = Base.IndexLinear()
Base.IteratorSize(::BufferedVector) = Base.HasLength()
Base.IteratorEltype(::BufferedVector) = Base.HasEltype()
Base.eltype(::BufferedVector{T}) where T = T
@inline function Base.push!(buffer::BufferedVector{T}, x::T) where {T}
    if length(buffer.elements) == buffer.occupied
        Base._growend!(buffer.elements, _grow_by(T))
    end
    buffer.occupied += 1
    @inbounds buffer.elements[buffer.occupied] = x
end
_grow_by(::Type{T}) where {T<:Union{UInt32,UInt64,Int64,Int32,Enum{Int32},Enum{UInt32}}} = div(128, sizeof(T))
_grow_by(::Type) = 16
_grow_by(::Type{T}) where {T<:Union{Bool,UInt8}} = 64

@inline function unsafe_push!(buffer::BufferedVector{T}, x::T) where {T}
    buffer.occupied += 1
    @inbounds buffer.elements[buffer.occupied] = x
end
Base.ensureroom(x::BufferedVector, n) = ((length(x.elements) < n) && Base._growend!(x.elements, n - length(x.elements)); return nothing)
skip_element!(x::BufferedVector) = x.occupied += 1
function shiftleft!(x::BufferedVector, n)
    n == 0 && return
    len = length(x)
    unsafe_copyto!(x.elements, 1, x.elements, 1 + n, len - n + 1)
    x.occupied -= n
    return nothing
end