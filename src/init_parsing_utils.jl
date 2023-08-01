### Select / Drop

_subset_columns!(parsing_ctx, select::Nothing, drop::Nothing) = nothing
_subset_columns!(parsing_ctx, select::T,       drop::Nothing) where {T} = __subset_columns!(parsing_ctx, true, select)
_subset_columns!(parsing_ctx, select::Nothing, drop::T) where {T} = __subset_columns!(parsing_ctx, false, drop)
_subset_columns!(parsing_ctx, select::T,       drop::S) where {T,S} = throw(ArgumentError("cannot specify both `select` and `drop`"))

function _drop_col!(ctx, idx)
    deleteat!(ctx.header, idx)
    deleteat!(ctx.schema, idx)
    ctx.enum_schema[idx] = Enums.SKIP
    return nothing
end

function __subset_columns!(ctx, flip::Bool, cols::Function)
    idx = length(ctx.header)
    for name in Iterators.reverse(ctx.header)
        xor(flip, cols(idx, name)) && _drop_col!(ctx, idx)
        idx -= 1
    end
    return nothing
end
function __subset_columns!(ctx, flip::Bool, cols::AbstractVector{Bool})
    idx = length(ctx.header)
    idx != length(cols) && throw(ArgumentError("invalid number of columns: $(length(cols))"))
    for flag in Iterators.reverse(cols)
        xor(flip, flag) && _drop_col!(ctx, idx)
        idx -= 1
    end
    return nothing
end
function __subset_columns!(ctx, flip::Bool, cols::AbstractVector{<:Integer})
    isempty(cols) && throw(ArgumentError("empty column index"))
    cols = sort(cols)
    last_idx = length(ctx.header)
    cols[begin] < 1 && throw(ArgumentError("invalid column index: $(cols[begin])"))
    cols[end] > last_idx && throw(ArgumentError("invalid column index: $(cols[end])"))

    for idx in last_idx:-1:1
        xor(flip, insorted(idx, cols)) && _drop_col!(ctx, idx)
    end
    return nothing
end
function __subset_columns!(ctx, flip::Bool, cols::AbstractVector{Symbol})
    isempty(cols) && throw(ArgumentError("empty column name"))
    s = Set(cols)
    idx = length(ctx.header)
    for name in Iterators.reverse(ctx.header)
        xor(flip, !isnothing(pop!(s, name, nothing))) && _drop_col!(ctx, idx)
        idx -= 1
    end
    !isempty(s) && throw(ArgumentError("invalid column names: $(collect(s))"))
    return nothing
end
__subset_columns!(ctx, flip::Bool, cols::AbstractVector{<:AbstractString}) = __subset_columns!(ctx, flip, map(Symbol, cols))

### makeunique

# copied from CSV.jl
function makeunique(names::Vector{Symbol})
    set = Set(names)
    length(set) == length(names) && return names
    nms = Symbol[]
    nextsuffix = Dict{Symbol, UInt}()
    for nm in names
        if haskey(nextsuffix, nm)
            k = nextsuffix[nm]
            newnm = Symbol(nm, :_, Symbol(k))
            while newnm in set || haskey(nextsuffix, newnm)
                k += 1
                newnm = Symbol(nm, :_, Symbol(k))
            end
            nextsuffix[nm] = k + 1
            nm = newnm
        end
        push!(nms, nm)
        nextsuffix[nm] = 1
    end
    @assert length(names) == length(nms)
    return nms
end

### Validation

function validate_schema(types::Vector{DataType})
    all_ok = all(_is_supported_type, types)
    if !all_ok
        unsupported_types = unique!(filter(t->!(_is_supported_type(t)), types))
        err_msg = "Provided schema contains unsupported types: $(join(unsupported_types, ", "))."
        throw(ArgumentError(err_msg))
    end
    return nothing
end

### Schema filling

const DEFAULT_COLUMN_TYPE = String

function _fill_schema!(schema_to_be_filled, header, schema_input, settings)
    resize!(schema_to_be_filled, length(header))
    __fill_schema!(schema_to_be_filled, header, schema_input, settings)
end

function __fill_schema!(schema_to_be_filled, header, schema_input::Dict{Int}, settings)
    bounds = UnitRange(extrema(keys(schema_input))...)
    if !(!settings.validate_type_map || issubset(bounds, 1:length(header)))
        throw(ArgumentError("Invalid column indices in schema mapping: $(collect(setdiff(keys(schema_input), 1:length(header)))), parsed header: $(header), row $(settings.header_at)"))
    end
    @inbounds for i in 1:length(header)
        schema_to_be_filled[i] = get(schema_input, i, DEFAULT_COLUMN_TYPE)
    end
end

function __fill_schema!(schema_to_be_filled, header, schema_input::Dict{Symbol}, settings)
    if !(!settings.validate_type_map || issubset(keys(schema_input), header))
        throw(ArgumentError("Unknown columns from schema mapping: $(collect(setdiff(keys(schema_input), header))), parsed header: $(header), row $(settings.header_at)"))
    end
    @inbounds for (i, colname) in enumerate(header)
        schema_to_be_filled[i] = get(schema_input, colname, DEFAULT_COLUMN_TYPE)
    end
end

function __fill_schema!(schema_to_be_filled, header, schema_input::Base.Callable, settings)
    @inbounds for (i, name) in enumerate(header)
        schema_to_be_filled[i] = schema_input(i, name)
    end
end

function __fill_schema!(schema_to_be_filled, header, ::Type{schema_input}, settings) where {schema_input}
    fill!(schema_to_be_filled, schema_input)
end

function __fill_schema!(schema_to_be_filled, header, ::Nothing, settings)
    fill!(schema_to_be_filled, DEFAULT_COLUMN_TYPE)
end
