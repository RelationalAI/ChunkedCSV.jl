# These are the user-provided arguments to `parse_file` or `setup_parser`, that we need to
# use for the bootstrapping phase, when we skip any unwanted leading rows and sniff the beginning of
# the file to determine/validate the newline, header, schema, etc.
# We'll use them to construct the `Lexer`, `ParsingContext` and `ChunkingContext`,
# after we sniff the beginning the file to determine the newline,
# header, etc.
struct InputArguments
    header::Union{Nothing,Vector{Symbol}}
    header_at::Int
    data_at::Int
    validate_type_map::Bool
    default_colname_prefix::String
    no_quoted_newlines::Bool
    newlinechar::Union{Nothing,UInt8}
    deduplicate_names::Bool
    function InputArguments(header::Vector{Symbol}, data_at, validate_type_map, default_colname_prefix, no_quoted_newlines, newlinechar, deduplicate_names)
        new(header,  0,           Int(data_at), validate_type_map, default_colname_prefix, no_quoted_newlines, newlinechar, deduplicate_names)
    end

    function InputArguments(header::Integer,        data_at, validate_type_map, default_colname_prefix, no_quoted_newlines, newlinechar, deduplicate_names)
        new(nothing, Int(header), Int(data_at), validate_type_map, default_colname_prefix, no_quoted_newlines, newlinechar, deduplicate_names)
    end
end

# Fail early if the user is trying to parse something we don't know how to parse
_is_supported_type(::Type{T}) where {T} = Parsers.supportedtype(T)
_is_supported_type(::Type{Nothing}) = true # Column skipping
_is_supported_type(::Type{GuessDateTime}) = true # Our custom DateTime parser that handles multiple subsets of ISO8601
function _is_supported_type(::Type{FixedDecimal{T,f}}) where {T,f}
    # This check is copied from FixedPointDecimals.jl, the library uses it as runtime check,
    # but we want to fail early if the user is trying to parse something is not possible to construct.
    # https://github.com/JuliaMath/FixedPointDecimals.jl/blob/1328b9a372d2285765a7255f154f09ffdd692508/src/FixedPointDecimals.jl#L83-L91
    n = FixedPointDecimals.max_exp10(T)
    return f >= 0 && (n < 0 || f <= n)
end

# Separate out the types that are not pre-compiled by the parser by default
# and return them as a single Tuple of unique types which can be passed to
# populate_result_buffer! to trigger recompilation. See `parsecustom!`
# in src/populate_result_buffer.jl for how this is used.
function _custom_types(schema::Vector{DataType})
    # We sort the unique types to always produce the same Tuple for the same
    # schema. But maybe the default ordering from the IdDict is good enough?
    custom_types = sort!(collect(keys(
            IdDict{Type,Nothing}(
                T => nothing for T in schema if isnothing(get(Enums._MAPPING, T, nothing))
            ))),
        by=objectid
    )
    return Tuple{custom_types...}
end

# Parsers.Options constructor with our defaults.
# Used in `setup_parser`.
function _create_options(;
    delim::Union{UInt8,Char,String,Nothing}=',',
    openquotechar::Union{UInt8,Char}='"',
    closequotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    sentinel::Union{Missing,Nothing,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    truestrings::Union{Nothing,Vector{String}}=["true", "True", "TRUE", "1", "t", "T"],
    falsestrings::Union{Nothing,Vector{String}}=["false", "False", "FALSE", "0", "f", "F"],
    dateformat::Union{String, Dates.DateFormat, Nothing, AbstractDict}=nothing,
    ignorerepeated::Bool=false,
    quoted::Bool=true,
    decimal::Union{Char,UInt8}='.',
    ignoreemptyrows::Bool=true,
    rounding::Union{Nothing,RoundingMode}=RoundNearest,
)
    return Parsers.Options(
        sentinel=sentinel,
        wh1=delim ==  ' ' ? '\v' : ' ',
        wh2=delim == '\t' ? '\v' : '\t',
        openquotechar=UInt8(openquotechar),
        closequotechar=UInt8(closequotechar),
        escapechar=UInt8(escapechar),
        delim=delim,
        quoted=quoted,
        stripwhitespace=stripwhitespace,
        trues=truestrings,
        falses=falsestrings,
        groupmark=groupmark,
        dateformat=dateformat,
        ignorerepeated=ignorerepeated,
        decimal=UInt8(decimal),
        ignoreemptylines=ignoreemptyrows,
        rounding=rounding,
    )
end

# Check that the schema and header are compatible
_validate(header::Vector, schema::Vector, validate_type_map) = length(header) == length(schema) || throw(ArgumentError("Provided header and schema lengths don't match. Header has $(length(header)) columns, schema has $(length(schema))."))
_validate(header::Vector, schema::Dict{Symbol}, validate_type_map) = !validate_type_map || issubset(keys(schema), header) || throw(ArgumentError("Provided header and schema names don't match. In schema, not in header: $(collect(setdiff(keys(schema), header))). In header, not in schema: $(setdiff(header, keys(schema)))"))
function _validate(header::Vector, schema::Dict{Int}, validate_type_map)
    validate_type_map || return
    len = length(header)
    (lo, hi) = extrema(keys(schema))
    (lo < 1 || hi > len) && throw(ArgumentError("Provided schema indices are incompatible with header of length $(len). Offending indices from schema $(collect(setdiff(keys(schema), 1:len))))."))
end
_validate(header, schema, validate_type_map) = true

_nbytes(::UInt8) = 1
_nbytes(x::Union{String,Char}) = ncodeunits(x)

# We're a bit stricter than Parsers.jl here, because the Lexer doesn't support
# multi-byte characters.
function validate_parser_args(;openquotechar, closequotechar, delim, escapechar, decimal, newlinechar, ignorerepeated)
    _nbytes(openquotechar) == 1 || throw(ArgumentError("`openquotechar` must be a single-byte character"))
    _nbytes(closequotechar) == 1 || throw(ArgumentError("`closequotechar` must be a single-byte character"))
    if isnothing(delim)
        ignorerepeated && throw(ArgumentError("auto-delimiter detection not supported when `ignorerepeated=true`; please provide delimiter like `delim=','`"))
    else
        _nbytes(delim) == 1 || throw(ArgumentError("`delim` must be a single-byte character"))
    end
    _nbytes(escapechar) == 1 || throw(ArgumentError("`escapechar` must be a single-byte character"))
    _nbytes(decimal) == 1 || throw(ArgumentError("`decimal` must be a single-byte character"))

    if !isnothing(newlinechar)
        _nbytes(newlinechar) > 1 && throw(ArgumentError("`newlinechar` must be a single-byte character."))
        ((newlinechar % UInt8) in ((openquotechar % UInt8), (closequotechar % UInt8), (escapechar % UInt8), (something(delim, 0x00) % UInt8))) &&
            throw(ArgumentError("`newlinechar` must be different from `delim`, `openquotechar`, `closequotechar`, and `escapechar`"))
    end

    return nothing
end
