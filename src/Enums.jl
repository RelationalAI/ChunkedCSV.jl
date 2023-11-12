module Enums
    import Dates
    import Parsers
    using FixedPointDecimals
    using ..ChunkedCSV: GuessDateTime

    # Enums used to represent known types that we manually unroll in populate_result_buffer.jl
    # Unrolling on enums is easier for the compiler than unrolling on types. For unknown types,
    # we use a generated function to unroll on the types in the schema, see `parsecustom!` in
    # src/row_parsing.jl for how this is used.
    @enum CSV_TYPE::UInt8 begin
        UNKNOWN
        SKIP # This represents a column that was skipped by the user
        INT
        BOOL
        FLOAT64
        DATE
        DATETIME
        GUESS_DATETIME
        CHAR
        STRING
    end

    const _MAPPING = IdDict(
        Int => INT,
        Bool => BOOL,
        Float64 => FLOAT64,
        Dates.Date => DATE,
        Dates.DateTime => DATETIME,
        GuessDateTime => GUESS_DATETIME,
        Char => CHAR,
        String => STRING,
        Missing => UNKNOWN,
        Nothing => SKIP,
        Parsers.PosLen31 => STRING,
    )

    # Check we don't miss any types in the mapping
    @assert isempty(symdiff(Base.instances(CSV_TYPE), unique(values(_MAPPING)))) symdiff(Base.instances(CSV_TYPE), unique(values(_MAPPING)))

    to_enum(@nospecialize(T)) = @inbounds get(_MAPPING, T, UNKNOWN)::CSV_TYPE
end
