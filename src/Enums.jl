module Enums
    import Dates
    using FixedPointDecimals
    using ..ChunkedCSV: GuessDateTime

    @enum CSV_TYPE::UInt8 begin
        UNKNOWN
        SKIP
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
    )

    @assert isempty(symdiff(Base.instances(CSV_TYPE), values(_MAPPING)))

    to_enum(@nospecialize(T)) = @inbounds get(_MAPPING, T, UNKNOWN)::CSV_TYPE
end
