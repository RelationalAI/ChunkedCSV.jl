module Enums
    import Dates
    using FixedPointDecimals

    @enum CSV_TYPE::UInt8 begin
        UNKNOWN
        SKIP
        INT
        BOOL
        FLOAT64
        DATE
        DATETIME
        CHAR
        STRING
        FIXEDDECIMAL_INT8_0
        FIXEDDECIMAL_INT8_1
        FIXEDDECIMAL_INT8_2
        FIXEDDECIMAL_INT16_0
        FIXEDDECIMAL_INT16_1
        FIXEDDECIMAL_INT16_2
        FIXEDDECIMAL_INT16_3
        FIXEDDECIMAL_INT16_4
        FIXEDDECIMAL_INT32_0
        FIXEDDECIMAL_INT32_1
        FIXEDDECIMAL_INT32_2
        FIXEDDECIMAL_INT32_3
        FIXEDDECIMAL_INT32_4
        FIXEDDECIMAL_INT32_5
        FIXEDDECIMAL_INT32_6
        FIXEDDECIMAL_INT32_7
        FIXEDDECIMAL_INT32_8
        FIXEDDECIMAL_INT64_0
        FIXEDDECIMAL_INT64_1
        FIXEDDECIMAL_INT64_2
        FIXEDDECIMAL_INT64_3
        FIXEDDECIMAL_INT64_4
        FIXEDDECIMAL_INT64_5
        FIXEDDECIMAL_INT64_6
        FIXEDDECIMAL_INT64_7
        FIXEDDECIMAL_INT64_8
        FIXEDDECIMAL_INT128_0
        FIXEDDECIMAL_INT128_1
        FIXEDDECIMAL_INT128_2
        FIXEDDECIMAL_INT128_3
        FIXEDDECIMAL_INT128_4
        FIXEDDECIMAL_INT128_5
        FIXEDDECIMAL_INT128_6
        FIXEDDECIMAL_INT128_7
        FIXEDDECIMAL_INT128_8
        FIXEDDECIMAL_UINT8_0
        FIXEDDECIMAL_UINT8_1
        FIXEDDECIMAL_UINT8_2
        FIXEDDECIMAL_UINT16_0
        FIXEDDECIMAL_UINT16_1
        FIXEDDECIMAL_UINT16_2
        FIXEDDECIMAL_UINT16_3
        FIXEDDECIMAL_UINT16_4
        FIXEDDECIMAL_UINT32_0
        FIXEDDECIMAL_UINT32_1
        FIXEDDECIMAL_UINT32_2
        FIXEDDECIMAL_UINT32_3
        FIXEDDECIMAL_UINT32_4
        FIXEDDECIMAL_UINT32_5
        FIXEDDECIMAL_UINT32_6
        FIXEDDECIMAL_UINT32_7
        FIXEDDECIMAL_UINT32_8
        FIXEDDECIMAL_UINT64_0
        FIXEDDECIMAL_UINT64_1
        FIXEDDECIMAL_UINT64_2
        FIXEDDECIMAL_UINT64_3
        FIXEDDECIMAL_UINT64_4
        FIXEDDECIMAL_UINT64_5
        FIXEDDECIMAL_UINT64_6
        FIXEDDECIMAL_UINT64_7
        FIXEDDECIMAL_UINT64_8
        FIXEDDECIMAL_UINT128_0
        FIXEDDECIMAL_UINT128_1
        FIXEDDECIMAL_UINT128_2
        FIXEDDECIMAL_UINT128_3
        FIXEDDECIMAL_UINT128_4
        FIXEDDECIMAL_UINT128_5
        FIXEDDECIMAL_UINT128_6
        FIXEDDECIMAL_UINT128_7
        FIXEDDECIMAL_UINT128_8
    end

    to_enum(::Type{Int}) = INT
    to_enum(::Type{Bool}) = BOOL
    to_enum(::Type{Float64}) = FLOAT64
    to_enum(::Type{Dates.Date}) = DATE
    to_enum(::Type{Dates.DateTime}) = DATETIME
    to_enum(::Type{Char}) = CHAR
    to_enum(::Type{String}) = STRING
    to_enum(::Type{FixedDecimal{Int8,0}}) = FIXEDDECIMAL_INT8_0
    to_enum(::Type{FixedDecimal{Int8,1}}) = FIXEDDECIMAL_INT8_1
    to_enum(::Type{FixedDecimal{Int8,2}}) = FIXEDDECIMAL_INT8_2
    to_enum(::Type{FixedDecimal{Int16,0}}) = FIXEDDECIMAL_INT16_0
    to_enum(::Type{FixedDecimal{Int16,1}}) = FIXEDDECIMAL_INT16_1
    to_enum(::Type{FixedDecimal{Int16,2}}) = FIXEDDECIMAL_INT16_2
    to_enum(::Type{FixedDecimal{Int16,3}}) = FIXEDDECIMAL_INT16_3
    to_enum(::Type{FixedDecimal{Int16,4}}) = FIXEDDECIMAL_INT16_4
    to_enum(::Type{FixedDecimal{Int32,0}}) = FIXEDDECIMAL_INT32_0
    to_enum(::Type{FixedDecimal{Int32,1}}) = FIXEDDECIMAL_INT32_1
    to_enum(::Type{FixedDecimal{Int32,2}}) = FIXEDDECIMAL_INT32_2
    to_enum(::Type{FixedDecimal{Int32,3}}) = FIXEDDECIMAL_INT32_3
    to_enum(::Type{FixedDecimal{Int32,4}}) = FIXEDDECIMAL_INT32_4
    to_enum(::Type{FixedDecimal{Int32,5}}) = FIXEDDECIMAL_INT32_5
    to_enum(::Type{FixedDecimal{Int32,6}}) = FIXEDDECIMAL_INT32_6
    to_enum(::Type{FixedDecimal{Int32,7}}) = FIXEDDECIMAL_INT32_7
    to_enum(::Type{FixedDecimal{Int32,8}}) = FIXEDDECIMAL_INT32_8
    to_enum(::Type{FixedDecimal{Int64,0}}) = FIXEDDECIMAL_INT64_0
    to_enum(::Type{FixedDecimal{Int64,1}}) = FIXEDDECIMAL_INT64_1
    to_enum(::Type{FixedDecimal{Int64,2}}) = FIXEDDECIMAL_INT64_2
    to_enum(::Type{FixedDecimal{Int64,3}}) = FIXEDDECIMAL_INT64_3
    to_enum(::Type{FixedDecimal{Int64,4}}) = FIXEDDECIMAL_INT64_4
    to_enum(::Type{FixedDecimal{Int64,5}}) = FIXEDDECIMAL_INT64_5
    to_enum(::Type{FixedDecimal{Int64,6}}) = FIXEDDECIMAL_INT64_6
    to_enum(::Type{FixedDecimal{Int64,7}}) = FIXEDDECIMAL_INT64_7
    to_enum(::Type{FixedDecimal{Int64,8}}) = FIXEDDECIMAL_INT64_8
    to_enum(::Type{FixedDecimal{Int128,0}}) = FIXEDDECIMAL_INT128_0
    to_enum(::Type{FixedDecimal{Int128,1}}) = FIXEDDECIMAL_INT128_1
    to_enum(::Type{FixedDecimal{Int128,2}}) = FIXEDDECIMAL_INT128_2
    to_enum(::Type{FixedDecimal{Int128,3}}) = FIXEDDECIMAL_INT128_3
    to_enum(::Type{FixedDecimal{Int128,4}}) = FIXEDDECIMAL_INT128_4
    to_enum(::Type{FixedDecimal{Int128,5}}) = FIXEDDECIMAL_INT128_5
    to_enum(::Type{FixedDecimal{Int128,6}}) = FIXEDDECIMAL_INT128_6
    to_enum(::Type{FixedDecimal{Int128,7}}) = FIXEDDECIMAL_INT128_7
    to_enum(::Type{FixedDecimal{Int128,8}}) = FIXEDDECIMAL_INT128_8
    to_enum(::Type{FixedDecimal{UInt8,0}}) = FIXEDDECIMAL_UINT8_0
    to_enum(::Type{FixedDecimal{UInt8,1}}) = FIXEDDECIMAL_UINT8_1
    to_enum(::Type{FixedDecimal{UInt8,2}}) = FIXEDDECIMAL_UINT8_2
    to_enum(::Type{FixedDecimal{UInt16,0}}) = FIXEDDECIMAL_UINT16_0
    to_enum(::Type{FixedDecimal{UInt16,1}}) = FIXEDDECIMAL_UINT16_1
    to_enum(::Type{FixedDecimal{UInt16,2}}) = FIXEDDECIMAL_UINT16_2
    to_enum(::Type{FixedDecimal{UInt16,3}}) = FIXEDDECIMAL_UINT16_3
    to_enum(::Type{FixedDecimal{UInt16,4}}) = FIXEDDECIMAL_UINT16_4
    to_enum(::Type{FixedDecimal{UInt32,0}}) = FIXEDDECIMAL_UINT32_0
    to_enum(::Type{FixedDecimal{UInt32,1}}) = FIXEDDECIMAL_UINT32_1
    to_enum(::Type{FixedDecimal{UInt32,2}}) = FIXEDDECIMAL_UINT32_2
    to_enum(::Type{FixedDecimal{UInt32,3}}) = FIXEDDECIMAL_UINT32_3
    to_enum(::Type{FixedDecimal{UInt32,4}}) = FIXEDDECIMAL_UINT32_4
    to_enum(::Type{FixedDecimal{UInt32,5}}) = FIXEDDECIMAL_UINT32_5
    to_enum(::Type{FixedDecimal{UInt32,6}}) = FIXEDDECIMAL_UINT32_6
    to_enum(::Type{FixedDecimal{UInt32,7}}) = FIXEDDECIMAL_UINT32_7
    to_enum(::Type{FixedDecimal{UInt32,8}}) = FIXEDDECIMAL_UINT32_8
    to_enum(::Type{FixedDecimal{UInt64,0}}) = FIXEDDECIMAL_UINT64_0
    to_enum(::Type{FixedDecimal{UInt64,1}}) = FIXEDDECIMAL_UINT64_1
    to_enum(::Type{FixedDecimal{UInt64,2}}) = FIXEDDECIMAL_UINT64_2
    to_enum(::Type{FixedDecimal{UInt64,3}}) = FIXEDDECIMAL_UINT64_3
    to_enum(::Type{FixedDecimal{UInt64,4}}) = FIXEDDECIMAL_UINT64_4
    to_enum(::Type{FixedDecimal{UInt64,5}}) = FIXEDDECIMAL_UINT64_5
    to_enum(::Type{FixedDecimal{UInt64,6}}) = FIXEDDECIMAL_UINT64_6
    to_enum(::Type{FixedDecimal{UInt64,7}}) = FIXEDDECIMAL_UINT64_7
    to_enum(::Type{FixedDecimal{UInt64,8}}) = FIXEDDECIMAL_UINT64_8
    to_enum(::Type{FixedDecimal{UInt128,0}}) = FIXEDDECIMAL_UINT128_0
    to_enum(::Type{FixedDecimal{UInt128,1}}) = FIXEDDECIMAL_UINT128_1
    to_enum(::Type{FixedDecimal{UInt128,2}}) = FIXEDDECIMAL_UINT128_2
    to_enum(::Type{FixedDecimal{UInt128,3}}) = FIXEDDECIMAL_UINT128_3
    to_enum(::Type{FixedDecimal{UInt128,4}}) = FIXEDDECIMAL_UINT128_4
    to_enum(::Type{FixedDecimal{UInt128,5}}) = FIXEDDECIMAL_UINT128_5
    to_enum(::Type{FixedDecimal{UInt128,6}}) = FIXEDDECIMAL_UINT128_6
    to_enum(::Type{FixedDecimal{UInt128,7}}) = FIXEDDECIMAL_UINT128_7
    to_enum(::Type{FixedDecimal{UInt128,8}}) = FIXEDDECIMAL_UINT128_8
end
