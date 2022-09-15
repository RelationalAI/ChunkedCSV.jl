using FixedPointDecimals, ScanByte, Parsers, BenchmarkTools, Test


function _shift(n::T, decpos) where {T<:Int128}
    if     decpos ==   0 && return n
    elseif decpos ==   1 && return T(10) * n
    elseif decpos ==   2 && return T(100) * n
    elseif decpos ==   3 && return T(1000) * n
    elseif decpos ==   4 && return T(10000) * n
    elseif decpos ==   5 && return T(100000) * n
    elseif decpos ==   6 && return T(1000000) * n
    elseif decpos ==   7 && return T(10000000) * n
    elseif decpos ==   8 && return T(100000000) * n
    elseif decpos ==   9 && return T(1000000000) * n
    elseif decpos ==  10 && return T(10000000000) * n
    elseif decpos ==  11 && return T(100000000000) * n
    elseif decpos ==  12 && return T(1000000000000) * n
    elseif decpos ==  13 && return T(10000000000000) * n
    elseif decpos ==  14 && return T(100000000000000) * n
    elseif decpos ==  15 && return T(1000000000000000) * n
    elseif decpos ==  16 && return T(10000000000000000) * n
    elseif decpos ==  17 && return T(100000000000000000) * n
    elseif decpos ==  18 && return T(1000000000000000000) * n
    elseif decpos ==  19 && return T(10000000000000000000) * n
    elseif decpos ==  20 && return T(100000000000000000000) * n
    elseif decpos ==  21 && return T(1000000000000000000000) * n
    elseif decpos ==  22 && return T(10000000000000000000000) * n
    elseif decpos ==  23 && return T(100000000000000000000000) * n
    elseif decpos ==  24 && return T(1000000000000000000000000) * n
    elseif decpos ==  25 && return T(10000000000000000000000000) * n
    elseif decpos ==  26 && return T(100000000000000000000000000) * n
    elseif decpos ==  27 && return T(1000000000000000000000000000) * n
    elseif decpos ==  28 && return T(10000000000000000000000000000) * n
    elseif decpos ==  29 && return T(100000000000000000000000000000) * n
    elseif decpos ==  30 && return T(1000000000000000000000000000000) * n
    elseif decpos ==  31 && return T(10000000000000000000000000000000) * n
    elseif decpos ==  32 && return T(100000000000000000000000000000000) * n
    elseif decpos ==  33 && return T(1000000000000000000000000000000000) * n
    elseif decpos ==  34 && return T(10000000000000000000000000000000000) * n
    elseif decpos ==  35 && return T(100000000000000000000000000000000000) * n
    elseif decpos ==  36 && return T(1000000000000000000000000000000000000) * n
    elseif decpos ==  37 && return T(10000000000000000000000000000000000000) * n
    elseif decpos ==  38 && return T(100000000000000000000000000000000000000) * n
    elseif decpos ==  39 && return T(1000000000000000000000000000000000000000) * n
    else
        @assert false "unreachable"
    end
end

function _shift(n::T, decpos) where {T<:Int64}
    if     decpos ==   0 && return n
    elseif decpos ==   1 && return T(10) * n
    elseif decpos ==   2 && return T(100) * n
    elseif decpos ==   3 && return T(1000) * n
    elseif decpos ==   4 && return T(10000) * n
    elseif decpos ==   5 && return T(100000) * n
    elseif decpos ==   6 && return T(1000000) * n
    elseif decpos ==   7 && return T(10000000) * n
    elseif decpos ==   8 && return T(100000000) * n
    elseif decpos ==   9 && return T(1000000000) * n
    elseif decpos ==  10 && return T(10000000000) * n
    elseif decpos ==  11 && return T(100000000000) * n
    elseif decpos ==  12 && return T(1000000000000) * n
    elseif decpos ==  13 && return T(10000000000000) * n
    elseif decpos ==  14 && return T(100000000000000) * n
    elseif decpos ==  15 && return T(1000000000000000) * n
    elseif decpos ==  16 && return T(10000000000000000) * n
    elseif decpos ==  17 && return T(100000000000000000) * n
    elseif decpos ==  18 && return T(1000000000000000000) * n
    elseif decpos ==  19 && return T(10000000000000000000) * n
    else
        @assert false "unreachable"
    end
end

function _shift(n::T, decpos) where {T<:Int32}
    if     decpos ==   1 && return T(10) * n
    elseif decpos ==   2 && return T(100) * n
    elseif decpos ==   3 && return T(1000) * n
    elseif decpos ==   4 && return T(10000) * n
    elseif decpos ==   5 && return T(100000) * n
    elseif decpos ==   6 && return T(1000000) * n
    elseif decpos ==   7 && return T(10000000) * n
    elseif decpos ==   8 && return T(100000000) * n
    elseif decpos ==   9 && return T(1000000000) * n
    elseif decpos ==  10 && return T(10000000000) * n
    else
        @assert false "unreachable"
    end
end

function _shift(n::T, decpos) where {T<:Int16}
    if     decpos ==  0 && return n
    elseif decpos ==  1 && return T(10) * n
    elseif decpos ==  2 && return T(100) * n
    elseif decpos ==  3 && return T(1000) * n
    elseif decpos ==  4 && return T(10000) * n
    elseif decpos ==  5 && return T(100000) * n
    else
        @assert false "unreachable"
    end
end

function _shift(n::T, decpos, mode::RoundingMode=Base.RoundNearest) where {T<:Int8}
    if     decpos ==  1 && return T(10) * n
    elseif decpos ==  2 && return T(100) * n
    elseif decpos ==  3 && return T(1000) * n
    else
        @assert false "unreachable"
    end
end

maxdigits(::Type{Int8}) = 3
maxdigits(::Type{Int16}) = 5
maxdigits(::Type{Int32}) = 10
maxdigits(::Type{Int64}) = 18
maxdigits(::Type{Int128}) = 39

_typemaxbytes(::Type{Int8}, i) = @inbounds NTuple{3,UInt8}((0x31, 0x32, 0x37))[i]
_typemaxbytes(::Type{Int16}, i) = @inbounds NTuple{5,UInt8}((0x33, 0x32, 0x37, 0x36, 0x37))[i]
_typemaxbytes(::Type{Int32}, i) = @inbounds NTuple{10,UInt8}((0x32, 0x31, 0x34, 0x37, 0x34, 0x38, 0x33, 0x36, 0x34, 0x37))[i]
_typemaxbytes(::Type{Int64}, i) = @inbounds NTuple{18,UInt8}((0x39, 0x32, 0x32, 0x33, 0x33, 0x37, 0x32, 0x30, 0x33, 0x36, 0x38, 0x35, 0x34, 0x37, 0x37, 0x35, 0x38, 0x30, 0x37))[i]
_typemaxbytes(::Type{Int128}, i) = @inbounds NTuple{39,UInt8}((0x31, 0x37, 0x30, 0x31, 0x34, 0x31, 0x31, 0x38, 0x33, 0x34, 0x36, 0x30, 0x34, 0x36, 0x39, 0x32, 0x33, 0x31, 0x37, 0x33, 0x31, 0x36, 0x38, 0x37, 0x33, 0x30, 0x33, 0x37, 0x31, 0x35, 0x38, 0x38, 0x34, 0x31, 0x30, 0x35, 0x37, 0x32, 0x37))[i]

const Ees = Val(ByteSet((UInt8('E'), UInt8('e'))))
function decimal_base2(::Type{T}, f, buf::AbstractVector{UInt8}, options::Parsers.Options, mode::Base.RoundingMode=Base.RoundNearest) where {T<:Int32}
    is_neg = buf[1] == UInt8('-')
    int_start_offset = (is_neg || buf[1] == '+') ? 1 : 0
    int_sign = T(is_neg ? -1 : 1)

    len = length(buf)
    e_position = Int(something(memchr(buf, Ees), 0))
    last_byte_to_parse = e_position == 0 ? len : e_position

    if e_position != 0
        e_val = Int(Parsers.xparse2(Int32, buf, e_position+Int32(1), len).val)
        last_byte_to_parse -= 1
    else
        e_val = 0
    end

    decimal_position = Int(something(memchr(buf, options.decimal), 0))::Int
    if decimal_position > 0
        while buf[last_byte_to_parse] == 0x30
            last_byte_to_parse -= 1
        end
        if last_byte_to_parse == decimal_position
            decimal_position = 0
            last_byte_to_parse -= 1
        end
    end

    if buf[1+int_start_offset] == 0x30
        if len > 1 + int_start_offset
            if buf[2+int_start_offset] == options.decimal
                int_start_offset += 1
                e_val -= 1
                while true
                    1+int_start_offset > len && return T(0)
                    b = buf[1+int_start_offset]
                    if b == 0x30
                        int_start_offset += 1
                        e_val -= 1
                    elseif b == options.decimal
                        int_start_offset += 1
                        decimal_position = 0
                    else
                        break
                    end
                end
            else
                #parse error leading zero
            end
        else
            return T(0) # ok
        end
    end

    number_of_fractional_digits = -e_val
    number_of_digits = last_byte_to_parse - int_start_offset
    if decimal_position > 0
        number_of_fractional_digits += last_byte_to_parse - decimal_position
        number_of_digits -= 1
    end

    decimal_shift = f - number_of_fractional_digits

    backing_integer_digits = number_of_digits - number_of_fractional_digits + f
    parse_through_decimal = 0 < decimal_position <= backing_integer_digits
    digits_to_iter = min(backing_integer_digits, number_of_digits)

    # No overflow possible
    if backing_integer_digits < maxdigits(T)
        out = zero(T)
        for i in (1+int_start_offset:int_start_offset + digits_to_iter + parse_through_decimal)
            out = T(1 + 9 * (i != decimal_position)) * out + T(buf[i]-0x30) * (i != decimal_position)
        end

        if number_of_fractional_digits < f
            out = _shift(out, decimal_shift)
        else
            out += int_sign * _parse_round(T, buf, int_start_offset + digits_to_iter + parse_through_decimal, last_byte_to_parse, decimal_position, mode)
        end
    elseif backing_integer_digits == maxdigits(T) # maybe overflow
        out = zero(T)
        j = 0
        check_next = true
        for i in (1+int_start_offset:int_start_offset + digits_to_iter + parse_through_decimal)
            (i == decimal_position) && continue
            j += 1
            b = buf[i]
            if check_next
                maxbyte = _typemaxbytes(T, j)
                if b == maxbyte
                    check_next = true
                elseif b < maxbyte
                    check_next = false
                else
                    return T(0) # overflow
                end
            end
            out = T(10) * out + T(b-0x30)
        end
        if number_of_fractional_digits < f
            out = _shift(out, decimal_shift)
        else
            out += int_sign * _parse_round(T, buf, int_start_offset + digits_to_iter + parse_through_decimal, last_byte_to_parse, decimal_position, mode)
        end
    else # overflow
        out = T(0)
    end

    return int_sign * out
end

_parse_round(::Type{T}, buf, s, e, d, ::RoundingMode{:ToZero}) where {T} = T(0)
_parse_round(::Type{T}, buf, s, e, d, ::RoundingMode{:Throws}) where {T} = s < e && !all(==(0), view(buf, s:e)) ? error("") : T(0)
function _parse_round(::Type{T}, buf, s, e, d, ::RoundingMode{:Nearest}) where {T}
    carries_over = false
    @assert s <= length(buf)
    @assert 1 < s <= e "$s $e"
    b = buf[e] - 0x30
    s >= e && return T(b > 0x05 || (b == 0x05 && buf[s-1]))
    for i in e:-1:s+1
        i == d && continue
        pb = buf[i-1] - 0x30 + carries_over
        carries_over = false
        if b > 0x05 || (b == 0x05 && isodd(pb))
            if i - 1 == s
                return T(1)
            else
                carries_over = true
            end
        end
        b = pb
    end
    return T(0)
end


using Test

options = Parsers.Options()
@test decimal_base2(Int32, Int8(4), UInt8.(collect("0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("0.0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("0.0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("0.0e0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("0.0e+0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("0.0e-0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-0.0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-0.0e0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-0.0e+0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-0.0e-0")), options) == 0
@test decimal_base2(Int32, Int8(4), UInt8.(collect("1")), options) == 10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("1.0")), options) == 10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("1.0e+0")), options) == 10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("1.0e-0")), options) == 10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("0.1e+1")), options) == 10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("0.01e+2")), options) == 10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("0.00000000001e+11")), options) == 10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("10.0e-1")), options) == 10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("100.0e-2")), options) == 10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("100000000000.0e-11")), options) == 10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-1")), options) == -10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-1.0")), options) == -10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-1.0e+0")), options) == -10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-1.0e-0")), options) == -10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-0.1e+1")), options) == -10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-0.01e+2")), options) == -10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-0.00000000001e+11")), options) == -10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-10.0e-1")), options) == -10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-100.0e-2")), options) == -10000
@test decimal_base2(Int32, Int8(4), UInt8.(collect("-100000000000.0e-11")), options) == -10000



# # @testset "decimal position" begin
#     @test decimal_base2(Int32, 2, UInt8.(collect("123")), options)   == 123_00
#     @test decimal_base2(Int32, 2, UInt8.(collect("0.123")), options) == 0_12
#     @test decimal_base2(Int32, 2, UInt8.(collect(".123")), options)  == 0_12
#     @test decimal_base2(Int32, 2, UInt8.(collect("1.23")), options)  == 1_23
#     @test decimal_base2(Int32, 2, UInt8.(collect("12.3")), options)  == 12_30
#     @test decimal_base2(Int32, 2, UInt8.(collect("123.")), options)  == 123_00
#     @test decimal_base2(Int32, 2, UInt8.(collect("123.0")), options) == 123_00

#     @test decimal_base2(Int32, 2, UInt8.(collect("-123")), options)   == -123_00
#     @test decimal_base2(Int32, 2, UInt8.(collect("-0.123")), options) == -0_12
#     @test decimal_base2(Int32, 2, UInt8.(collect("-.123")), options)  == -0_12
#     @test decimal_base2(Int32, 2, UInt8.(collect("-1.23")), options)  == -1_23
#     @test decimal_base2(Int32, 2, UInt8.(collect("-12.3")), options)  == -12_30
#     @test decimal_base2(Int32, 2, UInt8.(collect("-123.")), options)  == -123_00
#     @test decimal_base2(Int32, 2, UInt8.(collect("-123.0")), options) == -123_00
# # end

# @testset "scientific notation" begin
#     @test parse(FD4, "12e0")   == reinterpret(FD4, 00012_0000)
#     @test parse(FD4, "12e3")   == reinterpret(FD4, 12000_0000)
#     @test parse(FD4, "12e-3")  == reinterpret(FD4, 00000_0120)
#     @test parse(FD4, "1.2e0")  == reinterpret(FD4, 00001_2000)
#     @test parse(FD4, "1.2e3")  == reinterpret(FD4, 01200_0000)
#     @test parse(FD4, "1.2e-3") == reinterpret(FD4, 00000_0012)
#     @test parse(FD4, "1.2e-4") == reinterpret(FD4, 00000_0001)

#     @test parse(FD4, "-12e0")   == reinterpret(FD4, -00012_0000)
#     @test parse(FD4, "-12e3")   == reinterpret(FD4, -12000_0000)
#     @test parse(FD4, "-12e-3")  == reinterpret(FD4, -00000_0120)
#     @test parse(FD4, "-1.2e0")  == reinterpret(FD4, -00001_2000)
#     @test parse(FD4, "-1.2e3")  == reinterpret(FD4, -01200_0000)
#     @test parse(FD4, "-1.2e-3") == reinterpret(FD4, -00000_0012)

#     @test parse(FD2, "999e-1") == reinterpret(FD2, 99_90)
#     @test parse(FD2, "999e-2") == reinterpret(FD2, 09_99)
#     @test parse(FD2, "999e-3") == reinterpret(FD2, 01_00)
#     @test parse(FD2, "999e-4") == reinterpret(FD2, 00_10)
#     @test parse(FD2, "999e-5") == reinterpret(FD2, 00_01)
#     @test parse(FD2, "999e-6") == reinterpret(FD2, 00_00)

#     @test parse(FD2, "-999e-1") == reinterpret(FD2, -99_90)
#     @test parse(FD2, "-999e-2") == reinterpret(FD2, -09_99)
#     @test parse(FD2, "-999e-3") == reinterpret(FD2, -01_00)
#     @test parse(FD2, "-999e-4") == reinterpret(FD2, -00_10)
#     @test parse(FD2, "-999e-5") == reinterpret(FD2, -00_01)
#     @test parse(FD2, "-999e-6") == reinterpret(FD2, -00_00)

#     @test parse(FD4, "9"^96 * "e-100") == reinterpret(FD4, 0_001)
# end

# @testset "round to nearest" begin
#     @test parse(FD2, "0.444") == reinterpret(FD2, 0_44)
#     @test parse(FD2, "0.445") == reinterpret(FD2, 0_44)
#     @test parse(FD2, "0.446") == reinterpret(FD2, 0_45)
#     @test parse(FD2, "0.454") == reinterpret(FD2, 0_45)
#     @test parse(FD2, "0.455") == reinterpret(FD2, 0_46)
#     @test parse(FD2, "0.456") == reinterpret(FD2, 0_46)

#     @test parse(FD2, "-0.444") == reinterpret(FD2, -0_44)
#     @test parse(FD2, "-0.445") == reinterpret(FD2, -0_44)
#     @test parse(FD2, "-0.446") == reinterpret(FD2, -0_45)
#     @test parse(FD2, "-0.454") == reinterpret(FD2, -0_45)
#     @test parse(FD2, "-0.455") == reinterpret(FD2, -0_46)
#     @test parse(FD2, "-0.456") == reinterpret(FD2, -0_46)

#     @test parse(FD2, "0.009")  == reinterpret(FD2,  0_01)
#     @test parse(FD2, "-0.009") == reinterpret(FD2, -0_01)

#     @test parse(FD4, "1.5e-4") == reinterpret(FD4, 0_0002)
# end

# @testset "round to zero" begin
#     @test parse(FD2, "0.444", RoundToZero) == reinterpret(FD2, 0_44)
#     @test parse(FD2, "0.445", RoundToZero) == reinterpret(FD2, 0_44)
#     @test parse(FD2, "0.446", RoundToZero) == reinterpret(FD2, 0_44)
#     @test parse(FD2, "0.454", RoundToZero) == reinterpret(FD2, 0_45)
#     @test parse(FD2, "0.455", RoundToZero) == reinterpret(FD2, 0_45)
#     @test parse(FD2, "0.456", RoundToZero) == reinterpret(FD2, 0_45)

#     @test parse(FD2, "-0.444", RoundToZero) == reinterpret(FD2, -0_44)
#     @test parse(FD2, "-0.445", RoundToZero) == reinterpret(FD2, -0_44)
#     @test parse(FD2, "-0.446", RoundToZero) == reinterpret(FD2, -0_44)
#     @test parse(FD2, "-0.454", RoundToZero) == reinterpret(FD2, -0_45)
#     @test parse(FD2, "-0.455", RoundToZero) == reinterpret(FD2, -0_45)
#     @test parse(FD2, "-0.456", RoundToZero) == reinterpret(FD2, -0_45)

#     @test parse(FD2, "0.009", RoundToZero)  == reinterpret(FD2, 0_00)
#     @test parse(FD2, "-0.009", RoundToZero) == reinterpret(FD2, 0_00)

#     @test parse(FD4, "1.5e-4", RoundToZero) == reinterpret(FD4, 0_0001)




