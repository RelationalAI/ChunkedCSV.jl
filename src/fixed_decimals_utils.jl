@inline function _shift(n::T, decpos) where {T<:Union{UInt128,Int128}}
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

@inline function _shift(n::T, decpos) where {T<:Union{UInt64,Int64}}
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
    else
        @assert false "unreachable"
    end
end

@inline function _shift(n::T, decpos) where {T<:Union{UInt32,Int32}}
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

@inline function _shift(n::T, decpos) where {T<:Union{UInt16,Int16}}
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

@inline function _shift(n::T, decpos) where {T<:Union{UInt8,Int8}}
    if     decpos ==  1 && return T(10) * n
    elseif decpos ==  2 && return T(100) * n
    elseif decpos ==  3 && return T(1000) * n
    else
        @assert false "unreachable"
    end
end

maxdigits(::Type{Int8}) = 3
maxdigits(::Type{UInt8}) = 3
maxdigits(::Type{Int16}) = 5
maxdigits(::Type{UInt16}) = 5
maxdigits(::Type{Int32}) = 10
maxdigits(::Type{UInt32}) = 10
maxdigits(::Type{Int64}) = 19
maxdigits(::Type{UInt64}) = 20
maxdigits(::Type{Int128}) = 39
maxdigits(::Type{UInt128}) = 39

_typemaxbytes(::Type{Int8}, i, is_neg) = @inbounds NTuple{3,UInt8}((0x31, 0x32, 0x37))[i] + ((i == 3) * is_neg)
_typemaxbytes(::Type{Int16}, i, is_neg) = @inbounds NTuple{5,UInt8}((0x33, 0x32, 0x37, 0x36, 0x37))[i] + ((i == 5) * is_neg)
_typemaxbytes(::Type{Int32}, i, is_neg) = @inbounds NTuple{10,UInt8}((0x32, 0x31, 0x34, 0x37, 0x34, 0x38, 0x33, 0x36, 0x34, 0x37))[i] + ((i == 10) * is_neg)
_typemaxbytes(::Type{Int64}, i, is_neg) = @inbounds NTuple{19,UInt8}((0x39, 0x32, 0x32, 0x33, 0x33, 0x37, 0x32, 0x30, 0x33, 0x36, 0x38, 0x35, 0x34, 0x37, 0x37, 0x35, 0x38, 0x30, 0x37))[i] + ((i == 19) * is_neg)
_typemaxbytes(::Type{Int128}, i, is_neg) = @inbounds NTuple{39,UInt8}((0x31, 0x37, 0x30, 0x31, 0x34, 0x31, 0x31, 0x38, 0x33, 0x34, 0x36, 0x30, 0x34, 0x36, 0x39, 0x32, 0x33, 0x31, 0x37, 0x33, 0x31, 0x36, 0x38, 0x37, 0x33, 0x30, 0x33, 0x37, 0x31, 0x35, 0x38, 0x38, 0x34, 0x31, 0x30, 0x35, 0x37, 0x32, 0x37))[i] + ((i == 39) * is_neg)
_typemaxbytes(::Type{UInt8}, i, is_neg) = @inbounds NTuple{3,UInt8}((0x32, 0x35, 0x35))[i]
_typemaxbytes(::Type{UInt16}, i, is_neg) = @inbounds NTuple{5,UInt8}((0x36, 0x35, 0x35, 0x33, 0x35))[i]
_typemaxbytes(::Type{UInt32}, i, is_neg) = @inbounds NTuple{10,UInt8}((0x34, 0x32, 0x39, 0x34, 0x39, 0x36, 0x37, 0x32, 0x39, 0x35))[i]
_typemaxbytes(::Type{UInt64}, i, is_neg) = @inbounds NTuple{19,UInt8}((0x31, 0x38, 0x34, 0x34, 0x36, 0x37, 0x34, 0x34, 0x30, 0x37, 0x33, 0x37, 0x30, 0x39, 0x35, 0x35, 0x31, 0x36, 0x31, 0x35))[i]
_typemaxbytes(::Type{UInt128}, i, is_neg) = @inbounds NTuple{39,UInt8}((0x33, 0x34, 0x30, 0x32, 0x38, 0x32, 0x33, 0x36, 0x36, 0x39, 0x32, 0x30, 0x39, 0x33, 0x38, 0x34, 0x36, 0x33, 0x34, 0x36, 0x33, 0x33, 0x37, 0x34, 0x36, 0x30, 0x37, 0x34, 0x33, 0x31, 0x37, 0x36, 0x38, 0x32, 0x31, 0x31, 0x34, 0x35, 0x35))[i]

# Parsers.jl typically process a byte at a time but for Decimals we want to know
# where the decimal point and `e` is so we can avoid overflows. E.g. in 123456789123456789.1e-10
# we don't have to materialize the whole 123456789123456789 since we know
# that there are 12345679 digits before the decimal point, the rest of the digits can be
# rounded if needed.
function _dec_grp_exp_end(buf, pos, len, b, code, options)
    decimal_position = 0
    ngroupmarks = 0
    exp_position = 0

    groupmark = options.groupmark
    has_groupmark = !isnothing(groupmark)
    delim = options.delim.token
    cq = options.cq.token
    decimal = options.decimal
    found_any_number = false

    @inbounds while true
        if b == UInt8('e') || b == UInt8('E')
            if !(pos < len)
                code |= Parsers.INVALID | Parsers.EOF
                break
            end
            if !found_any_number || exp_position != 0 || ngroupmarks != 0
                code |= Parsers.INVALID
                break
            end
            exp_position = pos

            pos += 1
            b = buf[pos]
            if b == UInt8('-') || b == UInt8('+')
                if !(pos < len)
                    code |= Parsers.INVALID | Parsers.EOF
                    break
                end
                pos += 1
                b = buf[pos]
            end

            if b - UInt8('0') > 0x09
                code |= Parsers.INVALID
                break
            end
        elseif b == decimal
            if decimal_position != 0 || exp_position != 0
                code |= Parsers.INVALID
                break
            end
            decimal_position = pos
        elseif has_groupmark && b == groupmark
            if !(pos < len)
                code |= Parsers.INVALID | Parsers.EOF
                break
            end
            if decimal_position != 0 || exp_position != 0
                code |= Parsers.INVALID
                break
            end
            pos += 1
            b = buf[pos]
            if b - UInt8('0') > 0x09
                code |= Parsers.INVALID
                break
            end
            found_any_number = true
            ngroupmarks += 1
        elseif b == delim || (Parsers.quoted(code) && b == cq)
            break
        elseif UInt8('0') <= b <= UInt8('9')
            found_any_number = true
        elseif b in (UInt8('\n'), UInt8('\r'))
            break
        else
            code |= Parsers.INVALID
            break
        end

        if pos == len
            code |= Parsers.EOF
            break
        else
            pos += 1
            b = buf[pos]
        end
    end
    if !found_any_number
        code |= Parsers.INVALID
    end
    return decimal_position, ngroupmarks, exp_position, pos, code
end


Base.@propagate_inbounds _typeparser(::Type{T}, f, buf::AbstractString, pos, len, b, code, options::Parsers.Options, mode::Base.RoundingMode=Base.RoundNearest) where {T<:Union{Int8,Int16,Int32,Int64,Int128,UInt8,UInt16,UInt32,UInt64,UInt128}} =
    _typeparser(T, f, codeunits(buf), pos, len, b, code, options, mode)
Base.@propagate_inbounds function _typeparser(::Type{T}, f, buf::AbstractVector{UInt8}, pos, len, b, code, options::Parsers.Options, mode::Base.RoundingMode=Base.RoundNearest) where {T<:Union{Int8,Int16,Int32,Int64,Int128,UInt8,UInt16,UInt32,UInt64,UInt128}}
    decimal = options.decimal

    is_neg = b == UInt8('-')
    if (is_neg || b == '+')
        pos += 1
    end
    if pos > len
        code |= Parsers.EOF | Parsers.INVALID
        return (T(0), code, pos)
    end

    int_sign = T(is_neg ? -1 : 1)
    b = buf[pos]

    decimal_position, ngroupmarks, exp_position, field_end, code = _dec_grp_exp_end(buf, pos, len, b, code, options)
    Parsers.invalid(code) && return (T(0), code, pos)

    # if we got here, we can safely ignore all bytes that == groupmark, these also must appear before decimal_position and exp_position
    groupmark = something(options.groupmark, 0xff)

    last_byte_to_parse = max(pos, exp_position == 0 ? field_end - !Parsers.eof(code) - Parsers.quoted(code) : exp_position - 1)
    if exp_position != 0
        exp_result = Parsers.xparse(Int, buf, exp_position+1, field_end, options)
        exp_val = exp_result.val
        !Parsers.ok(exp_result.code) && return (T(0), code |= exp_result.code, field_end)
    else
        exp_val = 0
    end

    parse_through_decimal = decimal_position != 0
    has_digits_past_decimal = parse_through_decimal
    number_of_fractional_digits = 0
    if has_digits_past_decimal
        # Remove trailing zeros after decimal
        while buf[last_byte_to_parse] == UInt8('0')
            last_byte_to_parse -= 1
        end
        if last_byte_to_parse == decimal_position
            parse_through_decimal = false
            has_digits_past_decimal = false
            last_byte_to_parse -= 1
        end
    end

    # Remove leading zeros in 0.00..x
    if field_end > pos && buf[pos] == UInt8('0')
        if field_end > 1 + pos
            if buf[1+pos] == decimal
                pos += 1
                while true
                    1+pos > last_byte_to_parse && return (T(0), code | Parsers.OK, field_end) # This was something like "0.00"
                    b = buf[1+pos]
                    if b == UInt8('0')
                        pos += 1
                    elseif b == decimal
                        pos += 1
                        parse_through_decimal = false
                    else
                        break
                    end
                end
            else
                #parse error leading zero 00.x...
                return (T(0), code | Parsers.INVALID, field_end)
            end
        else
            return (T(0), code | Parsers.OK, field_end) # ok
        end
    end

    number_of_fractional_digits -= exp_val
    number_of_digits = last_byte_to_parse - pos + 1 - parse_through_decimal
    if has_digits_past_decimal
        number_of_fractional_digits += last_byte_to_parse - decimal_position
    end

    decimal_shift = f - number_of_fractional_digits

    backing_integer_digits = number_of_digits - number_of_fractional_digits + f - ngroupmarks
    digits_to_iter = min(backing_integer_digits, number_of_digits)

    if backing_integer_digits < 0 # All digits are past our precision, no overflow possible
        return (T(0), code | Parsers.OK, field_end)
    elseif backing_integer_digits == 0 # All digits are past our precision but we may get a 1 from rounding, no overflow possible
        out = T(0)
        if number_of_fractional_digits != f
            round_val, code = _parse_round(T, buf, pos + parse_through_decimal, last_byte_to_parse, decimal_position, code, mode)
            out += int_sign * round_val
        end
    elseif backing_integer_digits < maxdigits(T) # The number of digits to accumulate is smaller than the capacity of T, no overflow possible
        out = zero(T)
        for i in (pos:pos - 1 + digits_to_iter + parse_through_decimal + ngroupmarks)
            (i == decimal_position) && continue
            b = buf[i]
            b == groupmark && continue
            out = T(10) * out + T(b - UInt8('0'))
        end
        out *= int_sign
        if number_of_fractional_digits < f
            out = _shift(out, decimal_shift)
        elseif number_of_fractional_digits != f
            round_val, code = _parse_round(T, buf, pos + digits_to_iter + parse_through_decimal + ngroupmarks, last_byte_to_parse, decimal_position, code, mode)
            out += int_sign * round_val
        end
    elseif backing_integer_digits == maxdigits(T) # The number of digits to accumulate is the same as the capacity of T, overflow may happen
        out = zero(T)
        j = 0
        check_next = true
        for i in (pos:pos - 1 + digits_to_iter + parse_through_decimal + ngroupmarks)
            (i == decimal_position) && continue
            b = buf[i]
            b == groupmark && continue
            j += 1
            if check_next
                maxbyte = _typemaxbytes(T, j, is_neg)
                if b == maxbyte
                    check_next = true
                elseif b < maxbyte
                    check_next = false
                else
                    return (out, code | Parsers.OVERFLOW, field_end)
                end
            end
            out = T(10) * out + T(b-UInt8('0'))
        end
        out *= int_sign

        if number_of_fractional_digits < f
            out = _shift(out, decimal_shift)
        elseif number_of_fractional_digits != f
            round_val, code = _parse_round(T, buf, pos - 1 + digits_to_iter + parse_through_decimal + ngroupmarks, last_byte_to_parse, decimal_position, code, mode)
            out += int_sign * round_val
        end
    else # Always overflows
        return (T(0), code | Parsers.OVERFLOW, field_end)
    end
    return (out, code | Parsers.OK, field_end)
end


# Add 1 or 0 to our integer depending on the rounding mode and trailing bytes (that we din't use because the precision of our decimal is not high enough)
_parse_round(::Type{T}, buf, s, e, d, code, ::RoundingMode{:ToZero}) where {T} = (T(0), code)
_parse_round(::Type{T}, buf, s, e, d, code, ::RoundingMode{:Throws}) where {T} = s < e && !all(==(UInt8('0')), view(buf, s:e)) ? (T(0), code | Parsers.INVALID) : (T(0), code)
function _parse_round(::Type{T}, buf, s, e, d, code, ::RoundingMode{:Nearest}) where {T}
    @assert s <= length(buf) "$s $e"
    @assert 1 < s <= e || 1 <= s < e  "$s $length(buf)"
    @inbounds begin
        if e == s
            b = buf[e] - UInt8('0')
            b > 0x09 && (return (T(0), code | Parsers.INVALID))
            b > 0x05 && (return (T(1), code))
            b == 0x05 && isodd(buf[e-1-((e-1)==d)]) && (return (T(1), code))
            return (T(0), code)
        end
        carries_over = false
        prev_b = 0x00
        for i in e:-1:s
            i == d && continue
            b = buf[i] - UInt8('0')
            b > 0x09 && (return (T(0), code | Parsers.INVALID))
            b += carries_over
            carries_over = (b > 0x05 || (b == 0x05 && isodd(prev_b)))
            prev_b = b
        end
        return (T(carries_over), code)
    end
end


# FixedDecimal is not a subtype of Integer or AbstractFloat, so xparse won't put it on the fast path
struct _FixedDecimal{T<:Integer,f} <: Integer
    x::FixedDecimal{T,f}
    _FixedDecimal{T, f}(x::FixedDecimal{T, f}) where {T<:Integer, f} = new{T,f}(x)
end

function Parsers.typeparser(::Type{_FixedDecimal{T,f}}, source::AbstractVector{UInt8}, pos, len, b, code, pl, options) where {T<:Integer,f}
    @inbounds x, code, pos = _typeparser(T, f, source, pos, len, b, code, options, RoundNearest)
    # We need to step one past field_end on EOF so that Parsers.jl recognize it's not a bad delim
    # but only when we didn't end up on an empty field?
    Parsers.eof(code) && !Parsers.invalid(code) && (pos += 1)
    return pos, code, Parsers.PosLen(pl.pos, pos - pl.pos), _FixedDecimal{T, f}(reinterpret(FixedDecimal{T,f}, x))
end
