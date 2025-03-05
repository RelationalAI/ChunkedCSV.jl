"""
    GuessDateTime

A type that implements `Parsers.typeparser` to parse various ISO8601-like formats into a `DateTime`.
If the input timestamp has a timezone information, we always convert it to UTC.

It will parse the following formats:
- `yyyy-mm-dd`
- `yyyy-mm-dd HH:MM:SS`
- `yyyy-mm-dd HH:MM:SS.s`  # where `s` is 1-3 digits, but we also support rounding to milliseconds
- `yyyy-mm-dd HH:MM:SSZ`   # where `Z` is any valid timezone
- `yyyy-mm-dd HH:MM:SS.sZ`
- `yyyy-mm-dd`
- `yyyy-mm-ddTHH:MM:SS`
- `yyyy-mm-ddTHH:MM:SS.s`
- `yyyy-mm-ddTHH:MM:SSZ`
- `yyyy-mm-ddTHH:MM:SS.sZ`

Negative years are also supported. The smallest DateTime value that can be represented is
`-292277024-05-15T16:47:04.192` and the largest is `292277025-08-17T07:12:55.807`.

Additionally, since some systems use 32 bit integers to represent years, all valid
timestamps with in the range `[-2147483648-01-01T00:00:00.000, -292277024-05-15T16:47:04.193]`
will be clamped to the minimal representable DateTime, `-292277024-05-15T16:47:04.192`, and all valid
timestamps with in the range `[292277025-08-17T07:12:55.808, 2147483647-12-31T23:59:59.999]`
will be clamped to the maximal representable DateTime, `292277025-08-17T07:12:55.807`.

# Examples:
```julia
julia> Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01")
Parsers.Result{Dates.DateTime}(code=`SUCCESS: OK | EOF `, tlen=10, val=2014-01-01T00:00:00)

julia> Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56")
Parsers.Result{Dates.DateTime}(code=`SUCCESS: OK | EOF `, tlen=19, val=2014-01-01T00:00:00)

julia> Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56.789")
Parsers.Result{Dates.DateTime}(code=`SUCCESS: OK | EOF `, tlen=23, val=2014-01-01T12:34:56.789)

julia> Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56Z")
Parsers.Result{Dates.DateTime}(code=`SUCCESS: OK | EOF `, tlen=20, val=2014-01-01T12:34:56)

julia> Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56.789Z")
Parsers.Result{Dates.DateTime}(code=`SUCCESS: OK | EOF `, tlen=24, val=2014-01-01T12:34:56.789)
```
"""
struct GuessDateTime <: Dates.TimeType; x::Dates.DateTime  end
GuessDateTime(vals...) = GuessDateTime(DateTime(vals...))
Base.convert(::Type{DateTime}, x::GuessDateTime) = x.x
Base.convert(::Type{GuessDateTime}, x::DateTime) = GuessDateTime(x)

Dates.default_format(::Type{GuessDateTime}) = Dates.default_format(Dates.DateTime)
Dates.validargs(::Type{GuessDateTime}, vals...) = Dates.validargs(Dates.DateTime, vals...)
Parsers.default_format(::Type{GuessDateTime}) = Parsers.default_format(Dates.DateTime)
Parsers.supportedtype(::Type{GuessDateTime}) = true
Parsers.returntype(::Type{GuessDateTime}) = DateTime

function _unsafe_datetime(y=0, m=1, d=1, h=0, mi=0, s=0, ms=0)
    rata = ms + 1000 * (s + 60mi + 3600h + 86400 * Dates.totaldays(y, m, d))
    return DateTime(Dates.UTM(rata))
end
function _clamped_datetime(y=0, m=1, d=1, h=0, mi=0, s=0, ms=0)
    dt = _unsafe_datetime(y, m, d, h, mi, s, ms)
    y >= 292277025 && dt < ZERO_DATETIME && return MAX_DATETIME
    y <= -292277024 && dt > ZERO_DATETIME && return MIN_DATETIME
    return dt
end

function _clamped_datetime_from_zoned(year::Int, zdt::ZonedDateTime)
    dt = DateTime(zdt, TimeZones.UTC)
    year >= 292277025 && dt < ZERO_DATETIME && return MAX_DATETIME
    year <= -292277024 && dt > ZERO_DATETIME && return MIN_DATETIME
    return dt
end

const MAX_DATETIME = DateTime(Dates.UTM(typemax(Int)))
const MIN_DATETIME = DateTime(Dates.UTM(typemin(Int)))
const ZERO_DATETIME = DateTime(Dates.UTM(0))

# [-]y{1,10}-[m]m-[d]d((T|\s)HH:MM:SS(\.s{1,3})?)?(zzzz|ZZZ|\Z)?
Base.@propagate_inbounds function _default_tryparse_timestamp(buf, pos, len, code, b, options)
    delim = options.delim.token
    cq = options.cq.token
    rounding = options.rounding
    # ensure there is enough room for at least y-m-d
    if len - pos + 1 < 5
        (b != delim) && (code |= Parsers.INVALID)
        (pos >= len) && (code |= Parsers.EOF)
        return _unsafe_datetime(0), code, pos
    end
    sign_mul = 1
    if b == UInt8('-')
        sign_mul = -1
        pos += 1
        b = buf[pos]
    end

    year = 0
    for i in 1:10
        b -= 0x30
        b > 0x09 && (return _unsafe_datetime(0), code | Parsers.INVALID, pos)
        year = Int(b) + 10 * year
        b = buf[pos += 1]
        b == UInt8('-') && break
    end
    year *= sign_mul
    if b != UInt8('-') || year > typemax(Int32) || year < typemin(Int32)
        return (_unsafe_datetime(year), code | Parsers.INVALID, pos)
    end
    b = buf[pos += 1]

    month = 0
    for _ in 1:2
        b -= 0x30
        b > 0x09 && (return _unsafe_datetime(year), code | Parsers.INVALID, pos)
        month = Int(b) + 10 * month
        b = buf[pos += 1]
        b == UInt8('-') && break
    end
    month > 12 && (return _unsafe_datetime(year), code | Parsers.INVALID, pos)
    b != UInt8('-') && (return _unsafe_datetime(year, month), code | Parsers.INVALID, pos)
    b = buf[pos += 1]

    day = 0
    for _ in 1:2
        b -= 0x30
        b > 0x09 && (return _unsafe_datetime(year, month), code | Parsers.INVALID, pos)
        day = Int(b) + 10 * day
        pos == len && (code |= Parsers.EOF; break)
        b = buf[pos += 1]
        (b == UInt8('T') ||  b == UInt8(' ')) && break
    end
    day > Dates.daysinmonth(year, month) && (return _unsafe_datetime(year, month), code | Parsers.INVALID, pos)
    if (pos >= len || (b != UInt8('T') && b != UInt8(' ')))
        if !(b == delim || b == cq)
            code |= Parsers.EOF
            pos += 1
        end
        return _clamped_datetime(year, month, day), code | Parsers.OK, pos
    end
    # ensure there is enough room for at least HH:MM:DD
    len - pos < 8 && (return _unsafe_datetime(0), code | Parsers.INVALID, len)
    b = buf[pos += 1]

    hour = 0
    for _ in 1:2
        b -= 0x30
        b > 0x09 && (return _unsafe_datetime(year, month, day), code | Parsers.INVALID, pos)
        hour = Int(b) + 10 * hour
        b = buf[pos += 1]
    end
    hour > 24 && (return _unsafe_datetime(year, month, day), code | Parsers.INVALID, pos)
    b != UInt8(':') && (return _unsafe_datetime(year, month, day, hour), code | Parsers.INVALID, pos)
    b = buf[pos += 1]

    minute = 0
    for _ in 1:2
        b -= 0x30
        b > 0x09 && (return _unsafe_datetime(year, month, day, hour), code | Parsers.INVALID, pos)
        minute = Int(b) + 10 * minute
        b = buf[pos += 1]
    end
    minute > 60 && (return _unsafe_datetime(year, month, day, hour), code | Parsers.INVALID, pos)
    b != UInt8(':') && (return _unsafe_datetime(year, month, day, hour, minute), code | Parsers.INVALID, pos)
    b = buf[pos += 1]

    second = 0
    for _ in 1:2
        b -= 0x30
        b > 0x09 && (return _unsafe_datetime(year, month, day, hour, minute), code | Parsers.INVALID, pos)
        second = Int(b) + 10 * second
        pos == len && break
        b = buf[pos += 1]
    end
    if (pos == len || b == delim || b == cq)
        code |= isnothing(Dates.validargs(DateTime, year, month, day, hour, minute, second, 0)) ? Parsers.OK : Parsers.INVALID
        if !(b == delim || b == cq)
            code |= Parsers.EOF
            pos += 1
        end
        return _clamped_datetime(year, month, day, hour, minute, second), code, pos
    end

    millisecond = 0
    if b == UInt8('.')
        i = 0
        while pos < len && ((b = (buf[pos += 1] - UInt8('0'))) <= 0x09)
            millisecond = Int(b) + 10 * millisecond
            i += 1
        end

        i == 0 && (return _unsafe_datetime(year, month, day, hour, minute, second), code | Parsers.INVALID, pos)
        i < 3 && (millisecond *= 10 ^ (3 - i))

        if i > 3
            if rounding === nothing
                d, r = divrem(millisecond, Int64(10) ^ (i - 3))
                if r != 0
                    return (_unsafe_datetime(year, month, day, hour, minute, second), code | Parsers.INEXACT, pos)
                end
                millisecond = d
            elseif rounding::RoundingMode === RoundNearest
                millisecond = div(millisecond, Int64(10) ^ (i - 3), RoundNearest)
            elseif rounding::RoundingMode === RoundNearestTiesAway
                millisecond = div(millisecond, Int64(10) ^ (i - 3), RoundNearestTiesAway)
            elseif rounding::RoundingMode === RoundNearestTiesUp
                millisecond = div(millisecond, Int64(10) ^ (i - 3), RoundNearestTiesUp)
            elseif rounding::RoundingMode === RoundToZero
                millisecond = div(millisecond, Int64(10) ^ (i - 3), RoundToZero)
            elseif rounding::RoundingMode === RoundFromZero
                millisecond = div(millisecond, Int64(10) ^ (i - 3), RoundFromZero)
            elseif rounding::RoundingMode === RoundUp
                millisecond = div(millisecond, Int64(10) ^ (i - 3), RoundUp)
            elseif rounding::RoundingMode === RoundDown
                millisecond = div(millisecond, Int64(10) ^ (i - 3), RoundDown)
            else
                throw(ArgumentError("invalid rounding mode: $rounding"))
            end
        end

        b += UInt8('0')
        if (pos == len || b == delim || b == cq)
            code |= isnothing(Dates.validargs(DateTime, year, month, day, hour, minute, second, millisecond)) ? Parsers.OK : Parsers.INVALID
            if !(b == delim || b == cq)
                code |= Parsers.EOF
                pos += 1
            end
            return _clamped_datetime(year, month, day, hour, minute, second, millisecond), code, pos
        end
    end
    b == UInt8(' ') && pos < len && (b = buf[pos += 1])
    tz, pos, code = _tryparse_timezone(buf, pos, b, len, code)
    pos >= len && (code |= Parsers.EOF)
    Parsers.invalid(code) && (return _unsafe_datetime(year, month, day, hour, minute, second, millisecond), code , pos)
    if isnothing(Dates.validargs(ZonedDateTime, year, month, day, hour, minute, second, millisecond, tz))
        code |= Parsers.OK
        if tz === _Z
            # Avoiding TimeZones.ZonedDateTime to save some allocations in case the `tz`
            # corresponds to a UTC time zone.
            return (_clamped_datetime(year, month, day, hour, minute, second, millisecond), code, pos)
        else
            dt = _clamped_datetime(year, month, day, hour, minute, second, millisecond)
            zdt = TimeZones.ZonedDateTime(dt, TimeZones.TimeZone(tz))
            return (_clamped_datetime_from_zoned(year, zdt), code, pos)
        end
    else
        return (_unsafe_datetime(0), code | Parsers.INVALID, pos)
    end
end

# To avoid allocating a string, we reuse this constant for all UTC equivalent timezones
# (SubString is what we get from Parsers.tryparsenext when parsing timezones)
# This is needed until https://github.com/JuliaTime/TimeZones.jl/issues/271 is fixed
const _Z = SubString("Z", 1:1)
@inline function _tryparse_timezone(buf, pos, b, len, code)
    nb = len - pos
    @inbounds if b == UInt8('+') || b == UInt8('-')
        if nb > 1 && buf[pos+1] == UInt8('0')
            if buf[pos+2] == UInt8('0')
                if nb == 2
                    return (_Z, pos+3, code) # [+-]00
                elseif nb > 4 && buf[pos+3] == UInt8(':') && buf[pos+4] == UInt8('0') && buf[pos+5] == UInt8('0')
                    return (_Z, pos+6, code) # [+-]00:00
                elseif nb > 3 && buf[pos+3] == UInt8('0') && buf[pos+4] == UInt8('0')
                    return (_Z, pos+5, code) # [+-]0000
                elseif buf[pos+3] - UInt8('0') > 0x09
                    return (_Z, pos+3, code) # [+-]00
                end
            end
        end
        (tz, pos, _, code) = Parsers.tryparsenext(Dates.DatePart{'z'}(4, false), buf, pos, len, b, code)
        return tz, pos, code
    end

    @inbounds if b == UInt8('G')
        if nb > 1 && buf[pos+1] == UInt8('M') && buf[pos+2] == UInt8('T')
            return (_Z, pos+3, code) # GMT
        end
    elseif b == UInt8('z') || b == UInt8('Z')
        return (_Z, pos+1, code)     # [Zz]
    elseif b == UInt8('U')
        if nb > 1 && buf[pos+1] == UInt8('T') && buf[pos+2] == UInt8('C')
            return (_Z, pos+3, code) # UTC
        end
    end
    (tz, pos, _, code) = Parsers.tryparsenext(Dates.DatePart{'Z'}(3, false), buf, pos, len, b, code)
    return tz, pos, code
end

function Parsers.typeparser(::Parsers.AbstractConf{GuessDateTime}, source::AbstractVector{UInt8}, pos, len, b, code, pl, options)
    if isnothing(options.dateformat)
        (x, code, pos) = @inbounds _default_tryparse_timestamp(source, pos, len, code, b, options)
        return (pos, code, Parsers.PosLen(pl.pos, pos - pl.pos), x)
    else
        return Parsers.typeparser(Dates.DateTime, source, pos, len, b, code, pl, options)
    end
end
