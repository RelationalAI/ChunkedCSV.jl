using Dates
using ChunkedCSV
using Parsers
using Test

const DT = b"1969-07-20 20:17:00"

macro test_noalloc(e) :(@test(@allocated($(esc(e))) == 0)) end

@testset "GuessDateTime" begin
    @testset "Parsing UTC equivalent timezones does not allocate" begin
        for tz in (b"-00", b"+00", b"-0000", b"+0000", b"-00:00", b"+00:00", b"UTC", b"GMT")
            dt = vcat(DT, tz)
            @testset "$tz" begin
                res = Parsers.xparse(ChunkedCSV.GuessDateTime, dt, 1, length(dt), Parsers.OPTIONS, DateTime)
                @test res.val == DateTime(1969, 7, 20, 20, 17)
                @test Parsers.ok(res.code)
                @test_noalloc Parsers.xparse(ChunkedCSV.GuessDateTime, dt, 1, length(dt), Parsers.OPTIONS, DateTime)
            end
        end
    end

    @testset "Datetimes with only the Date part" begin
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "0-1-1")
        @test res.val == DateTime(0, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "0-01-01")
        @test res.val == DateTime(0, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "-0-01-01")
        @test res.val == DateTime(0, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "1-01-01")
        @test res.val == DateTime(1, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "-1-01-01")
        @test res.val == DateTime(-1, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "10-01-01")
        @test res.val == DateTime(10, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "-10-01-01")
        @test res.val == DateTime(-10, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "-001-01-01")
        @test res.val == DateTime(-1, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "001-01-01")
        @test res.val == DateTime(1, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "-201-01-01")
        @test res.val == DateTime(-201, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "201-01-01")
        @test res.val == DateTime(201, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "-2014-01-01")
        @test res.val == DateTime(-2014, 1, 1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01")
        @test res.val == DateTime(2014, 1, 1)
        @test Parsers.ok(res.code)
    end

    @testset "typemin and typemax" begin
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.MIN_DATETIME))
        @test res.val == ChunkedCSV.MIN_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.MAX_DATETIME))
        @test res.val == ChunkedCSV.MAX_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.MIN_DATETIME, "Z"))
        @test res.val == ChunkedCSV.MIN_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.MAX_DATETIME, "Z"))
        @test res.val == ChunkedCSV.MAX_DATETIME
        @test Parsers.ok(res.code)
    end


    @testset "clamping" begin
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(typemax(Int32), "-12-31"))
        @test res.val == ChunkedCSV.MAX_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(typemin(Int32), "-01-01"))
        @test res.val == ChunkedCSV.MIN_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(typemax(Int32), "-12-31 23:59:59"))
        @test res.val == ChunkedCSV.MAX_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(typemin(Int32), "-01-01 00:00:00"))
        @test res.val == ChunkedCSV.MIN_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(typemax(Int32), "-12-31 23:59:59.999"))
        @test res.val == ChunkedCSV.MAX_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(typemin(Int32), "-01-01 00:00:00.000"))
        @test res.val == ChunkedCSV.MIN_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(typemax(Int32), "-12-31 23:59:59.999-0100"))
        @test res.val == ChunkedCSV.MAX_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(typemin(Int32), "-01-01 00:00:00.000+0100"))
        @test res.val == ChunkedCSV.MIN_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2147483647-12-31T23:59:59.999")
        @test res.val == ChunkedCSV.MAX_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "292277025-08-17T07:12:55.808")
        @test res.val == ChunkedCSV.MAX_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "-2147483648-01-01T00:00:00.000")
        @test res.val == ChunkedCSV.MIN_DATETIME
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "-292277024-05-15T16:47:04.191")
        @test res.val == ChunkedCSV.MIN_DATETIME
        @test Parsers.ok(res.code)
    end

    @testset "overflow" begin
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(Int(typemax(Int32))+1, "-12-31"))
        @test Parsers.invalid(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(Int(typemin(Int32))-1, "-01-01"))
        @test Parsers.invalid(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(Int(typemax(Int32))+1, "-12-31 23:59:59"))
        @test Parsers.invalid(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(Int(typemin(Int32))-1, "-01-01 00:00:00"))
        @test Parsers.invalid(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(Int(typemax(Int32))+1, "-12-31 23:59:59.999"))
        @test Parsers.invalid(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(Int(typemin(Int32))-1, "-01-01 00:00:00.000"))
        @test Parsers.invalid(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2147483648-01-01T00:00:00.000")
        @test Parsers.invalid(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "-2147483649-12-31T23:59:59.999")
        @test Parsers.invalid(res.code)

    end

    @testset "clamping due to timezone offset application" begin
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.MIN_DATETIME, "+0100"))
        @test res.val == ChunkedCSV.MIN_DATETIME # clamped
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.MIN_DATETIME + Hour(1), "+0100"))
        @test res.val == ChunkedCSV.MIN_DATETIME # exact
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.MIN_DATETIME, "-0100"))
        @test res.val == ChunkedCSV.MIN_DATETIME + Hour(1)
        @test Parsers.ok(res.code)


        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.MAX_DATETIME, "+0100"))
        @test res.val == ChunkedCSV.MAX_DATETIME - Hour(1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.MAX_DATETIME, "-0100"))
        @test res.val == ChunkedCSV.MAX_DATETIME # clamped
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.MAX_DATETIME - Hour(1), "-0100"))
        @test res.val == ChunkedCSV.MAX_DATETIME # exact
        @test Parsers.ok(res.code)

        # crossing zero due to timezone offset
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.ZERO_DATETIME, "-0100"))
        @test res.val == ChunkedCSV.ZERO_DATETIME + Hour(1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(ChunkedCSV.ZERO_DATETIME, "+0100"))
        @test res.val == ChunkedCSV.ZERO_DATETIME - Hour(1)
        @test Parsers.ok(res.code)
    end

    @testset "basic" begin
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01T12:34:56")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56.7")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 700)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01T12:34:56.7")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 700)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56.78")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 780)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01T12:34:56.78")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 780)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56.789")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01T12:34:56.789")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01T12:34:56.789Z")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01T12:34:56.789 Z")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56.7890")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01T12:34:56.7890")
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
        @test Parsers.ok(res.code)
    end

    @testset "rounding" begin
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56.78901", rounding=RoundNearest)
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01T12:34:56.78901", rounding=RoundNearest)
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56.78901Z", rounding=RoundNearest)
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01T12:34:56.78901Z", rounding=RoundNearest)
        @test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "$(ChunkedCSV.MAX_DATETIME)01Z", rounding=RoundNearest)
        @test res.val == ChunkedCSV.MAX_DATETIME

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "$(ChunkedCSV.MIN_DATETIME)01Z", rounding=RoundNearest)
        @test res.val == ChunkedCSV.MIN_DATETIME
    end

    @testset "invalid" begin
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime,    "-1-1").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime,    "0--1").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime,    "0-1-").code)

        @test Parsers.ok(     Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-31").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime,    "--12-31").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-13-31").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-32").code)

        @test Parsers.ok(     Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-31 23:59:59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime,    "--12-31 23:59:59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-13-31 23:59:59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-32 23:59:59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-31 24:59:59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-31 23:60:59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-13-31 23:59:60.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-13-31 23:59:60.1000").code)

        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000 12-31 23:59:59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12 31 23:59:59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-31x23-59:59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-31 23 59:59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-31 23:59 59.999").code)
        @test Parsers.invalid(Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-31 23:59:59 999").code)

        @test Parsers.ok(     Parsers.xparse(ChunkedCSV.GuessDateTime, "2000-12-31 23:59:59.999N").code)
    end
end
