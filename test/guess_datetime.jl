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
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(reinterpret(DateTime, typemin(Int))))
        @test res.val == reinterpret(DateTime, typemin(Int))
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(reinterpret(DateTime, typemax(Int))))
        @test res.val == reinterpret(DateTime, typemax(Int))
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(reinterpret(DateTime, typemin(Int)), "Z"))
        @test res.val == reinterpret(DateTime, typemin(Int))
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(reinterpret(DateTime, typemax(Int)), "Z"))
        @test res.val == reinterpret(DateTime, typemax(Int))
        @test Parsers.ok(res.code)
    end

    @testset "overflow due to timezone offset application" begin
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(reinterpret(DateTime, typemin(Int)), "+0100"))
        @test Parsers.invalid(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(reinterpret(DateTime, typemax(Int)), "+0100"))
        @test res.val == reinterpret(DateTime, typemax(Int)) - Hour(1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(reinterpret(DateTime, typemin(Int)), "-0100"))
        @test res.val == reinterpret(DateTime, typemin(Int)) + Hour(1)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(reinterpret(DateTime, typemax(Int)), "-0100"))
        @test Parsers.invalid(res.code)

        # crossing zero due to timezone offset
        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(reinterpret(DateTime, 0), "-0100"))
        @test res.val == reinterpret(DateTime, 0) + Hour(1)
        @test Parsers.ok(res.code)

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, string(reinterpret(DateTime, 0), "+0100"))
        @test res.val == reinterpret(DateTime, 0) - Hour(1)
        @test Parsers.ok(res.code)
    end

    @testset "" begin
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

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "$(reinterpret(DateTime, typemax(Int)))01Z", rounding=RoundNearest)
        @test res.val == reinterpret(DateTime, typemax(Int))

        res = Parsers.xparse(ChunkedCSV.GuessDateTime, "$(reinterpret(DateTime, typemin(Int)))01Z", rounding=RoundNearest)
        @test res.val == reinterpret(DateTime, typemin(Int))
    end
end
