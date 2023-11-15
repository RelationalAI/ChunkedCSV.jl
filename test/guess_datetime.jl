using Dates
using ChunkedCSV
using Parsers
using Test

const DT = b"1969-07-20 20:17:00"

macro test_noalloc(e) :(@test(@allocated($(esc(e))) == 0)) end

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

res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01")
@test res.val == DateTime(2014, 1, 1)
@test Parsers.ok(res.code)

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

res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01 12:34:56.7890")
@test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
@test Parsers.ok(res.code)

res = Parsers.xparse(ChunkedCSV.GuessDateTime, "2014-01-01T12:34:56.7890")
@test res.val == DateTime(2014, 1, 1, 12, 34, 56, 789)
@test Parsers.ok(res.code)

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
