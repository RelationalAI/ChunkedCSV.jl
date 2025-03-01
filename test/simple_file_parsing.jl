using Test
using ChunkedCSV
using ChunkedCSV: RowStatus
import Parsers
using UUIDs
using Dates
using FixedPointDecimals
using CodecZlibNG
using ChunkedCSV: TestContext, GuessDateTime, Enums


const _EXISTING_TEST_FILES = Dict{String,String}()
const _EXISTING_TEST_FILES_GZ = Dict{String,String}()

iobuffer(x::String) = IOBuffer(x)

function iostream(x::String)
    path = get!(_EXISTING_TEST_FILES, x) do
        (path, io) = mktemp()
        write(io, x)
        close(io)
        return path
    end
    return path
end

function gzip_stream(x::String)
    path = get!(_EXISTING_TEST_FILES_GZ, x) do
        (path, io) = mktemp()
        write(io, GzipCompressorStream(IOBuffer(x)))
        close(io)
        return path
    end
    return path
end

alg=:serial
sentinel=""
io_t = iobuffer
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Simple file single chunk ($(io_t), $(alg))" begin
        @testset "int" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                1,2,3
                3,4,4
                """),
                [Int,Int,Int],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b, :c]
            @test testctx.schema == [Int, Int, Int]
            @test testctx.results[1].cols[1] == [1,3]
            @test testctx.results[1].cols[2] == [2,4]
            @test testctx.results[1].cols[3] == [3,4]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2
        end

        @testset "float" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b
                1.0,2.0
                3.0,4.0
                """),
                [Float64, Float64],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Float64, Float64]
            @test testctx.results[1].cols[1] == [1.0,3.0]
            @test testctx.results[1].cols[2] == [2.0,4.0]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
        end

        @testset "decimal" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                1.0,-2e-2,1.000924
                2,3.3e-1,1.00345
                """),
                [FixedDecimal{Int32,2},FixedDecimal{Int64,3},FixedDecimal{UInt128,2}],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b, :c]
            @test testctx.schema == [FixedDecimal{Int32,2},FixedDecimal{Int64,3}, FixedDecimal{UInt128,2}]
            @test testctx.results[1].cols[1] == [FixedDecimal{Int32,2}(1.0), FixedDecimal{Int32,2}(2)]
            @test testctx.results[1].cols[2] == [FixedDecimal{Int64,2}(-2e-2), FixedDecimal{Int64,2}(3.3e-1)]
            @test testctx.results[1].cols[3] == [FixedDecimal{UInt128,2}(1.0), FixedDecimal{UInt128,2}(1.0)]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2
        end

        @testset "date" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b
                0-01-01,1600-02-29
                9999-12-31,1904-02-29
                """),
                [Date,Date],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Date, Date]
            @test testctx.results[1].cols[1] == [Date(0, 1, 1), Date(9999, 12, 31)]
            @test testctx.results[1].cols[2] == [Date(1600, 2, 29), Date(1904, 2, 29)]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
        end

        @testset "guess datetime" begin
            testctx = TestContext()
            parse_file(io_t("""
                id,a,b,c
                1,2000-01-01T10:20:30GMT,1969-07-20,1969-07-20
                2,2000-01-01T10:20:30Z,1969-07-20 00:00:00,1969-07-20 00:00:00
                3,2000-01-01T10:20:30,1969-07-20 00:00:00.0,1969-07-20 00:00:00.0
                4,2000-01-01 10:20:30Z,1969-07-20 00:00:00.00,1969-07-20 00:00:00.00
                5,2000-01-01 10:20:30,1969-07-20 00:00:00.000,1969-07-20 00:00:00.000
                6,2000-01-01 10:20:30,1969-07-20 00:00:00.000UTC,1969-07-20 00:00:00.000UTC
                7,2000-01-01T10:20:30+0000,1969-07-20 00:00:00.000+0000,1969-07-20 00:00:00.000+0000
                8,2000-01-01T10:20:30UTC,1969-07-19 17:00:00.000-0700,1969-07-19 17:00:00.000-0700
                9,2000-01-01 10:20:30+00:00,1969-07-19 17:00:00.000America/Los_Angeles,1969-07-19 17:00:00.000America/Los_Angeles
                10,2000-01-01 10:20:30UTC,1969-07-20 09:00:00.000+0900,1969-07-20 09:00:00.000+0900
                11,2000-01-01 02:20:30-0800,1969-07-20 09:00:00.000 Asia/Tokyo,1969-07-20 09:00:00.000 Asia/Tokyo
                12,2000-01-01 10:20:30GMT,1969-07-20 00:00:00.000Z,1969-07-20 00:00:00.000Z
                13,2000-01-01 10:20:30+00,1969-07-20 00:00:00.00-0000,1969-07-20 00:00:00.00-0000
                14,2000-01-01 10:20:30-00,1969-07-20 00:00:00.00-00:00,1969-07-20 00:00:00.00-00:00
                """),
                [Int,GuessDateTime,GuessDateTime,GuessDateTime],
                testctx,
                force=alg,
            )
            @test testctx.header == [:id, :a, :b, :c]
            @test testctx.schema == [Int, DateTime,DateTime,DateTime]
            @test testctx.results[1].cols[1] == 1:14
            @test testctx.results[1].cols[2] == fill(DateTime(2000,1,1,10,20,30), 14)
            @test testctx.results[1].cols[3] == fill(DateTime(1969,7,20,00,00,00), 14)
            @test testctx.results[1].cols[4] == fill(DateTime(1969,7,20,00,00,00), 14)
            @test length(testctx.results[1].cols[1]) == 14
            @test length(testctx.results[1].cols[2]) == 14
            @test length(testctx.results[1].cols[3]) == 14
        end

        @testset "datetime" begin
            testctx = TestContext()
            parse_file(io_t("""
                id,a,b,c
                1,2000-01-01,2000-01-01T10:20:30,2000-01-01T10:20:30.400
                2,2000-01-01,2000-01-01T10:20:30,2000-01-01T10:20:30.400
                3,2000-01-01,2000-01-01T10:20:30,2000-01-01T10:20:30.400
                """),
                [Int,DateTime,DateTime,DateTime],
                testctx,
                force=alg,
            )
            @test testctx.header == [:id, :a, :b, :c]
            @test testctx.schema == [Int, DateTime,DateTime,DateTime]
            @test testctx.results[1].cols[1] == 1:3
            @test testctx.results[1].cols[2] == fill(DateTime(2000,1,1), 3)
            @test testctx.results[1].cols[3] == fill(DateTime(2000,1,1,10,20,30), 3)
            @test testctx.results[1].cols[4] == fill(DateTime(2000,1,1,10,20,30,400), 3)
            @test length(testctx.results[1].cols[1]) == 3
            @test length(testctx.results[1].cols[2]) == 3
            @test length(testctx.results[1].cols[3]) == 3

            testctx = TestContext()
            parse_file(io_t("""
                id,a
                1,2000-01-01 10:20:30.400
                2,2000-01-01 10:20:30.400
                3,2000-01-01 10:20:30.400
                """),
                [Int,DateTime],
                testctx,
                force=alg,
                dateformat=Dates.dateformat"yyyy-mm-dd HH:MM:SS.sss",
            )
            @test testctx.header == [:id, :a]
            @test testctx.schema == [Int, DateTime]
            @test testctx.results[1].cols[1] == 1:3
            @test testctx.results[1].cols[2] == fill(DateTime(2000,1,1,10,20,30,400), 3)
            @test length(testctx.results[1].cols[1]) == 3
            @test length(testctx.results[1].cols[2]) == 3
        end

        @testset "string" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b
                "1","2"
                "3","4"
                """),
                [String,String],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31]
            @test testctx.results[1].cols[1] == [Parsers.PosLen31(6, 1), Parsers.PosLen31(14, 1)]
            @test testctx.results[1].cols[2] == [Parsers.PosLen31(10, 1), Parsers.PosLen31(18, 1)]
            @test testctx.strings[1][1] == ["1", "3"]
            @test testctx.strings[1][2] == ["2", "4"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2

            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                "","",""
                """),
                [String,String,String],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b, :c]
            @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31, Parsers.PosLen31]
            # https://github.com/JuliaData/Parsers.jl/issues/138
            @test testctx.results[1].cols[1] == [Parsers.PosLen31(8,0)]
            @test testctx.results[1].cols[2] == [Parsers.PosLen31(11,0)]
            @test testctx.results[1].cols[3] == [Parsers.PosLen31(14,0)]
            @test testctx.strings[1][1] == [""]
            @test testctx.strings[1][2] == [""]
            @test testctx.strings[1][3] == [""]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
            @test length(testctx.results[1].cols[3]) == 1
            @test testctx.results[1].row_statuses[1] == RowStatus.Ok

            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                ,,"""),
                [String,String,String],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b, :c]
            @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31, Parsers.PosLen31]
            # https://github.com/JuliaData/Parsers.jl/issues/138
            @test testctx.results[1].cols[1] == [Parsers.PosLen31(7,0)]
            @test testctx.results[1].cols[2] == [Parsers.PosLen31(8,0)]
            @test testctx.results[1].cols[3] == [Parsers.PosLen31(9,0)]
            @test testctx.strings[1][1] == [""]
            @test testctx.strings[1][2] == [""]
            @test testctx.strings[1][3] == [""]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
            @test length(testctx.results[1].cols[3]) == 1
            @test testctx.results[1].row_statuses[1] == RowStatus.MissingValues
            @test vec(collect(testctx.results[1].missing_values)) == [true, true, true]
        end

        @testset "char" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                "a","b","c"
                a,b,c
                """),
                [Char,Char,Char],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b, :c]
            @test testctx.schema == [Char, Char, Char]
            @test testctx.results[1].cols[1][1] == 'a'
            @test testctx.results[1].cols[2][1] == 'b'
            @test testctx.results[1].cols[3][1] == 'c'
            @test testctx.results[1].cols[1][2] == 'a'
            @test testctx.results[1].cols[2][2] == 'b'
            @test testctx.results[1].cols[3][2] == 'c'
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2
        end

        @testset "bool" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b
                true,false
                True,False
                t,f
                T,F
                1,0
                "true","false"
                "True","False"
                "t","f"
                "T","F"
                "1","0"
                """),
                [Bool,Bool],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Bool, Bool]
            @test testctx.results[1].cols[1] == fill(true, 10)
            @test testctx.results[1].cols[2] == fill(false, 10)
            @test length(testctx.results[1].cols[1]) == 10
            @test length(testctx.results[1].cols[2]) == 10
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Simple file, multiple chunks ($(io_t), $(alg))" begin
        @testset "$alg int" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b
                1,2
                3,4
                """),
                [Int,Int],
                empty!(testctx),
                buffersize=6,
                force=alg,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1] == [1]
            @test testctx.results[1].cols[2] == [2]
            @test testctx.results[2].cols[1] == [3]
            @test testctx.results[2].cols[2] == [4]
            @test length(testctx.results) == 2
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
            @test length(testctx.results[2].cols[1]) == 1
            @test length(testctx.results[2].cols[2]) == 1
        end

        @testset "$alg float" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b
                1.0,2.0
                3.0,4.0
                """),
                [Float64, Float64],
                testctx,
                buffersize=10,
                force=alg,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Float64, Float64]
            @test testctx.results[1].cols[1] == [1.0]
            @test testctx.results[1].cols[2] == [2.0]
            @test testctx.results[2].cols[1] == [3.0]
            @test testctx.results[2].cols[2] == [4.0]
            @test length(testctx.results) == 2
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
            @test length(testctx.results[2].cols[1]) == 1
            @test length(testctx.results[2].cols[2]) == 1
        end

        @testset "$alg string" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b
                "1","2"
                "3","4"
                """),
                [String,String],
                testctx,
                buffersize=8,
                force=alg,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31]
            @test testctx.results[1].cols[1] == [Parsers.PosLen31(2, 1)]
            @test testctx.results[1].cols[2] == [Parsers.PosLen31(6, 1)]
            @test testctx.results[2].cols[1] == [Parsers.PosLen31(2, 1)]
            @test testctx.results[2].cols[2] == [Parsers.PosLen31(6, 1)]
            @test testctx.strings[1][1] == ["1"]
            @test testctx.strings[1][2] == ["2"]
            @test testctx.strings[2][1] == ["3"]
            @test testctx.strings[2][2] == ["4"]
            @test length(testctx.results) == 2
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
            @test length(testctx.results[2].cols[1]) == 1
            @test length(testctx.results[2].cols[2]) == 1
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Skipping rows ($(io_t), $(alg))" begin
        @testset "string" begin
            testctx = TestContext()
            parse_file(io_t("""
                xxxxxx
                xxxxxx
                xxxxxx
                xxxxxx
                a,b
                "1","2"
                "3","4"
                "5","6"
                """),
                [String,String],
                testctx,
                buffersize=8,
                force=alg,
                header=5,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31]
            @test testctx.results[1].cols[1][1] == Parsers.PosLen31(2, 1)
            @test testctx.results[1].cols[2][1] == Parsers.PosLen31(6, 1)
            @test testctx.results[2].cols[1][1] == Parsers.PosLen31(2, 1)
            @test testctx.results[2].cols[2][1] == Parsers.PosLen31(6, 1)
            @test testctx.results[3].cols[1][1] == Parsers.PosLen31(2, 1)
            @test testctx.results[3].cols[2][1] == Parsers.PosLen31(6, 1)
            @test testctx.strings[1][1][1] == "1"
            @test testctx.strings[1][2][1] == "2"
            @test testctx.strings[2][1][1] == "3"
            @test testctx.strings[2][2][1] == "4"
            @test testctx.strings[3][1][1] == "5"
            @test testctx.strings[3][2][1] == "6"
            @test length(testctx.results) == 3
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
            @test length(testctx.results[2].cols[1]) == 1
            @test length(testctx.results[2].cols[2]) == 1
            @test length(testctx.results[3].cols[1]) == 1
            @test length(testctx.results[3].cols[2]) == 1
        end

        @testset "int" begin
            testctx = TestContext()
            parse_file(io_t("""
                xxx
                xxx
                xxx
                xxx
                a,b
                1,2
                3,4
                5,6
                """),
                [Int,Int],
                testctx,
                buffersize=4,
                force=alg,
                header=5,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1][1] == 1
            @test testctx.results[1].cols[2][1] == 2
            @test testctx.results[2].cols[1][1] == 3
            @test testctx.results[2].cols[2][1] == 4
            @test testctx.results[3].cols[1][1] == 5
            @test testctx.results[3].cols[2][1] == 6
            @test length(testctx.results) == 3
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
            @test length(testctx.results[2].cols[1]) == 1
            @test length(testctx.results[2].cols[2]) == 1
            @test length(testctx.results[3].cols[1]) == 1
            @test length(testctx.results[3].cols[2]) == 1
        end

        @testset "float" begin
            testctx = TestContext()
            parse_file(io_t("""
                xxxxxx
                xxxxxx
                xxxxxx
                xxxxxx
                a,b
                1.0,2.0
                3.0,4.0
                5.0,6.0
                """),
                [Float64,Float64],
                testctx,
                buffersize=8,
                force=alg,
                header=5,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Float64, Float64]
            @test testctx.results[1].cols[1][1] == 1.0
            @test testctx.results[1].cols[2][1] == 2.0
            @test testctx.results[2].cols[1][1] == 3.0
            @test testctx.results[2].cols[2][1] == 4.0
            @test testctx.results[3].cols[1][1] == 5.0
            @test testctx.results[3].cols[2][1] == 6.0
            @test length(testctx.results) == 3
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
            @test length(testctx.results[2].cols[1]) == 1
            @test length(testctx.results[2].cols[2]) == 1
            @test length(testctx.results[3].cols[1]) == 1
            @test length(testctx.results[3].cols[2]) == 1
        end

        @testset "limit" begin
            testctx = TestContext()
            parse_file(io_t("""
                1,2
                3,4
                """),
                [Int,Int],
                testctx,
                limit=1,
                header=false,
                force=alg,
            )
            @test testctx.header == [:COL_1, :COL_2]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1][1] == 1
            @test testctx.results[1].cols[2][1] == 2
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1

            testctx = TestContext()
            parse_file(io_t("""
                xxx
                xxx
                1,2
                3,4
                """),
                [Int,Int],
                testctx,
                limit=1,
                header=false,
                skipto=3,
                force=alg,
            )
            @test testctx.header == [:COL_1, :COL_2]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1][1] == 1
            @test testctx.results[1].cols[2][1] == 2
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1

            testctx = TestContext()
            parse_file(io_t("1,2\n" ^ 100),
                [Int,Int],
                testctx,
                limit=50,
                header=false,
                force=alg,
                buffersize=10,
            )
            res = collect(testctx)
            @test res.COL_1 == ones(Int, 50)
            @test res.COL_2 == 2ones(Int, 50)
        end

        @testset "limit and skiprow" begin
            testctx = TestContext()
            parse_file(io_t("""
                1,2
                3,4
                5,6
                """),
                [Int,Int],
                testctx,
                limit=1,
                skipto=2,
                header=false,
                force=alg,
            )
            @test testctx.header == [:COL_1, :COL_2]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1][1] == 3
            @test testctx.results[1].cols[2][1] == 4
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1

            testctx = TestContext()
            parse_file(io_t("""
                xxx
                a,b
                1,2
                3,4
                """),
                [Int,Int],
                testctx,
                limit=1,
                header=2,
                force=alg,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1][1] == 1
            @test testctx.results[1].cols[2][1] == 2
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Comments ($(io_t), $(alg))" begin
        @testset "only comments, single chunk" begin
            testctx = TestContext()
            parse_file(io_t("""
                #xx
                #xx
                a,b
                #xx
                1,2
                #3,4
                5,6
                #7,8
                """),
                [Int, Int],
                testctx,
                comment="#",
                force=alg
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1][[1, 3]] == [1, 5]
            @test testctx.results[1].cols[2][[1, 3]] == [2, 6]

            @test length(testctx.results) == 1
            @test testctx.results[1].row_statuses == [RowStatus.Ok, RowStatus.MissingValues | RowStatus.SkippedRow, RowStatus.Ok, RowStatus.MissingValues | RowStatus.SkippedRow]
            @test length(testctx.results[1].cols[1]) == 4
            @test length(testctx.results[1].cols[2]) == 4
        end

        @testset "only comments, multiple chunks" begin
            testctx = TestContext()
            parse_file(io_t("""
                #xx
                #xx
                a,b
                #xx
                1,2
                #3,4
                5,6
                #7,8
                """),
                [Int, Int],
                testctx,
                comment="#",
                force=alg,
                buffersize=5,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1] == [1]
            @test testctx.results[1].cols[2] == [2]
            @test testctx.results[3].cols[1] == [5]
            @test testctx.results[3].cols[2] == [6]

            @test length(testctx.results) == 4
            @test testctx.results[1].row_statuses == [RowStatus.Ok]
            @test testctx.results[2].row_statuses == [RowStatus.MissingValues | RowStatus.SkippedRow]
            @test testctx.results[3].row_statuses == [RowStatus.Ok]
            @test testctx.results[4].row_statuses == [RowStatus.MissingValues | RowStatus.SkippedRow]
            @test length(testctx.results[2].cols[1]) == 1
            @test length(testctx.results[2].cols[2]) == 1
            @test length(testctx.results[4].cols[1]) == 1
            @test length(testctx.results[4].cols[2]) == 1
        end

        @testset "comments and header row, single chunk" begin
            testctx = TestContext()
            parse_file(io_t("""
                #xx
                #xx
                xxx
                #xx
                a,b
                #xx
                1,2
                #3,4
                5,6
                #7,8
                """),
                [Int, Int],
                testctx,
                comment="#",
                force=alg,
                header=5,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1][[1, 3]] == [1, 5]
            @test testctx.results[1].cols[2][[1, 3]] == [2, 6]

            @test length(testctx.results) == 1
            @test testctx.results[1].row_statuses == [RowStatus.Ok, RowStatus.MissingValues | RowStatus.SkippedRow, RowStatus.Ok, RowStatus.MissingValues | RowStatus.SkippedRow]
            @test length(testctx.results[1].cols[1]) == 4
            @test length(testctx.results[1].cols[2]) == 4
        end

        @testset "comments and header row, multiple chunks" begin
            testctx = TestContext()
            parse_file(io_t("""
                #xx
                #xx
                xxx
                #xx
                a,b
                #xx
                1,2
                #3,4
                5,6
                #7,8
                """),
                [Int, Int],
                testctx,
                comment="#",
                force=alg,
                buffersize=5,
                header=5,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1] == [1]
            @test testctx.results[1].cols[2] == [2]
            @test testctx.results[3].cols[1] == [5]
            @test testctx.results[3].cols[2] == [6]

            @test length(testctx.results) == 4
            @test testctx.results[1].row_statuses == [RowStatus.Ok]
            @test testctx.results[2].row_statuses == [RowStatus.MissingValues | RowStatus.SkippedRow]
            @test testctx.results[3].row_statuses == [RowStatus.Ok]
            @test testctx.results[4].row_statuses == [RowStatus.MissingValues | RowStatus.SkippedRow]
            @test length(testctx.results[2].cols[1]) == 1
            @test length(testctx.results[2].cols[2]) == 1
            @test length(testctx.results[4].cols[1]) == 1
            @test length(testctx.results[4].cols[2]) == 1
        end

        @testset "comments, header row and data row, single chunk" begin
            testctx = TestContext()
            parse_file(io_t("""
                #xx
                #xx
                xxx
                #xx
                a,b
                #xx
                xxx
                #xx
                1,2
                #3,4
                5,6
                #7,8
                """),
                [Int, Int],
                testctx,
                comment="#",
                force=alg,
                header=5,
                skipto=9,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1][[1, 3]] == [1, 5]
            @test testctx.results[1].cols[2][[1, 3]] == [2, 6]

            @test length(testctx.results) == 1
            @test testctx.results[1].row_statuses == [RowStatus.Ok, RowStatus.MissingValues | RowStatus.SkippedRow, RowStatus.Ok, RowStatus.MissingValues | RowStatus.SkippedRow]
            @test length(testctx.results[1].cols[1]) == 4
            @test length(testctx.results[1].cols[2]) == 4
        end

        @testset "comments, header row and data row, multiple chunks" begin
            testctx = TestContext()
            parse_file(io_t("""
                #xx
                #xx
                xxx
                a,b
                #xx
                xxx
                #xx
                1,2
                #3,4
                5,6
                #7,8
                """),
                [Int, Int],
                testctx,
                comment="#",
                force=alg,
                buffersize=5,
                header=4,
                skipto=8,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int, Int]
            @test testctx.results[1].cols[1] == [1]
            @test testctx.results[1].cols[2] == [2]
            @test testctx.results[3].cols[1] == [5]
            @test testctx.results[3].cols[2] == [6]

            @test length(testctx.results) == 4
            @test testctx.results[1].row_statuses == [RowStatus.Ok]
            @test testctx.results[2].row_statuses == [RowStatus.MissingValues | RowStatus.SkippedRow]
            @test testctx.results[3].row_statuses == [RowStatus.Ok]
            @test testctx.results[4].row_statuses == [RowStatus.MissingValues | RowStatus.SkippedRow]
            @test length(testctx.results[2].cols[1]) == 1
            @test length(testctx.results[2].cols[2]) == 1
            @test length(testctx.results[4].cols[1]) == 1
            @test length(testctx.results[4].cols[2]) == 1
        end

        @testset "file with header and comments only" begin
            testctx = TestContext()
            parse_file(io_t("""
                #xx
                #xx
                xxx
                #xx
                a,b
                #xx
                xxx
                #xx
                #1,2
                #3,4
                #5,6
                #7,8
                """),
                [Int, Int],
                testctx,
                comment="#",
                force=alg,
                header=5,
                skipto=8,
                buffersize=5,
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int, Int]
            @test length(testctx.results) == 1
            @test all(isempty, testctx.results[1].cols)
        end

        @testset "file with comments only" begin
            testctx = TestContext()
            parse_file(io_t("""
                #xx
                #xx
                #xx
                #xx
                #xx
                #1,2
                #3,4
                #5,6
                #7,8
                """),
                [Int, Int],
                testctx,
                comment="#",
                force=alg,
                header=false,
                buffersize=5,
            )
            @test testctx.header == [:COL_1, :COL_2]
            @test testctx.schema == [Int, Int]
            @test length(testctx.results) == 1
            @test all(isempty, testctx.results[1].cols)
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Headers ($(io_t), $(alg))" begin
        @testset "No header, no schema" begin
            testctx = TestContext()
            parse_file(io_t("""
                1,2
                3,4
                """),
                nothing,
                testctx,
                header=false,
                force=alg,
            )
            @test testctx.header == [:COL_1, :COL_2]
            @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31]
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(1, 1), Parsers.PosLen31(5, 1)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(3, 1), Parsers.PosLen31(7, 1)]
            @test testctx.strings[1][1][1:2] == ["1", "3"]
            @test testctx.strings[1][2][1:2] == ["2", "4"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2

            testctx = TestContext()
            parse_file(io_t("""
                xxx
                xxx
                1,2
                3,4
                """),
                nothing,
                testctx,
                header=false,
                skipto=3,
                force=alg,
            )
            @test testctx.header == [:COL_1, :COL_2]
            @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31]
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(9, 1), Parsers.PosLen31(13, 1)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(11, 1), Parsers.PosLen31(15, 1)]
            @test testctx.strings[1][1][1:2] == ["1", "3"]
            @test testctx.strings[1][2][1:2] == ["2", "4"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
        end

        @testset "No header" begin
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(io_t("""
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    header=false,
                    force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1][1:2] == [1,3]
                @test testctx.results[1].cols[2][1:2] == [2,4]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2

                testctx = TestContext()
                parse_file(io_t("""
                    xxx
                    xxx
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    header=false,
                    skipto=3,
                    force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1][1:2] == [1,3]
                @test testctx.results[1].cols[2][1:2] == [2,4]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end
        end

        @testset "default_colname_prefix" begin
            testctx = TestContext()
            parse_file(io_t("""
                1,2
                3,4
                """),
                [Int,Int],
                testctx,
                header=false,
                force=alg,
                default_colname_prefix="#"
            )
            @test testctx.header == [Symbol("#1"), Symbol("#2")]


            testctx = TestContext()
            parse_file(io_t("""
                1,2
                3,4
                """),
                nothing,
                testctx,
                header=false,
                force=alg,
                default_colname_prefix="##"
            )
            @test testctx.header == [Symbol("##1"), Symbol("##2")]
        end

        @testset "Opt out of validation" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                1,2,3
                3,4,5
                """),
                Dict(:q => Int, :b => Int),
                testctx,
                header=[:a, :b, :c],
                force=alg,
                validate_type_map=false,
            )
            @test testctx.header == [:a, :b, :c]
            @test testctx.schema == [Parsers.PosLen31, Int, Parsers.PosLen31]

            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                1,2,3
                3,4,5
                """),
                Dict(:q => Int, :b => Int),
                testctx,
                header=1,
                force=alg,
                validate_type_map=false,
            )
            @test testctx.header == [:a, :b, :c]
            @test testctx.schema == [Parsers.PosLen31, Int, Parsers.PosLen31]
        end

        @testset "Escape in a header" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,"b", "c\"\""
                """),
                [String,String,String],
                testctx,
                force=alg,
                escapechar='"',
            )
            @test testctx.header == [:a, :b, Symbol("c\"")]

            testctx = TestContext()
            parse_file(io_t("""
                a,"b", "c\\\""
                """),
                [String,String,String],
                testctx,
                force=alg,
                escapechar='\\',
            )
            @test testctx.header == [:a, :b, Symbol("c\"")]

            testctx = TestContext()
                parse_file(io_t("""
                    a,"b", "c\"\""
                    """),
                nothing,
                testctx,
                force=alg,
                escapechar='"',
            )
            @test testctx.header == [:a, :b, Symbol("c\"")]

            testctx = TestContext()
                parse_file(io_t("""
                a,"b", "c\\\""
                """),
                nothing,
                testctx,
                force=alg,
                escapechar='\\',
            )
            @test testctx.header == [:a, :b, Symbol("c\"")]
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Empty input ($(io_t), $(alg))" begin
        @testset "Empty lines" begin
            testctx = TestContext()
            parse_file(io_t("a,b,c\n\n"), nothing, testctx, force=alg, ignoreemptyrows=false)
            @test testctx.header == [:a, :b, :c]
            @test testctx.results[1].row_statuses == [RowStatus.TooFewColumns]

            testctx = TestContext()
            parse_file(io_t("a,b,c\n\n"), nothing, testctx, force=alg, ignoreemptyrows=true)
            @test testctx.header == [:a, :b, :c]
            @test testctx.results[1].row_statuses == [RowStatus.SkippedRow | RowStatus.MissingValues]

            testctx = TestContext()
            parse_file(io_t("a,b,c\r\r"), nothing, testctx, force=alg, ignoreemptyrows=true, newlinechar='\r')
            @test testctx.header == [:a, :b, :c]
            @test testctx.results[1].row_statuses == [RowStatus.SkippedRow | RowStatus.MissingValues]

            testctx = TestContext()
            parse_file(io_t("a,b,c\r\n\r\n"), nothing, testctx, force=alg, ignoreemptyrows=true, newlinechar='\n')
            @test testctx.header == [:a, :b, :c]
            @test testctx.results[1].row_statuses == [RowStatus.SkippedRow | RowStatus.MissingValues]


            testctx = TestContext()
            parse_file(io_t("a,b,c\r\n\r\n1,2,3\r\n4,5,6"), [Int,Int,Int], testctx, force=alg, ignoreemptyrows=true)
            @test testctx.header == [:a, :b, :c]
            @test testctx.results[1].row_statuses == [RowStatus.SkippedRow | RowStatus.MissingValues, RowStatus.Ok, RowStatus.Ok]
            @test testctx.results[1].cols[1][2:3] == [1, 4]
            @test testctx.results[1].cols[2][2:3] == [2, 5]
            @test testctx.results[1].cols[3][2:3] == [3, 6]

            testctx = TestContext()
            parse_file(io_t("a,b,c\n\n1,2,3\n4,5,6"), [Int,Int,Int], testctx, force=alg, ignoreemptyrows=true)
            @test testctx.header == [:a, :b, :c]
            @test testctx.results[1].row_statuses == [RowStatus.SkippedRow | RowStatus.MissingValues, RowStatus.Ok, RowStatus.Ok]
            @test testctx.results[1].cols[1][2:3] == [1, 4]
            @test testctx.results[1].cols[2][2:3] == [2, 5]
            @test testctx.results[1].cols[3][2:3] == [3, 6]

            testctx = TestContext()
            parse_file(io_t("a,b,c\r\r1,2,3\r4,5,6"), [Int,Int,Int], testctx, force=alg, ignoreemptyrows=true, newlinechar='\r')
            @test testctx.header == [:a, :b, :c]
            @test testctx.results[1].row_statuses == [RowStatus.SkippedRow | RowStatus.MissingValues, RowStatus.Ok, RowStatus.Ok]
            @test testctx.results[1].cols[1][2:3] == [1, 4]
            @test testctx.results[1].cols[2][2:3] == [2, 5]
            @test testctx.results[1].cols[3][2:3] == [3, 6]

            testctx = TestContext()
            parse_file(io_t("a,b,c\r\n\r\n1,2,3\r\n\r\n4,5,6"), [Int,Int,Int], testctx, force=alg, ignoreemptyrows=true)
            @test testctx.header == [:a, :b, :c]
            @test testctx.results[1].row_statuses == [RowStatus.SkippedRow | RowStatus.MissingValues, RowStatus.Ok, RowStatus.SkippedRow | RowStatus.MissingValues, RowStatus.Ok]
            @test testctx.results[1].cols[1][[2,4]] == [1, 4]
            @test testctx.results[1].cols[2][[2,4]] == [2, 5]
            @test testctx.results[1].cols[3][[2,4]] == [3, 6]
        end

        @testset "no file header, no provided header, no schema" begin
            testctx = TestContext()
            parse_file(io_t(""), nothing, testctx, force=alg, header=false)
            @test isempty(testctx.results[1].cols)
            @test isempty(testctx.header)
            @test isempty(testctx.schema)
        end

        @testset "no file header, has provided header, no schema" begin
            testctx = TestContext()
            parse_file(io_t(""), nothing, testctx, force=alg, header=[:A, :B])
            @test length(testctx.results[1].cols) == 2
            @test testctx.header == [:A, :B]
            @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31]
        end

        @testset "no file header, no provided header, has schema" begin
            testctx = TestContext()
            parse_file(io_t(""), [Int, String], testctx, force=alg, header=false)
            @test length(testctx.results[1].cols) == 2
            @test testctx.header == [:COL_1, :COL_2]
            @test testctx.schema == [Int, Parsers.PosLen31]
        end

        @testset "has file header, has provided header, has schema" begin
            testctx = TestContext()
            parse_file(io_t(""), [Int, String], testctx, force=alg, header=[:A, :B])
            @test length(testctx.results[1].cols) == 2
            @test testctx.header == [:A, :B]
            @test testctx.schema == [Int, Parsers.PosLen31]
        end

        @testset "has file header, no provided header, no schema" begin
            testctx = TestContext()
            parse_file(io_t(""), nothing, testctx, force=alg, header=true)
            @test isempty(testctx.results[1].cols)
            @test isempty(testctx.header)
            @test isempty(testctx.schema)
        end

        @testset "has file header, no provided header, has schema" begin
            testctx = TestContext()
            parse_file(io_t(""), [Int, String], testctx, force=alg, header=true)
            @test length(testctx.results[1].cols) == 2
            @test testctx.header == [:COL_1, :COL_2]
            @test testctx.schema == [Int, Parsers.PosLen31]
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "RFC4180 ($(io_t), $(alg))" begin
        # https://www.ietf.org/rfc/rfc4180.txt
        @testset "Each record is located on a separate line, delimited by a line break (CRLF)." begin
            testctx = TestContext()
            parse_file(io_t("aaa,bbb,ccc\nzzz,yyy,xxx\n"), nothing, testctx, force=alg, header=false)
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(1, 3), Parsers.PosLen31(13, 3)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(5, 3), Parsers.PosLen31(17, 3)]
            @test testctx.results[1].cols[3][1:2] == [Parsers.PosLen31(9, 3), Parsers.PosLen31(21, 3)]
            @test testctx.strings[1][1][1:2] == ["aaa", "zzz"]
            @test testctx.strings[1][2][1:2] == ["bbb", "yyy"]
            @test testctx.strings[1][3][1:2] == ["ccc", "xxx"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2

            testctx = TestContext()
            parse_file(io_t("aaa,bbb,ccc\r\nzzz,yyy,xxx\r\n"), nothing, testctx, force=alg, header=false)
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(1, 3), Parsers.PosLen31(14, 3)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(5, 3), Parsers.PosLen31(18, 3)]
            @test testctx.results[1].cols[3][1:2] == [Parsers.PosLen31(9, 3), Parsers.PosLen31(22, 3)]
            @test testctx.strings[1][1][1:2] == ["aaa", "zzz"]
            @test testctx.strings[1][2][1:2] == ["bbb", "yyy"]
            @test testctx.strings[1][3][1:2] == ["ccc", "xxx"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2
        end

        @testset "The last record in the file may or may not have an ending line break." begin
            testctx = TestContext()
            parse_file(io_t("aaa,bbb,ccc\nzzz,yyy,xxx"), nothing, testctx, force=alg, header=false)
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(1, 3), Parsers.PosLen31(13, 3)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(5, 3), Parsers.PosLen31(17, 3)]
            @test testctx.results[1].cols[3][1:2] == [Parsers.PosLen31(9, 3), Parsers.PosLen31(21, 3)]
            @test testctx.strings[1][1][1:2] == ["aaa", "zzz"]
            @test testctx.strings[1][2][1:2] == ["bbb", "yyy"]
            @test testctx.strings[1][3][1:2] == ["ccc", "xxx"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2

            testctx = TestContext()
            parse_file(io_t("aaa,bbb,ccc\r\nzzz,yyy,xxx"), nothing, testctx, force=alg, header=false)
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(1, 3), Parsers.PosLen31(14, 3)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(5, 3), Parsers.PosLen31(18, 3)]
            @test testctx.results[1].cols[3][1:2] == [Parsers.PosLen31(9, 3), Parsers.PosLen31(22, 3)]
            @test testctx.strings[1][1][1:2] == ["aaa", "zzz"]
            @test testctx.strings[1][2][1:2] == ["bbb", "yyy"]
            @test testctx.strings[1][3][1:2] == ["ccc", "xxx"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2
        end

        @testset """
            There maybe an optional header line appearing as the first line
            of the file with the same format as normal record lines.  This
            header will contain names corresponding to the fields in the file
            and should contain the same number of fields as the records in
            the rest of the file (the presence or absence of the header line
            should be indicated via the optional "header" parameter of this
            MIME type).""" begin
            testctx = TestContext()
            parse_file(io_t("field_name,field_name,field_name\naaa,bbb,ccc\nzzz,yyy,xxx"), nothing, testctx, force=alg)
            @test testctx.header == [:field_name, :field_name_1, :field_name_2]
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(33+1, 3), Parsers.PosLen31(33+13, 3)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(33+5, 3), Parsers.PosLen31(33+17, 3)]
            @test testctx.results[1].cols[3][1:2] == [Parsers.PosLen31(33+9, 3), Parsers.PosLen31(33+21, 3)]
            @test testctx.strings[1][1][1:2] == ["aaa", "zzz"]
            @test testctx.strings[1][2][1:2] == ["bbb", "yyy"]
            @test testctx.strings[1][3][1:2] == ["ccc", "xxx"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2

            testctx = TestContext()
            parse_file(io_t("field_name,field_name,field_name\r\naaa,bbb,ccc\r\nzzz,yyy,xxx"), nothing, testctx, force=alg)
            @test testctx.header == [:field_name, :field_name_1, :field_name_2]
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(34+1, 3), Parsers.PosLen31(34+14, 3)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(34+5, 3), Parsers.PosLen31(34+18, 3)]
            @test testctx.results[1].cols[3][1:2] == [Parsers.PosLen31(34+9, 3), Parsers.PosLen31(34+22, 3)]
            @test testctx.strings[1][1][1:2] == ["aaa", "zzz"]
            @test testctx.strings[1][2][1:2] == ["bbb", "yyy"]
            @test testctx.strings[1][3][1:2] == ["ccc", "xxx"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2
        end

        @testset """
            Within the header and each record, there may be one or more
            fields, separated by commas.  Each line should contain the same
            number of fields throughout the file.  Spaces are considered part
            of a field and should not be ignored.  The last field in the
            record must not be followed by a comma.
            """ begin
            testctx = TestContext()
            parse_file(io_t("aaa,bbb,ccc"), nothing, testctx, force=alg, header=false)
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(1, 3)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(5, 3)]
            @test testctx.results[1].cols[3][1:1] == [Parsers.PosLen31(9, 3)]
            @test testctx.strings[1][1][1:1] == ["aaa"]
            @test testctx.strings[1][2][1:1] == ["bbb"]
            @test testctx.strings[1][3][1:1] == ["ccc"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
            @test length(testctx.results[1].cols[3]) == 1
        end

        @testset """
            Each field may or may not be enclosed in double quotes (however
            some programs, such as Microsoft Excel, do not use double quotes
            at all).  If fields are not enclosed with double quotes, then
            double quotes may not appear inside the fields. """ begin
            testctx = TestContext()
            parse_file(io_t("\"aaa\",\"bbb\",\"ccc\"\nzzz,yyy,xxx"), nothing, testctx, force=alg, header=false)
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(2, 3), Parsers.PosLen31(19, 3)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(8, 3), Parsers.PosLen31(23, 3)]
            @test testctx.results[1].cols[3][1:2] == [Parsers.PosLen31(14, 3), Parsers.PosLen31(27, 3)]
            @test testctx.strings[1][1][1:2] == ["aaa", "zzz"]
            @test testctx.strings[1][2][1:2] == ["bbb", "yyy"]
            @test testctx.strings[1][3][1:2] == ["ccc", "xxx"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2

            testctx = TestContext()
            parse_file(io_t("\"aaa\",\"bbb\",\"ccc\"\r\nzzz,yyy,xxx"), nothing, testctx, force=alg, header=false)
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(2, 3), Parsers.PosLen31(20, 3)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(8, 3), Parsers.PosLen31(24, 3)]
            @test testctx.results[1].cols[3][1:2] == [Parsers.PosLen31(14, 3), Parsers.PosLen31(28, 3)]
            @test testctx.strings[1][1][1:2] == ["aaa", "zzz"]
            @test testctx.strings[1][2][1:2] == ["bbb", "yyy"]
            @test testctx.strings[1][3][1:2] == ["ccc", "xxx"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2
        end

        @testset """
            Fields containing line breaks (CRLF), double quotes, and commas
            should be enclosed in double-quotes.""" begin
            testctx = TestContext()
            parse_file(io_t("\"aaa\",\"b\nbb\",\"ccc\"\nzzz,yyy,xxx"), nothing, testctx, force=alg, header=false)
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(2, 3), Parsers.PosLen31(20, 3)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(8, 4), Parsers.PosLen31(24, 3)]
            @test testctx.results[1].cols[3][1:2] == [Parsers.PosLen31(15, 3), Parsers.PosLen31(28, 3)]
            @test testctx.strings[1][1][1:2] == ["aaa", "zzz"]
            @test testctx.strings[1][2][1:2] == ["b\nbb", "yyy"]
            @test testctx.strings[1][3][1:2] == ["ccc", "xxx"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2

            testctx = TestContext()
            parse_file(io_t("\"aaa\",\"b\r\nbb\",\"ccc\"\r\nzzz,yyy,xxx"), nothing, testctx, force=alg, header=false)
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(2, 3), Parsers.PosLen31(22, 3)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(8, 5), Parsers.PosLen31(26, 3)]
            @test testctx.results[1].cols[3][1:2] == [Parsers.PosLen31(16, 3), Parsers.PosLen31(30, 3)]
            @test testctx.strings[1][1][1:2] == ["aaa", "zzz"]
            @test testctx.strings[1][2][1:2] == ["b\r\nbb", "yyy"]
            @test testctx.strings[1][3][1:2] == ["ccc", "xxx"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
            @test length(testctx.results[1].cols[3]) == 2
        end

        @testset """
            If double-quotes are used to enclose fields, then a double-quote
            appearing inside a field must be escaped by preceding it with
            another double quote. """ begin
            testctx = TestContext()
            parse_file(io_t("\"aaa\",\"b\"\"bb\",\"ccc\""), nothing, testctx, force=alg, header=false)
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(2, 3)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(8, 5, false, true)]
            @test testctx.results[1].cols[3][1:1] == [Parsers.PosLen31(16, 3)]
            @test testctx.strings[1][1][1:1] == ["aaa"]
            @test testctx.strings[1][2][1:1] == ["b\"bb"]
            @test testctx.strings[1][3][1:1] == ["ccc"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
            @test length(testctx.results[1].cols[3]) == 1
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Whitespace ($(io_t), $(alg))" begin
        @testset "Unquoted string fields preserve whitespace" begin
            testctx = TestContext()
            parse_file(io_t("""a, b\n  foo  , "bar"    \n"""), nothing, testctx, force=alg)
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(6, 7)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(16, 3)]
            @test testctx.strings[1][1][1:1] == ["  foo  "]
            @test testctx.strings[1][2][1:1] == ["bar"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end

        @testset "Whitespace surrounding quoted fields is stripped" begin
            testctx = TestContext()
            parse_file(io_t("""a, b\n  "foo"  , "bar"    \n"""), nothing, testctx, force=alg)
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(9, 3)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(18, 3)]
            @test testctx.strings[1][1][1:1] == ["foo"]
            @test testctx.strings[1][2][1:1] == ["bar"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1

            testctx = TestContext()
            parse_file(io_t("""a, b\n"foo"  , "bar"    \n"""), nothing, testctx, force=alg)
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(7, 3)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(16, 3)]
            @test testctx.strings[1][1][1:1] == ["foo"]
            @test testctx.strings[1][2][1:1] == ["bar"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end

        @testset "Newlines inside a quoted field are handled properly" begin
            testctx = TestContext()
            parse_file(io_t("""a, b\n     "foo", "bar\n     acsas"\n"""), nothing, testctx, force=alg)
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(12, 3)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(19, 14)]
            @test testctx.strings[1][1][1:1] == ["foo"]
            @test testctx.strings[1][2][1:1] == ["bar\n     acsas"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1

            testctx = TestContext()
            parse_file(io_t("""a, b\n     "foo", "bar\n\r     acsas"\n"""), nothing, testctx, force=alg)
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(12, 3)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(19, 15)]
            @test testctx.strings[1][1][1:1] == ["foo"]
            @test testctx.strings[1][2][1:1] == ["bar\n\r     acsas"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end

        @testset "Escaped quotes inside a quoted field are handled properly" begin
            testctx = TestContext()
            parse_file(io_t("""a, b\n"foo"  ,"The cat said, ""meow"" "\n"""), nothing, testctx, force=alg)
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(7, 3)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(15, 23, false, true)]
            @test testctx.strings[1][1][1:1] == ["foo"]
            @test testctx.strings[1][2][1:1] == ["The cat said, \"meow\" "]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1

            testctx = TestContext()
            parse_file(io_t("""a, b\n"foo"  ,"The cat said, \\"meow\\" "\n"""), nothing, testctx, force=alg, escapechar='\\')
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(7, 3)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(15, 23, false, true)]
            @test testctx.strings[1][1][1:1] == ["foo"]
            @test testctx.strings[1][2][1:1] == ["The cat said, \"meow\" "]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end

        @testset "Characters outside of a quoted field should be marked as ValueParsingError" begin
            testctx = TestContext()
            parse_file(io_t("""a, b\n"foo"  , "bar"         234235\n"""), nothing, testctx, force=alg)
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(7, 3)]
            @test testctx.strings[1][1][1:1] == ["foo"]
            @test testctx.results[1].row_statuses[1] & ChunkedCSV.RowStatus.ValueParsingError > 0
            @test testctx.results[1].errored_values[1, 2]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end

        @testset "Carriage return ($(io_t), $(alg))" begin
            @testset "ints" begin
                testctx = TestContext()
                parse_file(io_t("""
                    a,b
                    0,1\r
                    1,2\r
                    2,3\r
                    3,\r
                    """),
                    [Int,Int],
                    testctx,
                    force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int,Int]
                @test length(testctx.results) == 1
                @test testctx.results[1].cols[1][1:4] == 0:3
                @test testctx.results[1].cols[2][1:3] == 1:3
                @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].missing_values[1, 2]
            end

            @testset "decimals" begin
                testctx = TestContext()
                parse_file(io_t("""
                    a,b
                    0,1\r
                    1,1.0\r
                    2,10.0e-1\r
                    3,\r
                    """),
                    [Int,FixedDecimal{Int,4}],
                    testctx,
                    force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int,FixedDecimal{Int,4}]
                @test length(testctx.results) == 1
                @test testctx.results[1].cols[1][1:4] == 0:3
                @test testctx.results[1].cols[2][1:3] == fill(FixedDecimal{Int,4}(1), 3)
                @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].missing_values[1, 2]
            end

            @testset "guess datetimes" begin
                testctx = TestContext()
                parse_file(io_t("""
                    a,b
                    0,1969-07-20\r
                    1,1969-07-20 00:00:00\r
                    2,1969-07-20 00:00:00.00\r
                    3,1969-07-20 00:00:00.000UTC\r
                    4,1969-07-19 17:00:00.000America/Los_Angeles\r
                    5,1969-07-20 00:00:00.00-0000\r
                    6,\r
                    """),
                    [Int,GuessDateTime],
                    testctx,
                    force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int,DateTime]
                @test length(testctx.results) == 1
                @test testctx.results[1].cols[1][1:7] == 0:6
                @test testctx.results[1].cols[2][1:6] == fill(DateTime(1969, 7, 20), 6)
                @test testctx.results[1].row_statuses[7] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].missing_values[1, 2]
            end
        end

        @testset "Custom newlinechar ($(io_t), $(alg))" begin
            for alg in [:serial, :parallel]
                @testset "int" begin
                    testctx = TestContext()
                    parse_file(io_t("a,b,c\r1,2,3\r3,4,4"),
                        [Int,Int,Int],
                        testctx,
                        force=alg,
                        newlinechar='\r'
                    )
                    @test testctx.header == [:a, :b, :c]
                    @test testctx.schema == [Int, Int, Int]
                    @test testctx.results[1].cols[1][1:2] == [1,3]
                    @test testctx.results[1].cols[2][1:2] == [2,4]
                    @test testctx.results[1].cols[3][1:2] == [3,4]
                    @test length(testctx.results[1].cols[1]) == 2
                    @test length(testctx.results[1].cols[2]) == 2
                    @test length(testctx.results[1].cols[3]) == 2
                end
            end
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Ending on an newline ($(io_t), $(alg))" begin
        testctx = TestContext()
        parse_file(io_t("""
            a,b
            0,z
            1,"S\"\"\"
            2,"S\"\"\""""),
            [Int,String],
            testctx,
            buffersize=8, # last char of the current chunk is a newline
            force=alg,
            escapechar='"',
        )
        @test testctx.header == [:a, :b]
        @test testctx.schema == [Int,Parsers.PosLen31]
        @test length(testctx.results) == 3
        @test testctx.results[1].cols[1][1] == 0
        @test testctx.results[2].cols[1][1] == 1
        @test testctx.results[3].cols[1][1] == 2
        @test testctx.results[1].cols[2][1] == Parsers.PosLen31(7,1)
        @test testctx.results[2].cols[2][1] == Parsers.PosLen31(4,3,false,true)
        @test testctx.results[3].cols[2][1] == Parsers.PosLen31(4,3,false,true)
        @test testctx.strings[1][2][1] == "z"
        @test testctx.strings[2][2][1] == "S\""
        @test testctx.strings[3][2][1] == "S\""

        testctx = TestContext()
        parse_file(io_t("""
            a,b
            0,z
            1,"S\"\"\"
            2,"S\"\"\"
            """),
            [Int,String],
            testctx,
            buffersize=15, # first char of the next chunk is a newline
            force=alg,
            escapechar='"',
        )
        @test testctx.header == [:a, :b]
        @test testctx.schema == [Int,Parsers.PosLen31]
        @test length(testctx.results) == 3
        @test testctx.results[1].cols[1][1] == 0
        @test testctx.results[2].cols[1][1] == 1
        @test testctx.results[3].cols[1][1] == 2
        @test testctx.results[1].cols[2][1] == Parsers.PosLen31(7,1)
        @test testctx.results[2].cols[2][1] == Parsers.PosLen31(4,3,false,true)
        @test testctx.results[3].cols[2][1] == Parsers.PosLen31(4,3,false,true)
        @test testctx.strings[1][2][1] == "z"
        @test testctx.strings[2][2][1] == "S\""
        @test testctx.strings[3][2][1] == "S\""
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Quoted fields ($(io_t), $(alg))" begin
        @testset "floats" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c,d,e
                0,1,"1",1,"1"\r
                1,1.0,"1.0",1.0,"1.0"
                2,10.0e-1,"10.0e-1",10.0e-1,"10.0e-1"\r
                """),
                [Int,Float64,Float64,Float64,Float64],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b, :c, :d, :e]
            @test testctx.schema == [Int,Float64,Float64,Float64,Float64]
            @test length(testctx.results) == 1
            @test testctx.results[1].cols[1][1:3] == 0:2
            @test testctx.results[1].cols[2][1:3] == fill(1.0, 3)
            @test testctx.results[1].cols[3][1:3] == fill(1.0, 3)
            @test testctx.results[1].cols[4][1:3] == fill(1.0, 3)
            @test testctx.results[1].cols[5][1:3] == fill(1.0, 3)
            @test testctx.results[1].row_statuses[1:3] == fill(ChunkedCSV.RowStatus.Ok, 3)
        end

        @testset "guess datetimes" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c,d,e
                0,1969-07-20,"1969-07-20",1969-07-20,"1969-07-20"
                1,1969-07-20 00:00:00,"1969-07-20 00:00:00",1969-07-20 00:00:00,"1969-07-20 00:00:00"\r
                2,1969-07-20 00:00:00.00,"1969-07-20 00:00:00.00",1969-07-20 00:00:00.00,"1969-07-20 00:00:00.00"
                3,1969-07-20 00:00:00.000UTC,"1969-07-20 00:00:00.000UTC",1969-07-20 00:00:00.000UTC,"1969-07-20 00:00:00.000UTC"
                4,1969-07-19 17:00:00.000America/Los_Angeles,"1969-07-19 17:00:00.000America/Los_Angeles",1969-07-19 17:00:00.000America/Los_Angeles,"1969-07-19 17:00:00.000America/Los_Angeles"\r
                5,1969-07-20 00:00:00.00-0000,"1969-07-20 00:00:00.00-0000",1969-07-20 00:00:00.00-0000,"1969-07-20 00:00:00.00-0000"
                6,1969-07-20 00:00:00.00Z,"1969-07-20 00:00:00.00Z","1969-07-20 00:00:00.00Z","1969-07-20T00:00:00Z"
                """),
                [Int,GuessDateTime,GuessDateTime,GuessDateTime,GuessDateTime],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b, :c, :d, :e]
            @test testctx.schema == [Int,DateTime,DateTime,DateTime,DateTime]
            @test length(testctx.results) == 1
            @test testctx.results[1].cols[1][1:7] == 0:6
            @test testctx.results[1].cols[2][1:7] == fill(DateTime(1969, 7, 20), 7)
            @test testctx.results[1].cols[3][1:7] == fill(DateTime(1969, 7, 20), 7)
            @test testctx.results[1].cols[4][1:7] == fill(DateTime(1969, 7, 20), 7)
            @test testctx.results[1].cols[5][1:7] == fill(DateTime(1969, 7, 20), 7)
            @test testctx.results[1].row_statuses[1:7] == fill(ChunkedCSV.RowStatus.Ok, 7)
        end

        @testset "decimals" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c,d,e
                0,1,"1",1,"1"\r
                1,1.0,"1.0",1.0,"1.0"
                2,10.0e-1,"10.0e-1",10.0e-1,"10.0e-1"\r
                """),
                [Int,FixedDecimal{Int,4},FixedDecimal{Int,4},FixedDecimal{Int,4},FixedDecimal{Int,4}],
                testctx,
                force=alg,
            )
            @test testctx.header == [:a, :b, :c, :d, :e]
            @test testctx.schema == [Int,FixedDecimal{Int,4},FixedDecimal{Int,4},FixedDecimal{Int,4},FixedDecimal{Int,4}]
            @test length(testctx.results) == 1
            @test testctx.results[1].cols[1][1:3] == 0:2
            @test testctx.results[1].cols[2][1:3] == fill(FixedDecimal{Int,4}(1), 3)
            @test testctx.results[1].cols[3][1:3] == fill(FixedDecimal{Int,4}(1), 3)
            @test testctx.results[1].cols[4][1:3] == fill(FixedDecimal{Int,4}(1), 3)
            @test testctx.results[1].cols[5][1:3] == fill(FixedDecimal{Int,4}(1), 3)
            @test testctx.results[1].row_statuses[1:3] == fill(ChunkedCSV.RowStatus.Ok, 3)
        end

        @testset "Opening and closing quotes ($(io_t), $(alg))" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b
                1,S2E
                3,S"ES4"EE
                """),
                nothing,
                testctx,
                force=alg,
                openquotechar='S',
                closequotechar='E',
            )
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(5, 1), Parsers.PosLen31(11, 1)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(8, 1), Parsers.PosLen31(14, 6, false, true)]
            @test testctx.strings[1][1][1:2] == ["1", "3"]
            @test testctx.strings[1][2][1:2] == ["2", "ES4E"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2
        end

        @testset "Each chunk ends on a quote ($(io_t), $(alg))" begin
            testctx = TestContext()
            parse_file(io_t("""
                0,"S\"\"\"
                1,"S\"\"\"
                2,"S\"\"\"
                3,"S\"\"\"
                4,"S\"\"\""""),
                [Int,String],
                testctx,
                buffersize=8,
                escapechar='"',
                header=false,
                force=alg,
            )
            @test testctx.schema == [Int,Parsers.PosLen31]
            @test length(testctx.results) == 5
            @test testctx.results[1].cols[1][1] == 0
            @test testctx.results[2].cols[1][1] == 1
            @test testctx.results[3].cols[1][1] == 2
            @test testctx.results[4].cols[1][1] == 3
            @test testctx.results[5].cols[1][1] == 4
            @test testctx.results[1].cols[2][1] == Parsers.PosLen31(4,3,false,true)
            @test testctx.results[2].cols[2][1] == Parsers.PosLen31(4,3,false,true)
            @test testctx.results[3].cols[2][1] == Parsers.PosLen31(4,3,false,true)
            @test testctx.results[4].cols[2][1] == Parsers.PosLen31(4,3,false,true)
            @test testctx.results[5].cols[2][1] == Parsers.PosLen31(4,3,false,true)
            @test testctx.strings[1][2][1] == "S\""
            @test testctx.strings[2][2][1] == "S\""
            @test testctx.strings[3][2][1] == "S\""
            @test testctx.strings[4][2][1] == "S\""
            @test testctx.strings[5][2][1] == "S\""
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Escapes ($(io_t), $(alg))" begin
        @testset "Escaped quote" begin
            testctx = TestContext()
            parse_file(io_t("""
                a|b
                1|"2\\""
                3|4
                """),
                nothing,
                testctx,
                escapechar='\\',
                force=alg,
                delim='|',
            )
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:2] == [Parsers.PosLen31(5, 1), Parsers.PosLen31(13, 1)]
            @test testctx.results[1].cols[2][1:2] == [Parsers.PosLen31(8, 3, false, true), Parsers.PosLen31(15, 1)]
            @test testctx.strings[1][1][1:2] == ["1", "3"]
            @test testctx.strings[1][2][1:2] == ["2\"", "4"]
            @test length(testctx.results[1].cols[1]) == 2
            @test length(testctx.results[1].cols[2]) == 2

            testctx = TestContext()
            parse_file(io_t("""
                a|b
                1|"2\\"\""""),
                nothing,
                testctx,
                escapechar='\\',
                delim='|',
            )
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(5, 1)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(8, 3, false, true)]
            @test testctx.strings[1][1][1:1] == ["1"]
            @test testctx.strings[1][2][1:1] == ["2\""]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end

        @testset "Two escape characters before closing quote" begin
            testctx = TestContext()
            parse_file(io_t("""
                a,b
                "a","b\\\\"
                """),
                nothing,
                testctx,
                force=alg,
                escapechar='\\'
            )
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(6, 1)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(10, 3, false, true)]
            @test testctx.strings[1][1][1:1] == ["a"]
            @test testctx.strings[1][2][1:1] == ["b\\"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end

        for buffersize in (12, 13, 14, 15)
            @testset "Ending on an escapechar @ $buffersize" begin
                # buffersize 12 ends the first buffer on letter S
                testctx = TestContext()
                parse_file(io_t("""
                    a,b
                    0,z
                    1,"S\"\"\"
                    """),
                    [Int,String],
                    testctx,
                    buffersize=buffersize,
                    escapechar='"',
                    force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int,Parsers.PosLen31]
                @test length(testctx.results) == 2
                @test testctx.results[1].cols[1][1] == 0
                @test testctx.results[2].cols[1][1] == 1
                @test testctx.results[1].cols[2][1] == Parsers.PosLen31(7,1)
                @test testctx.results[2].cols[2][1] == Parsers.PosLen31(4,3,false,true)
                @test testctx.strings[1][2][1] == "z"
                @test testctx.strings[2][2][1] == "S\""

                testctx = TestContext()
                parse_file(io_t("""
                    a,b
                    0,z
                    1,"S\\\\\"
                    """),
                    [Int,String],
                    testctx,
                    buffersize=buffersize,
                    escapechar='\\',
                    force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int,Parsers.PosLen31]
                @test length(testctx.results) == 2
                @test testctx.results[1].cols[1][1] == 0
                @test testctx.results[2].cols[1][1] == 1
                @test testctx.results[1].cols[2][1] == Parsers.PosLen31(7,1)
                @test testctx.results[2].cols[2][1] == Parsers.PosLen31(4,3,false,true)
                @test testctx.strings[1][2][1] == "z"
                @test testctx.strings[2][2][1] == "S\\"

                testctx = TestContext()
                parse_file(io_t("""
                    a,b
                    0,z
                    1,"S\\"\\""
                    """),
                    [Int,String],
                    testctx,
                    buffersize=buffersize,
                    escapechar='\\',
                    force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int,Parsers.PosLen31]
                @test length(testctx.results) == 2
                @test testctx.results[1].cols[1][1] == 0
                @test testctx.results[2].cols[1][1] == 1
                @test testctx.results[1].cols[2][1] == Parsers.PosLen31(7,1)
                @test testctx.results[2].cols[2][1] == Parsers.PosLen31(4,5,false,true)
                @test testctx.strings[1][2][1] == "z"
                @test testctx.strings[2][2][1] == "S\"\""

                @testset "multiple chunks end on an escape, $buffersize" begin
                    testctx = TestContext()
                    parse_file(io_t("""
                        a,b
                        0,z
                        1,"S\"\"\"
                        2,z
                        3,"S\"\"\"
                        4,z
                        5,"S\"\"\"
                        6,z
                        7,"S\"\"\"
                        """),
                        [Int,String],
                        testctx,
                        buffersize=buffersize, # first S in on position 12, second on 24...
                        escapechar='"',
                        force=alg,
                    )
                    @test testctx.header == [:a, :b]
                    @test testctx.schema == [Int,Parsers.PosLen31]
                    @test length(testctx.results) == 5
                    @test testctx.results[1].cols[1][1] == 0
                    @test testctx.results[2].cols[1] == 1:2
                    @test testctx.results[3].cols[1] == 3:4
                    @test testctx.results[4].cols[1] == 5:6
                    @test testctx.results[5].cols[1][1] == 7
                    @test testctx.results[1].cols[2][1] == Parsers.PosLen31(7,1)
                    @test testctx.results[2].cols[2] == [Parsers.PosLen31(4,3,false,true), Parsers.PosLen31(11,1)]
                    @test testctx.results[3].cols[2] == [Parsers.PosLen31(4,3,false,true), Parsers.PosLen31(11,1)]
                    @test testctx.results[4].cols[2] == [Parsers.PosLen31(4,3,false,true), Parsers.PosLen31(11,1)]
                    @test testctx.results[5].cols[2][1] == Parsers.PosLen31(4,3,false,true)
                    @test testctx.strings[1][2][1] == "z"
                    @test testctx.strings[2][2] == ["S\"", "z"]
                    @test testctx.strings[3][2] == ["S\"", "z"]
                    @test testctx.strings[4][2] == ["S\"", "z"]
                    @test testctx.strings[5][2][1] == "S\""
                end
            end
        end

        @testset "Ending on an escapechar misc" begin
            testctx = TestContext()
            ChunkedCSV.parse_file(io_t("""
                123456
                \"\"\"a\"\"\""""),
                nothing,
                testctx,
                header=false,
                buffersize=8,
                escapechar='"',
                force=alg,
            )
            @test testctx.results[1].cols[1] == [Parsers.PosLen31(1, 6)]
            @test testctx.results[2].cols[1] == [Parsers.PosLen31(2, 5, false, true)]
            @test testctx.strings[1][1] == ["123456"]
            @test testctx.strings[2][1] == ["\"a\""]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[2].cols[1]) == 1

            testctx = TestContext()
            ChunkedCSV.parse_file(io_t("""
                12345
                \"\"\"a\"\"\""""),
                nothing,
                testctx,
                header=false,
                buffersize=8,
                escapechar='"',
                force=alg,
            )
            @test testctx.results[1].cols[1] == [Parsers.PosLen31(1, 5)]
            @test testctx.results[2].cols[1] == [Parsers.PosLen31(2, 5, false, true)]
            @test testctx.strings[1][1] == ["12345"]
            @test testctx.strings[2][1] == ["\"a\""]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[2].cols[1]) == 1

            testctx = TestContext()
            ChunkedCSV.parse_file(io_t("""
                12345
                \"\\\"a\\\"\""""),
                nothing,
                testctx,
                header=false,
                buffersize=8,
                escapechar='\\',
                force=alg,
            )
            @test testctx.results[1].cols[1] == [Parsers.PosLen31(1, 5)]
            @test testctx.results[2].cols[1] == [Parsers.PosLen31(2, 5, false, true)]
            @test testctx.strings[1][1] == ["12345"]
            @test testctx.strings[2][1] == ["\"a\""]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[2].cols[1]) == 1

            # This example is set up so that the last partially filled buffer
            # ends on a quote and the next byte (from previous chunk) is an escape
            testctx = TestContext()
            ChunkedCSV.parse_file(IOBuffer("""
                \"\\\"data\\\"\"
                \"\\\"end\""""),
                nothing,
                testctx,
                header=false,
                buffersize=13,
                escapechar='\\',
            )
            @test testctx.results[1].cols[1] == [Parsers.PosLen31(2, 8, false, true)]
            @test testctx.results[2].cols[1] == [Parsers.PosLen31(2, 5, false, true)]
            @test testctx.strings[1][1] == ["\"data\""]
            @test testctx.strings[2][1] == ["\"end"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[2].cols[1]) == 1

            testctx = TestContext()
            ChunkedCSV.parse_file(IOBuffer("""
                \"\"\"data\"\"\"
                \"\"\"end\""""),
                nothing,
                testctx,
                header=false,
                buffersize=13,
                escapechar='"',
            )
            @test testctx.results[1].cols[1] == [Parsers.PosLen31(2, 8, false, true)]
            @test testctx.results[2].cols[1] == [Parsers.PosLen31(2, 5, false, true)]
            @test testctx.strings[1][1] == ["\"data\""]
            @test testctx.strings[2][1] == ["\"end"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[2].cols[1]) == 1

            # Ending on an escapechar and the next character is not an escape
            testctx = TestContext()
            ChunkedCSV.parse_file(IOBuffer("""
                \"\"\"data\"\"\"
                \"\"\n"""),
                nothing,
                testctx,
                header=false,
                buffersize=13,
                escapechar='"',
            )
            @test testctx.results[1].cols[1] == [Parsers.PosLen31(2, 8, false, true)]
            @test testctx.results[2].cols[1] == [Parsers.PosLen31(2, 0, false, false)]
            @test testctx.strings[1][1] == ["\"data\""]
            @test testctx.strings[2][1] == [""]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[2].cols[1]) == 1
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "BOM ($(io_t), $(alg))" begin
        @testset "buffersize=10" begin
            testctx = TestContext()
            parse_file(io_t("""
                \xef\xbb\xbfa,b
                "a","b\\\\"
                """),
                nothing,
                testctx,
                force=alg,
                escapechar='\\',
                buffersize=10,
            )
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(2, 1)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(6, 3, false, true)]
            @test testctx.strings[1][1][1:1] == ["a"]
            @test testctx.strings[1][2][1:1] == ["b\\"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end

        @testset "buffersize=200" begin
            testctx = TestContext()
            parse_file(io_t("""
                \xef\xbb\xbfa,b
                "a","b\\\\"
                """),
                nothing,
                testctx,
                force=alg,
                escapechar='\\',
                buffersize=200,
            )
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1][1:1] == [Parsers.PosLen31(6, 1)]
            @test testctx.results[1].cols[2][1:1] == [Parsers.PosLen31(10, 3, false, true)]
            @test testctx.strings[1][1][1:1] == ["a"]
            @test testctx.strings[1][2][1:1] == ["b\\"]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end
    end
end # for (io_t, alg)
for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
    @testset "Sentinels ($(io_t), $(alg))" begin
        for sentinel in ("", "NA", "NULL")
            @testset "sentinel \"$(sentinel)\" int" begin
                testctx = TestContext()
                parse_file(io_t("""
                    a,b,c
                    1,$(sentinel),3
                    3,4,$(sentinel)
                    $(sentinel),$(sentinel),$(sentinel)
                    """),
                    [Int,Int,Int],
                    testctx,
                    force=alg,
                    sentinel=isempty(sentinel) ? missing : [sentinel],
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [Int, Int, Int]
                @test testctx.results[1].cols[1][1:2] == [1,3]
                @test testctx.results[1].cols[2][2] == 4
                @test testctx.results[1].cols[3][1] == 3
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].missing_values[1, 2]
                @test testctx.results[1].missing_values[2, 3]
                @test testctx.results[1].missing_values[3, 1]
                @test testctx.results[1].missing_values[3, 2]
                @test testctx.results[1].missing_values[3, 3]
                @test length(testctx.results[1].cols[1]) == 3
                @test length(testctx.results[1].cols[2]) == 3
                @test length(testctx.results[1].cols[3]) == 3
            end

            @testset "sentinel \"$(sentinel)\" Char" begin
                testctx = TestContext()
                parse_file(io_t("""
                    a,b,c
                    a,$(sentinel),b
                    c,d,$(sentinel)
                    $(sentinel),$(sentinel),$(sentinel)
                    """),
                    [Char,Char,Char],
                    testctx,
                    force=alg,
                    sentinel=isempty(sentinel) ? missing : [sentinel],
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [Char,Char,Char]
                @test testctx.results[1].cols[1][1:2] == ['a','c']
                @test testctx.results[1].cols[2][2] == 'd'
                @test testctx.results[1].cols[3][1] == 'b'
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].missing_values[1, 2]
                @test testctx.results[1].missing_values[2, 3]
                @test testctx.results[1].missing_values[3, 1]
                @test testctx.results[1].missing_values[3, 2]
                @test testctx.results[1].missing_values[3, 3]
                @test length(testctx.results[1].cols[1]) == 3
                @test length(testctx.results[1].cols[2]) == 3
                @test length(testctx.results[1].cols[3]) == 3
            end

            @testset "sentinel \"$(sentinel)\" decimal" begin
                testctx = TestContext()
                parse_file(io_t("""
                    a,b,c
                    1.0,$(sentinel),3E+1
                    3e-1,40,$(sentinel)
                    0.000,$(sentinel),0.000
                    0.000,0.000,$(sentinel)
                    0,$(sentinel),0
                    0,0,$(sentinel)
                    $(sentinel),$(sentinel),$(sentinel)
                    """),
                    [FixedDecimal{Int32,1}, FixedDecimal{UInt32,2}, FixedDecimal{Int64,3}],
                    testctx,
                    sentinel=isempty(sentinel) ? missing : [sentinel],
                    force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [FixedDecimal{Int32,1}, FixedDecimal{UInt32,2}, FixedDecimal{Int64,3}]
                @test testctx.results[1].cols[1][1:6] == [FixedDecimal{Int32,1}(1.0),FixedDecimal{Int32,1}(0.3),FixedDecimal{Int32,1}(0),FixedDecimal{Int32,1}(0),FixedDecimal{Int32,1}(0),FixedDecimal{Int32,1}(0)]
                @test testctx.results[1].cols[2][[2,4,6]] == [FixedDecimal{UInt32,2}(40), FixedDecimal{UInt32,2}(0), FixedDecimal{UInt32,2}(0)]
                @test testctx.results[1].cols[3][[1,3,5]] == [FixedDecimal{Int64,3}(30), FixedDecimal{Int64,3}(0), FixedDecimal{Int64,3}(0)]
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[5] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[6] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[7] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].missing_values[1, 2]
                @test testctx.results[1].missing_values[2, 3]
                @test testctx.results[1].missing_values[3, 2]
                @test testctx.results[1].missing_values[4, 3]
                @test testctx.results[1].missing_values[5, 2]
                @test testctx.results[1].missing_values[6, 3]
                @test testctx.results[1].missing_values[7, 1]
                @test testctx.results[1].missing_values[7, 2]
                @test testctx.results[1].missing_values[7, 3]
                @test length(testctx.results[1].cols[1]) == 7
                @test length(testctx.results[1].cols[2]) == 7
                @test length(testctx.results[1].cols[3]) == 7
            end

            @testset "sentinel \"$(sentinel)\" guess datetime" begin
                testctx = TestContext()
                parse_file(io_t("""
                    a,b,c
                    1990-03-04 00:00:00,$(sentinel),1990-03-05 00:00:00
                    1990-03-04 00:00:00,1990-03-06 00:00:00,$(sentinel)
                    1990-03-04 00:00:00,$(sentinel),1990-03-05 00:00:00+00:00
                    1990-03-04 00:00:00,1990-03-06 00:00:00GMT,$(sentinel)
                    1990-03-04,$(sentinel),1990-03-05
                    1990-03-04,1990-03-06,$(sentinel)
                    $(sentinel),$(sentinel),$(sentinel)
                    """),
                    [GuessDateTime,GuessDateTime,GuessDateTime],
                    testctx,
                    sentinel=isempty(sentinel) ? missing : [sentinel],
                    force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [DateTime,DateTime,DateTime]
                @test testctx.results[1].cols[1][1:6] == [DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4)]
                @test testctx.results[1].cols[2][[2, 4, 6]] == [DateTime(1990, 3, 6), DateTime(1990, 3, 6), DateTime(1990, 3, 6)]
                @test testctx.results[1].cols[3][[1, 3, 5]] == [DateTime(1990, 3, 5), DateTime(1990, 3, 5), DateTime(1990, 3, 5)]
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[5] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[6] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[7] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].missing_values[1, 2]
                @test testctx.results[1].missing_values[2, 3]
                @test testctx.results[1].missing_values[3, 2]
                @test testctx.results[1].missing_values[4, 3]
                @test testctx.results[1].missing_values[5, 2]
                @test testctx.results[1].missing_values[6, 3]
                @test testctx.results[1].missing_values[7, 1]
                @test testctx.results[1].missing_values[7, 2]
                @test testctx.results[1].missing_values[7, 3]
                @test length(testctx.results[1].cols[1]) == 7
                @test length(testctx.results[1].cols[2]) == 7
                @test length(testctx.results[1].cols[3]) == 7
            end

            @testset "sentinel \"$(sentinel)\" datetime" begin
                testctx = TestContext()
                parse_file(io_t("""
                    a,b,c
                    1990-03-04T00:00:00,$(sentinel),1990-03-05T00:00:00
                    1990-03-04T00:00:00,1990-03-06T00:00:00,$(sentinel)
                    1990-03-04T00:00:00,$(sentinel),1990-03-05T00:00:00
                    1990-03-04T00:00:00,1990-03-06T00:00:00,$(sentinel)
                    1990-03-04,$(sentinel),1990-03-05
                    1990-03-04,1990-03-06,$(sentinel)
                    $(sentinel),$(sentinel),$(sentinel)
                    """),
                    [DateTime,DateTime,DateTime],
                    testctx,
                    sentinel=isempty(sentinel) ? missing : [sentinel],
                    force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [DateTime,DateTime,DateTime]
                @test testctx.results[1].cols[1][1:6] == [DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4)]
                @test testctx.results[1].cols[2][[2, 4, 6]] == [DateTime(1990, 3, 6), DateTime(1990, 3, 6), DateTime(1990, 3, 6)]
                @test testctx.results[1].cols[3][[1, 3, 5]] == [DateTime(1990, 3, 5), DateTime(1990, 3, 5), DateTime(1990, 3, 5)]
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[5] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[6] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[7] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].missing_values[1, 2]
                @test testctx.results[1].missing_values[2, 3]
                @test testctx.results[1].missing_values[3, 2]
                @test testctx.results[1].missing_values[4, 3]
                @test testctx.results[1].missing_values[5, 2]
                @test testctx.results[1].missing_values[6, 3]
                @test testctx.results[1].missing_values[7, 1]
                @test testctx.results[1].missing_values[7, 2]
                @test testctx.results[1].missing_values[7, 3]
                @test length(testctx.results[1].cols[1]) == 7
                @test length(testctx.results[1].cols[2]) == 7
                @test length(testctx.results[1].cols[3]) == 7
            end

            @testset "sentinel \"$(sentinel)\" SHA1" begin
                testctx = TestContext()
                s = Parsers.SHA1(Tuple(rand(UInt32, 5)))
                h = string(s)
                parse_file(io_t("""
                    a,b,c
                    $h,$(sentinel),$h
                    $h,$h,$(sentinel)
                    $h,$(sentinel),"$h"
                    $h,$h,$(sentinel)
                    "$h",$(sentinel),$h
                    "$h","$h",$(sentinel)
                    $(sentinel),$(sentinel),$(sentinel)
                    """),
                    [Parsers.SHA1,Parsers.SHA1,Parsers.SHA1],
                    testctx,
                    sentinel=isempty(sentinel) ? missing : [sentinel],
                    force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [Parsers.SHA1,Parsers.SHA1,Parsers.SHA1]
                @test testctx.results[1].cols[1][1:6] == [s, s, s, s, s, s]
                @test testctx.results[1].cols[2][[2, 4, 6]] == [s, s, s]
                @test testctx.results[1].cols[3][[1, 3, 5]] == [s, s, s]
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[5] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[6] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[7] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].missing_values[1, 2]
                @test testctx.results[1].missing_values[2, 3]
                @test testctx.results[1].missing_values[3, 2]
                @test testctx.results[1].missing_values[4, 3]
                @test testctx.results[1].missing_values[5, 2]
                @test testctx.results[1].missing_values[6, 3]
                @test testctx.results[1].missing_values[7, 1]
                @test testctx.results[1].missing_values[7, 2]
                @test testctx.results[1].missing_values[7, 3]
                @test length(testctx.results[1].cols[1]) == 7
                @test length(testctx.results[1].cols[2]) == 7
                @test length(testctx.results[1].cols[3]) == 7
            end

            @testset "sentinel \"$(sentinel)\" UUID" begin
                testctx = TestContext()
                u = UUID(rand(UInt128))
                parse_file(io_t("""
                    a,b,c
                    $u,$(sentinel),$u
                    $u,$u,$(sentinel)
                    $u,$(sentinel),"$u"
                    $u,$u,$(sentinel)
                    "$u",$(sentinel),$u
                    "$u","$u",$(sentinel)
                    $(sentinel),$(sentinel),$(sentinel)
                    """),
                    [UUID,UUID,UUID],
                    testctx,
                    sentinel=isempty(sentinel) ? missing : [sentinel],
                    force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [UUID,UUID,UUID]
                @test testctx.results[1].cols[1][1:6] == [u, u, u, u, u, u]
                @test testctx.results[1].cols[2][[2, 4, 6]] == [u, u, u]
                @test testctx.results[1].cols[3][[1, 3, 5]] == [u, u, u]
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[5] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[6] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].row_statuses[7] == ChunkedCSV.RowStatus.MissingValues
                @test testctx.results[1].missing_values[1, 2]
                @test testctx.results[1].missing_values[2, 3]
                @test testctx.results[1].missing_values[3, 2]
                @test testctx.results[1].missing_values[4, 3]
                @test testctx.results[1].missing_values[5, 2]
                @test testctx.results[1].missing_values[6, 3]
                @test testctx.results[1].missing_values[7, 1]
                @test testctx.results[1].missing_values[7, 2]
                @test testctx.results[1].missing_values[7, 3]
                @test length(testctx.results[1].cols[1]) == 7
                @test length(testctx.results[1].cols[2]) == 7
                @test length(testctx.results[1].cols[3]) == 7
            end
        end
    end

    for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
        @testset "Decimal columns ($(io_t), $(alg))" begin
            for (T, max_precision) in ((Int8,2), (Int16,4), (Int32,8), (Int64,8), (Int128,8), (UInt8,2), (UInt16,4), (UInt32,8), (UInt64,8), (UInt128,8))
                for prec in 0:max_precision
                    S = FixedDecimal{T,prec}
                    testctx = TestContext()
                    parse_file(io_t("""
                        a,b,c
                        0,0,0
                        1,1,1
                        """),
                        [S,S,S],
                        testctx,
                        force=alg,
                    )
                    @test testctx.header == [:a, :b, :c]
                    @test testctx.schema == [S,S,S]
                    @test testctx.results[1].cols[1] == [S(0), S(1)]
                    @test testctx.results[1].cols[2] == [S(0), S(1)]
                end
            end
        end
    end

    for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
        @testset "Row statuses ($(io_t), $(alg))" begin
            RS = ChunkedCSV.RowStatus
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                1,1,1
                2,,2
                2,,
                3,3
                3
                4,4,4,4
                4,4,4,4,4
                garbage,garbage,garbage
                garbage,5,garbage
                # comment
                ,,
                """),
                [Int,Int,Int],
                testctx,
                force=alg,
                comment="# ",
            )
            @test testctx.header == [:a, :b, :c]
            @test testctx.schema == [Int,Int,Int]
            @test testctx.results[1].cols[1][1:7] == [1, 2, 2, 3, 3, 4, 4]
            @test testctx.results[1].cols[2][[1, 4, 6, 7, 9]] == [1, 3, 4, 4, 5]
            @test testctx.results[1].cols[3][[1, 2, 6, 7]] == [1, 2, 4, 4]
            @test testctx.results[1].row_statuses[1] == RS.Ok
            @test testctx.results[1].row_statuses[2] == RS.MissingValues
            @test testctx.results[1].row_statuses[3] == RS.MissingValues
            @test testctx.results[1].row_statuses[4] == RS.TooFewColumns
            @test testctx.results[1].row_statuses[5] == RS.TooFewColumns
            @test testctx.results[1].row_statuses[6] == RS.TooManyColumns
            @test testctx.results[1].row_statuses[7] == RS.TooManyColumns
            @test testctx.results[1].row_statuses[8] == RS.ValueParsingError
            @test testctx.results[1].row_statuses[9] == RS.ValueParsingError
            @test testctx.results[1].row_statuses[10] == RS.MissingValues | RS.SkippedRow
            @test testctx.results[1].row_statuses[11] == RS.MissingValues
            missing_values = collect(testctx.results[1].missing_values)
            @test missing_values[1, 2]                        # 2,,2
            @test missing_values[2,:] == [false, true, true]  # 2,,
            @test all(missing_values[3,:])                    # # comment
            @test all(missing_values[4,:])                    # ,,
            errored_values = collect(testctx.results[1].errored_values)
            @test errored_values[1,:] == [false, false, true] # 3,3
            @test errored_values[2,:] == [false, true, true]  # 3
            @test all(errored_values[3,:])                    # garbage,garbage,garbage
            @test errored_values[4,:] == [true, false, true]  # garbage,1,garbage
        end
    end


    for (io_t, alg) in Iterators.product((iobuffer, iostream, gzip_stream), (:serial, :parallel))
        @testset "Column Skipping & row statuses ($(io_t), $(alg))" begin
            RS = ChunkedCSV.RowStatus
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                1,1,1
                2,,2
                2,,
                3,3
                3
                4,4,4,4
                4,4,4,4,4
                garbage,garbage,garbage
                garbage,5,garbage
                # comment
                ,,
                """),
                [Nothing,Int,Int],
                testctx,
                force=alg,
                comment="# ",
            )
            @test testctx.header == [:b, :c]
            @test testctx.schema == [Int,Int]
            @test testctx.results[1].cols[1][[1, 4, 6, 7, 9]] == [1, 3, 4, 4, 5]
            @test testctx.results[1].cols[2][[1, 2, 6, 7]] == [1, 2, 4, 4]
            @test testctx.results[1].row_statuses[1] == RS.Ok
            @test testctx.results[1].row_statuses[2] == RS.MissingValues
            @test testctx.results[1].row_statuses[3] == RS.MissingValues
            @test testctx.results[1].row_statuses[4] == RS.TooFewColumns
            @test testctx.results[1].row_statuses[5] == RS.TooFewColumns
            @test testctx.results[1].row_statuses[6] == RS.TooManyColumns
            @test testctx.results[1].row_statuses[7] == RS.TooManyColumns
            @test testctx.results[1].row_statuses[8] == RS.ValueParsingError
            @test testctx.results[1].row_statuses[9] == RS.ValueParsingError
            @test testctx.results[1].row_statuses[10] == RS.MissingValues | RS.SkippedRow
            @test testctx.results[1].row_statuses[11] == RS.MissingValues
            missing_values = collect(testctx.results[1].missing_values)
            @test missing_values[1,:] == [true, false] # 2,,2
            @test missing_values[2,:] == [true,true]   # 2,,
            @test missing_values[3,:] == [true,true]   # # comment
            @test missing_values[4,:] == [true,true]   # ,,
            errored_values = collect(testctx.results[1].errored_values)
            @test errored_values[1,:] == [false,true]  # 3,3
            @test errored_values[2,:] == [true,true]   # 3
            @test errored_values[3,:] == [true,true]   # garbage,garbage,garbage
            @test errored_values[4,:] == [false,true]  # garbage,1,garbage

            RS = ChunkedCSV.RowStatus
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                1,1,1
                2,,2
                2,,
                3,3
                3
                4,4,4,4
                4,4,4,4,4
                garbage,garbage,garbage
                garbage,5,garbage
                # comment
                ,,
                """),
                [Int,Nothing,Int],
                testctx,
                force=alg,
                comment="# ",
            )
            @test testctx.header == [:a, :c]
            @test testctx.schema == [Int,Int]
            @test testctx.results[1].cols[1][1:7] == [1, 2, 2, 3, 3, 4, 4]
            @test testctx.results[1].cols[2][[1, 2, 6, 7]] == [1, 2, 4, 4]
            @test testctx.results[1].row_statuses[1] == RS.Ok
            @test testctx.results[1].row_statuses[2] == RS.Ok
            @test testctx.results[1].row_statuses[3] == RS.MissingValues
            @test testctx.results[1].row_statuses[4] == RS.TooFewColumns
            @test testctx.results[1].row_statuses[5] == RS.TooFewColumns
            @test testctx.results[1].row_statuses[6] == RS.TooManyColumns
            @test testctx.results[1].row_statuses[7] == RS.TooManyColumns
            @test testctx.results[1].row_statuses[8] == RS.ValueParsingError
            @test testctx.results[1].row_statuses[9] == RS.ValueParsingError
            @test testctx.results[1].row_statuses[10] == RS.MissingValues | RS.SkippedRow
            @test testctx.results[1].row_statuses[11] == RS.MissingValues
            missing_values = collect(testctx.results[1].missing_values)
            @test missing_values[1,:] == [false, true] # 2,,
            @test missing_values[2,:] == [true, true]  # # comment
            @test missing_values[3,:] == [true, true]  # ,,
            errored_values = collect(testctx.results[1].errored_values)
            @test errored_values[1,:] == [false, true] # 3,3
            @test errored_values[2,:] == [false, true] # 3
            @test errored_values[3,:] == [true, true]  # garbage,garbage,garbage
            @test errored_values[4,:] == [true, true]  # garbage,1,garbage

            RS = ChunkedCSV.RowStatus
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                1,1,1
                2,,2
                2,,
                3,3
                3
                4,4,4,4
                4,4,4,4,4
                garbage,garbage,garbage
                garbage,5,garbage
                # comment
                ,,
                """),
                [Int,Int,Nothing],
                testctx,
                force=alg,
                comment="# ",
            )
            @test testctx.header == [:a, :b]
            @test testctx.schema == [Int,Int]
            @test testctx.results[1].cols[1][1:7] == [1, 2, 2, 3, 3, 4, 4]
            @test testctx.results[1].cols[2][[1, 4, 6, 7, 9]] == [1, 3, 4, 4, 5]
            @test testctx.results[1].row_statuses[1] == RS.Ok
            @test testctx.results[1].row_statuses[2] == RS.MissingValues
            @test testctx.results[1].row_statuses[3] == RS.MissingValues
            @test testctx.results[1].row_statuses[4] == RS.Ok
            @test testctx.results[1].row_statuses[5] == RS.TooFewColumns
            @test testctx.results[1].row_statuses[6] == RS.TooManyColumns
            @test testctx.results[1].row_statuses[7] == RS.TooManyColumns
            @test testctx.results[1].row_statuses[8] == RS.ValueParsingError
            @test testctx.results[1].row_statuses[9] == RS.ValueParsingError
            @test testctx.results[1].row_statuses[10] == RS.MissingValues | RS.SkippedRow
            @test testctx.results[1].row_statuses[11] == RS.MissingValues
            missing_values = collect(testctx.results[1].missing_values)
            @test missing_values[1,:] == [false, true] # 2,,
            @test missing_values[2,:] == [false, true] # 2,,2
            @test missing_values[3,:] == [true, true]  # # comment
            @test missing_values[4,:] == [true, true]  # ,,
            errored_values = collect(testctx.results[1].errored_values)
            @test errored_values[1,:] == [false, true] # 3
            @test errored_values[2,:] == [true, true]  # garbage,garbage,garbage
            @test errored_values[3,:] == [true, false] # garbage,1,garbage

            RS = ChunkedCSV.RowStatus
            testctx = TestContext()
            parse_file(io_t("""
                a,b,c
                1,1,1
                2,,2
                2,,
                3,3
                3
                4,4,4,4
                4,4,4,4,4
                garbage,garbage,garbage
                garbage,5,garbage
                # comment
                ,,
                """),
                [Int,Nothing,Nothing],
                testctx,
                force=alg,
                comment="# ",
            )
            @test testctx.header == [:a]
            @test testctx.schema == [Int]
            @test testctx.results[1].cols[1][1:7] == [1, 2, 2, 3, 3, 4, 4]
            @test testctx.results[1].row_statuses[1] == RS.Ok
            @test testctx.results[1].row_statuses[2] == RS.Ok
            @test testctx.results[1].row_statuses[3] == RS.Ok
            @test testctx.results[1].row_statuses[4] == RS.Ok
            @test testctx.results[1].row_statuses[5] == RS.Ok
            @test testctx.results[1].row_statuses[6] == RS.TooManyColumns
            @test testctx.results[1].row_statuses[7] == RS.TooManyColumns
            @test testctx.results[1].row_statuses[8] == RS.ValueParsingError
            @test testctx.results[1].row_statuses[9] == RS.ValueParsingError
            @test testctx.results[1].row_statuses[10] == RS.MissingValues | RS.SkippedRow
            @test testctx.results[1].row_statuses[11] == RS.MissingValues
            missing_values = collect(testctx.results[1].missing_values)
            @test missing_values[1,:] == [true] # # comment
            @test missing_values[2,:] == [true] # ,,
            errored_values = collect(testctx.results[1].errored_values)
            @test errored_values[1,:] == [true] # garbage,garbage,garbage
            @test errored_values[2,:] == [true] # garbage,1,garbage
        end
    end
end # for (io_t, alg)
