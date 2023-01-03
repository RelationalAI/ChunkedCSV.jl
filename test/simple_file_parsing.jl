using Test
using ChunkedCSV
using ChunkedCSV: TaskResultBuffer, ParsingContext, RowStatus
import Parsers
using Dates
using FixedPointDecimals

struct TestContext <: AbstractConsumeContext
    results::Vector{TaskResultBuffer}
    header::Vector{Symbol}
    schema::Vector{DataType}
    lock::ReentrantLock
    rownums::Vector{UInt32}
end
TestContext() = TestContext([], [], [], ReentrantLock(), [])
function ChunkedCSV.consume!(ctx::TestContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer{N,M}, row_num::UInt32, eol_idx::UInt32) where {N,M}
    @lock ctx.lock begin
        push!(ctx.results, deepcopy(task_buf))
        isempty(ctx.header) && append!(ctx.header, copy(parsing_ctx.header))
        isempty(ctx.schema) && append!(ctx.schema, copy(parsing_ctx.schema))
        push!(ctx.rownums, row_num)
    end
end
alg=:serial
sentinel=""

@testset "Simple file parsing" begin
    @testset "simple file, single chunk" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg int" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b,c
                    1,2,3
                    3,4,4
                    """),
                    [Int,Int,Int],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [Int, Int, Int]
                @test testctx.results[1].cols[1].elements[1:2] == [1,3]
                @test testctx.results[1].cols[2].elements[1:2] == [2,4]
                @test testctx.results[1].cols[3].elements[1:2] == [3,4]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2
            end

            @testset "$alg float" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    1.0,2.0
                    3.0,4.0
                    """),
                    [Float64, Float64],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Float64, Float64]
                @test testctx.results[1].cols[1].elements[1:2] == [1.0,3.0]
                @test testctx.results[1].cols[2].elements[1:2] == [2.0,4.0]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end

            @testset "$alg decimal" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b,c
                    1.0,-2e-2,1.000924
                    2,3.3e-1,1.00345
                    """),
                    [FixedDecimal{Int32,2},FixedDecimal{Int64,3},FixedDecimal{UInt128,2}],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [FixedDecimal{Int32,2},FixedDecimal{Int64,3}, FixedDecimal{UInt128,2}]
                @test testctx.results[1].cols[1].elements[1:2] == [FixedDecimal{Int32,2}(1.0), FixedDecimal{Int32,2}(2)]
                @test testctx.results[1].cols[2].elements[1:2] == [FixedDecimal{Int64,2}(-2e-2), FixedDecimal{Int64,2}(3.3e-1)]
                @test testctx.results[1].cols[3].elements[1:2] == [FixedDecimal{UInt128,2}(1.0), FixedDecimal{UInt128,2}(1.0)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2
            end

            @testset "$alg date" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    0-01-01,1600-02-29
                    9999-12-31,1904-02-29
                    """),
                    [Date,Date],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Date, Date]
                @test testctx.results[1].cols[1].elements[1:2] == [Date(0, 1, 1), Date(9999, 12, 31)]
                @test testctx.results[1].cols[2].elements[1:2] == [Date(1600, 2, 29), Date(1904, 2, 29)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end

            @testset "$alg datetime" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b,c
                    2000-01-01T10:20:30GMT,1969-07-20,1969-07-20
                    2000-01-01T10:20:30Z,1969-07-20 00:00:00,1969-07-20 00:00:00
                    2000-01-01T10:20:30,1969-07-20 00:00:00.0,1969-07-20 00:00:00.0
                    2000-01-01 10:20:30Z,1969-07-20 00:00:00.00,1969-07-20 00:00:00.00
                    2000-01-01 10:20:30,1969-07-20 00:00:00.000,1969-07-20 00:00:00.000
                    2000-01-01 10:20:30,1969-07-20 00:00:00.000UTC,1969-07-20 00:00:00.000UTC
                    2000-01-01T10:20:30+0000,1969-07-20 00:00:00.000+0000,1969-07-20 00:00:00.000+0000
                    2000-01-01T10:20:30UTC,1969-07-19 17:00:00.000-0700,1969-07-19 17:00:00.000-0700
                    2000-01-01 10:20:30+00:00,1969-07-19 17:00:00.000America/Los_Angeles,1969-07-19 17:00:00.000America/Los_Angeles
                    2000-01-01 10:20:30UTC,1969-07-20 09:00:00.000+0900,1969-07-20 09:00:00.000+0900
                    2000-01-01 02:20:30-0800,1969-07-20 09:00:00.000 Asia/Tokyo,1969-07-20 09:00:00.000 Asia/Tokyo
                    2000-01-01 10:20:30GMT,1969-07-20 00:00:00.000Z,1969-07-20 00:00:00.000Z
                    2000-01-01 10:20:30+00,1969-07-20 00:00:00.00-0000,1969-07-20 00:00:00.00-0000
                    2000-01-01 10:20:30-00,1969-07-20 00:00:00.00-00:00,1969-07-20 00:00:00.00-00:00
                    """),
                    [DateTime,DateTime,DateTime],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [DateTime, DateTime, DateTime]
                @test testctx.results[1].cols[1][1:14] == fill(DateTime(2000,1,1,10,20,30), 14)
                @test testctx.results[1].cols[2][1:14] == fill(DateTime(1969,7,20,00,00,00), 14)
                @test testctx.results[1].cols[3][1:14] == fill(DateTime(1969,7,20,00,00,00), 14)
                @test length(testctx.results[1].cols[1]) == 14
                @test length(testctx.results[1].cols[2]) == 14
                @test length(testctx.results[1].cols[3]) == 14
            end

            @testset "$alg string" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    "1","2"
                    "3","4"
                    """),
                    [String,String],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [String, String]
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(6, 1), Parsers.PosLen(14, 1)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(10, 1), Parsers.PosLen(18, 1)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end
        end
    end

    @testset "simple file, multiple chunks" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg int" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    buffersize=6,
                    _force=alg,
                )
                sort!(testctx.results, by=x->x.cols[1][1])
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 1
                @test testctx.results[1].cols[2].elements[1] == 2
                @test testctx.results[2].cols[1].elements[1] == 3
                @test testctx.results[2].cols[2].elements[1] == 4
                @test length(testctx.results) == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
            end

            @testset "$alg float" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    1.0,2.0
                    3.0,4.0
                    """),
                    [Float64, Float64],
                    testctx,
                    buffersize=10,
                    _force=alg,
                )
                sort!(testctx.results, by=x->x.cols[1][1])
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Float64, Float64]
                @test testctx.results[1].cols[1].elements[1] == 1.0
                @test testctx.results[1].cols[2].elements[1] == 2.0
                @test testctx.results[2].cols[1].elements[1] == 3.0
                @test testctx.results[2].cols[2].elements[1] == 4.0
                @test length(testctx.results) == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
            end

            @testset "$alg string" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    "1","2"
                    "3","4"
                    """),
                    [String,String],
                    testctx,
                    buffersize=8,
                    _force=alg,
                )
                sort!(testctx.results, by=x->x.cols[1][1].pos)
                @test testctx.header == [:a, :b]
                @test testctx.schema == [String, String]
                @test testctx.results[1].cols[1].elements[1] == Parsers.PosLen(2, 1)
                @test testctx.results[1].cols[2].elements[1] == Parsers.PosLen(6, 1)
                @test testctx.results[2].cols[1].elements[1] == Parsers.PosLen(2, 1)
                @test testctx.results[2].cols[2].elements[1] == Parsers.PosLen(6, 1)
                @test length(testctx.results) == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
            end
        end
    end

    @testset "Skipping rows" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg string" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
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
                    _force=alg,
                    header=5,
                )
                sort!(testctx.results, by=x->x.cols[1][1].pos)
                @test testctx.header == [:a, :b]
                @test testctx.schema == [String, String]
                @test testctx.results[1].cols[1].elements[1] == Parsers.PosLen(2, 1)
                @test testctx.results[1].cols[2].elements[1] == Parsers.PosLen(6, 1)
                @test testctx.results[2].cols[1].elements[1] == Parsers.PosLen(2, 1)
                @test testctx.results[2].cols[2].elements[1] == Parsers.PosLen(6, 1)
                @test testctx.results[3].cols[1].elements[1] == Parsers.PosLen(2, 1)
                @test testctx.results[3].cols[2].elements[1] == Parsers.PosLen(6, 1)
                @test length(testctx.results) == 3
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
                @test length(testctx.results[3].cols[1]) == 1
                @test length(testctx.results[3].cols[2]) == 1
            end
            @testset "$alg int" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
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
                    _force=alg,
                    header=5,
                )
                sort!(testctx.results, by=x->x.cols[1][1])
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 1
                @test testctx.results[1].cols[2].elements[1] == 2
                @test testctx.results[2].cols[1].elements[1] == 3
                @test testctx.results[2].cols[2].elements[1] == 4
                @test testctx.results[3].cols[1].elements[1] == 5
                @test testctx.results[3].cols[2].elements[1] == 6
                @test length(testctx.results) == 3
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
                @test length(testctx.results[3].cols[1]) == 1
                @test length(testctx.results[3].cols[2]) == 1
            end
            @testset "$alg float" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
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
                    _force=alg,
                    header=5,
                )
                sort!(testctx.results, by=x->x.cols[1][1])
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Float64, Float64]
                @test testctx.results[1].cols[1].elements[1] == 1.0
                @test testctx.results[1].cols[2].elements[1] == 2.0
                @test testctx.results[2].cols[1].elements[1] == 3.0
                @test testctx.results[2].cols[2].elements[1] == 4.0
                @test testctx.results[3].cols[1].elements[1] == 5.0
                @test testctx.results[3].cols[2].elements[1] == 6.0
                @test length(testctx.results) == 3
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
                @test length(testctx.results[3].cols[1]) == 1
                @test length(testctx.results[3].cols[2]) == 1
            end
        end

        @testset "Comments" begin
            for alg in (:serial, :singlebuffer, :doublebuffer)
                @testset "$alg only comments, single chunk" begin
                    testctx = TestContext()
                    parse_file(IOBuffer("""
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
                        _force=alg
                    )
                    @test testctx.header == [:a, :b]
                    @test testctx.schema == [Int, Int]
                    @test testctx.results[1].cols[1][[1, 3]] == [1, 5]
                    @test testctx.results[1].cols[2][[1, 3]] == [2, 6]

                    @test length(testctx.results) == 1
                    @test testctx.results[1].row_statuses == [RowStatus.Ok, RowStatus.SkippedRow, RowStatus.Ok, RowStatus.SkippedRow]
                    @test length(testctx.results[1].cols[1]) == 4
                    @test length(testctx.results[1].cols[2]) == 4
                end
            end

            for alg in (:serial, :singlebuffer, :doublebuffer)
                @testset "$alg only comments, multiple chunks" begin
                    testctx = TestContext()
                    parse_file(IOBuffer("""
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
                        _force=alg,
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
                    @test testctx.results[2].row_statuses == [RowStatus.SkippedRow]
                    @test testctx.results[3].row_statuses == [RowStatus.Ok]
                    @test testctx.results[4].row_statuses == [RowStatus.SkippedRow]
                    @test length(testctx.results[2].cols[1]) == 1
                    @test length(testctx.results[2].cols[2]) == 1
                    @test length(testctx.results[4].cols[1]) == 1
                    @test length(testctx.results[4].cols[2]) == 1
                end
            end

            for alg in (:serial, :singlebuffer, :doublebuffer)
                @testset "$alg comments and header row, single chunk" begin
                    testctx = TestContext()
                    parse_file(IOBuffer("""
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
                        _force=alg,
                        header=5,
                    )
                    @test testctx.header == [:a, :b]
                    @test testctx.schema == [Int, Int]
                    @test testctx.results[1].cols[1][[1, 3]] == [1, 5]
                    @test testctx.results[1].cols[2][[1, 3]] == [2, 6]

                    @test length(testctx.results) == 1
                    @test testctx.results[1].row_statuses == [RowStatus.Ok, RowStatus.SkippedRow, RowStatus.Ok, RowStatus.SkippedRow]
                    @test length(testctx.results[1].cols[1]) == 4
                    @test length(testctx.results[1].cols[2]) == 4
                end
            end

            for alg in (:serial, :singlebuffer, :doublebuffer)
                @testset "$alg comments and header row, multiple chunks" begin
                    testctx = TestContext()
                    parse_file(IOBuffer("""
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
                        _force=alg,
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
                    @test testctx.results[2].row_statuses == [RowStatus.SkippedRow]
                    @test testctx.results[3].row_statuses == [RowStatus.Ok]
                    @test testctx.results[4].row_statuses == [RowStatus.SkippedRow]
                    @test length(testctx.results[2].cols[1]) == 1
                    @test length(testctx.results[2].cols[2]) == 1
                    @test length(testctx.results[4].cols[1]) == 1
                    @test length(testctx.results[4].cols[2]) == 1
                end
            end

            for alg in (:serial, :singlebuffer, :doublebuffer)
                @testset "$alg comments, header row and data row, single chunk" begin
                    testctx = TestContext()
                    parse_file(IOBuffer("""
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
                        _force=alg,
                        header=5,
                        skipto=9,
                    )
                    @test testctx.header == [:a, :b]
                    @test testctx.schema == [Int, Int]
                    @test testctx.results[1].cols[1][[1, 3]] == [1, 5]
                    @test testctx.results[1].cols[2][[1, 3]] == [2, 6]

                    @test length(testctx.results) == 1
                    @test testctx.results[1].row_statuses == [RowStatus.Ok, RowStatus.SkippedRow, RowStatus.Ok, RowStatus.SkippedRow]
                    @test length(testctx.results[1].cols[1]) == 4
                    @test length(testctx.results[1].cols[2]) == 4
                end
            end

            for alg in (:serial, :singlebuffer, :doublebuffer)
                @testset "$alg comments, header row and data row, multiple chunks" begin
                    testctx = TestContext()
                    parse_file(IOBuffer("""
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
                        _force=alg,
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
                    @test testctx.results[2].row_statuses == [RowStatus.SkippedRow]
                    @test testctx.results[3].row_statuses == [RowStatus.Ok]
                    @test testctx.results[4].row_statuses == [RowStatus.SkippedRow]
                    @test length(testctx.results[2].cols[1]) == 1
                    @test length(testctx.results[2].cols[2]) == 1
                    @test length(testctx.results[4].cols[1]) == 1
                    @test length(testctx.results[4].cols[2]) == 1
                end
            end

            for alg in (:serial, :singlebuffer, :doublebuffer)
                @testset "$alg file with header and comments only" begin
                    testctx = TestContext()
                    parse_file(IOBuffer("""
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
                        _force=alg,
                        header=5,
                        skipto=8,
                        buffersize=5,
                    )
                    @test testctx.header == [:a, :b]
                    @test testctx.schema == [Int, Int]
                    @test length(testctx.results) == 1
                    @test all(isempty, testctx.results[1].cols)
                end
            end

            for alg in (:serial, :singlebuffer, :doublebuffer)
                @testset "$alg file with comments only" begin
                    testctx = TestContext()
                    parse_file(IOBuffer("""
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
                        _force=alg,
                        header=false,
                        buffersize=5,
                    )
                    @test testctx.header == [:COL_1, :COL_2]
                    @test testctx.schema == [Int, Int]
                    @test length(testctx.results) == 1
                    @test all(isempty, testctx.results[1].cols)
                end
            end
        end
    end
    @testset "No header, no schema" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    1,2
                    3,4
                    """),
                    nothing,
                    testctx,
                    header=false,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [String, String]
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(1, 1), Parsers.PosLen(5, 1)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(3, 1), Parsers.PosLen(7, 1)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("""
                    xxx
                    xxx
                    1,2
                    3,4
                    """),
                    nothing,
                    testctx,
                    header=false,
                    skipto=3,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [String, String]
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(9, 1), Parsers.PosLen(13, 1)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(11, 1), Parsers.PosLen(15, 1)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end
        end
    end

    @testset "No header" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    header=false,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1:2] == [1,3]
                @test testctx.results[1].cols[2].elements[1:2] == [2,4]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("""
                    xxx
                    xxx
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    header=false,
                    skipto=3,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1:2] == [1,3]
                @test testctx.results[1].cols[2].elements[1:2] == [2,4]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end
        end
    end

    @testset "limit" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    limit=1,
                    header=false,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 1
                @test testctx.results[1].cols[2].elements[1] == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1

                testctx = TestContext()
                parse_file(IOBuffer("""
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
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 1
                @test testctx.results[1].cols[2].elements[1] == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
            end
        end
    end

    @testset "limit and skiprow" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    1,2
                    3,4
                    5,6
                    """),
                    [Int,Int],
                    testctx,
                    limit=1,
                    skipto=2,
                    header=false,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 3
                @test testctx.results[1].cols[2].elements[1] == 4
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1

                testctx = TestContext()
                parse_file(IOBuffer("""
                    xxx
                    a,b
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    limit=1,
                    header=2,
                    _force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 1
                @test testctx.results[1].cols[2].elements[1] == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
            end
        end
    end


    @testset "Opt out of validation" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            a,b,c
            1,2,3
            3,4,5
            """),
            Dict(:q => Int, :b => Int),
            testctx,
            header=[:a, :b, :c],
            validate_type_map=false,
        )
        @test testctx.header == [:a, :b, :c]
        @test testctx.schema == [String, Int, String]

        testctx = TestContext()
        parse_file(IOBuffer("""
            a,b,c
            1,2,3
            3,4,5
            """),
            Dict(:q => Int, :b => Int),
            testctx,
            header=1,
            validate_type_map=false,
        )
        @test testctx.header == [:a, :b, :c]
        @test testctx.schema == [String, Int, String]
    end

    @testset "Empty string field" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b,c
                    "","",""
                    """),
                    [String,String,String],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [String, String, String]
                # https://github.com/JuliaData/Parsers.jl/issues/138
                @test testctx.results[1].cols[1].elements[1] == Parsers.PosLen(8,0)
                @test testctx.results[1].cols[2].elements[1] == Parsers.PosLen(11,0)
                @test testctx.results[1].cols[3].elements[1] == Parsers.PosLen(14,0)
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[1].cols[3]) == 1

                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b,c
                    ,,"""),
                    [String,String,String],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [String, String, String]
                # https://github.com/JuliaData/Parsers.jl/issues/138
                @test testctx.results[1].cols[1].elements[1] == Parsers.PosLen(7,0)
                @test testctx.results[1].cols[2].elements[1] == Parsers.PosLen(8,0)
                @test testctx.results[1].cols[3].elements[1] == Parsers.PosLen(9,0)
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[1].cols[3]) == 1
            end
        end
    end

    @testset "Char field" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b,c
                    "a","b","c"
                    a,b,c
                    """),
                    [Char,Char,Char],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [Char, Char, Char]
                @test testctx.results[1].cols[1].elements[1] == 'a'
                @test testctx.results[1].cols[2].elements[1] == 'b'
                @test testctx.results[1].cols[3].elements[1] == 'c'
                @test testctx.results[1].cols[1].elements[2] == 'a'
                @test testctx.results[1].cols[2].elements[2] == 'b'
                @test testctx.results[1].cols[3].elements[2] == 'c'
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2
            end
        end
    end

    @testset "Bool field" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
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
                    _force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Bool, Bool]
                @test testctx.results[1].cols[1] == fill(true, 10)
                @test testctx.results[1].cols[2] == fill(false, 10)
                @test length(testctx.results[1].cols[1]) == 10
                @test length(testctx.results[1].cols[2]) == 10
            end
        end
    end
end

@testset "Empty input" begin
    for alg in [:serial, :singlebuffer, :doublebuffer]
        @testset "$alg, no file header, no provided header, no schema" begin
            testctx = TestContext()
            parse_file(IOBuffer(""), nothing, testctx, _force=alg, header=false)
            @test isempty(testctx.results[1].cols)
            @test isempty(testctx.header)
            @test isempty(testctx.schema)
        end

        @testset "$alg, no file header, has provided header, no schema" begin
            testctx = TestContext()
            parse_file(IOBuffer(""), nothing, testctx, _force=alg, header=[:A, :B])
            @test length(testctx.results[1].cols) == 2
            @test testctx.header == [:A, :B]
            @test testctx.schema == [String, String]
        end

        @testset "$alg, no file header, no provided header, has schema" begin
            testctx = TestContext()
            parse_file(IOBuffer(""), [Int, String], testctx, _force=alg, header=false)
            @test length(testctx.results[1].cols) == 2
            @test testctx.header == [:COL_1, :COL_2]
            @test testctx.schema == [Int, String]
        end

        @testset "$alg, has file header, has provided header, has schema" begin
            testctx = TestContext()
            parse_file(IOBuffer(""), [Int, String], testctx, _force=alg, header=[:A, :B])
            @test length(testctx.results[1].cols) == 2
            @test testctx.header == [:A, :B]
            @test testctx.schema == [Int, String]
        end

        @testset "$alg, has file header, no provided header, no schema" begin
            testctx = TestContext()
            parse_file(IOBuffer(""), nothing, testctx, _force=alg, header=true)
            @test isempty(testctx.results[1].cols)
            @test isempty(testctx.header)
            @test isempty(testctx.schema)
        end

        @testset "$alg, has file header, no provided header, has schema" begin
            testctx = TestContext()
           parse_file(IOBuffer(""), [Int, String], testctx, _force=alg, header=true)
           @test length(testctx.results[1].cols) == 2
           @test testctx.header == [:COL_1, :COL_2]
           @test testctx.schema == [Int, String]
        end
    end
end

@testset "RFC4180" begin
    # https://www.ietf.org/rfc/rfc4180.txt
    @testset "Each record is located on a separate line, delimited by a line break (CRLF)." begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("aaa,bbb,ccc\nzzz,yyy,xxx\n"), nothing, testctx, _force=alg, header=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(1, 3), Parsers.PosLen(13, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(5, 3), Parsers.PosLen(17, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(9, 3), Parsers.PosLen(21, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("aaa,bbb,ccc\r\nzzz,yyy,xxx\r\n"), nothing, testctx, _force=alg, header=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(1, 3), Parsers.PosLen(14, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(5, 3), Parsers.PosLen(18, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(9, 3), Parsers.PosLen(22, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2
            end
        end
    end

    @testset "The last record in the file may or may not have an ending line break." begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("aaa,bbb,ccc\nzzz,yyy,xxx"), nothing, testctx, _force=alg, header=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(1, 3), Parsers.PosLen(13, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(5, 3), Parsers.PosLen(17, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(9, 3), Parsers.PosLen(21, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("aaa,bbb,ccc\r\nzzz,yyy,xxx"), nothing, testctx, _force=alg, header=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(1, 3), Parsers.PosLen(14, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(5, 3), Parsers.PosLen(18, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(9, 3), Parsers.PosLen(22, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2
            end
        end
    end

    @testset """
        There maybe an optional header line appearing as the first line
        of the file with the same format as normal record lines.  This
        header will contain names corresponding to the fields in the file
        and should contain the same number of fields as the records in
        the rest of the file (the presence or absence of the header line
        should be indicated via the optional "header" parameter of this
        MIME type).""" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("field_name,field_name,field_name\naaa,bbb,ccc\nzzz,yyy,xxx"), nothing, testctx, _force=alg)
                @test testctx.header == [:field_name, :field_name, :field_name]
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(33+1, 3), Parsers.PosLen(33+13, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(33+5, 3), Parsers.PosLen(33+17, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(33+9, 3), Parsers.PosLen(33+21, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("field_name,field_name,field_name\r\naaa,bbb,ccc\r\nzzz,yyy,xxx"), nothing, testctx, _force=alg)
                @test testctx.header == [:field_name, :field_name, :field_name]
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(34+1, 3), Parsers.PosLen(34+14, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(34+5, 3), Parsers.PosLen(34+18, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(34+9, 3), Parsers.PosLen(34+22, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2
            end
        end
    end

    @testset """
        Within the header and each record, there may be one or more
        fields, separated by commas.  Each line should contain the same
        number of fields throughout the file.  Spaces are considered part
        of a field and should not be ignored.  The last field in the
        record must not be followed by a comma.
        """ begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("aaa,bbb,ccc"), nothing, testctx, _force=alg, header=false)
                @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(1, 3)]
                @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(5, 3)]
                @test testctx.results[1].cols[3].elements[1:1] == [Parsers.PosLen(9, 3)]
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[1].cols[3]) == 1
            end
        end
    end

    @testset """
        Each field may or may not be enclosed in double quotes (however
        some programs, such as Microsoft Excel, do not use double quotes
        at all).  If fields are not enclosed with double quotes, then
        double quotes may not appear inside the fields. """ begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("\"aaa\",\"bbb\",\"ccc\"\nzzz,yyy,xxx"), nothing, testctx, _force=alg, header=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(2, 3), Parsers.PosLen(19, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(8, 3), Parsers.PosLen(23, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(14, 3), Parsers.PosLen(27, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("\"aaa\",\"bbb\",\"ccc\"\r\nzzz,yyy,xxx"), nothing, testctx, _force=alg, header=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(2, 3), Parsers.PosLen(20, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(8, 3), Parsers.PosLen(24, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(14, 3), Parsers.PosLen(28, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2
            end
        end
    end

    @testset """
        Fields containing line breaks (CRLF), double quotes, and commas
        should be enclosed in double-quotes.""" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("\"aaa\",\"b\nbb\",\"ccc\"\nzzz,yyy,xxx"), nothing, testctx, _force=alg, header=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(2, 3), Parsers.PosLen(20, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(8, 4), Parsers.PosLen(24, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(15, 3), Parsers.PosLen(28, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("\"aaa\",\"b\r\nbb\",\"ccc\"\r\nzzz,yyy,xxx"), nothing, testctx, _force=alg, header=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(2, 3), Parsers.PosLen(22, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(8, 5), Parsers.PosLen(26, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(16, 3), Parsers.PosLen(30, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2
            end
        end
    end

    @testset """
        If double-quotes are used to enclose fields, then a double-quote
        appearing inside a field must be escaped by preceding it with
        another double quote. """ begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("\"aaa\",\"b\"\"bb\",\"ccc\""), nothing, testctx, _force=alg, header=false)
                @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(2, 3)]
                @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(8, 5, false, true)]
                @test testctx.results[1].cols[3].elements[1:1] == [Parsers.PosLen(16, 3)]
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[1].cols[3]) == 1
            end
        end
    end
end

@testset "Whitespace" begin
    @testset "Unquoted string fields preserve whitespace" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""a, b\n  foo  , "bar"    \n"""), nothing, testctx, _force=alg)
                @test testctx.header == [:a, :b]
                @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(6, 7)]
                @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(16, 3)]
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
            end
        end
    end

    @testset "Whitespace surrounding quoted fields is stripped" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""a, b\n  "foo"  , "bar"    \n"""), nothing, testctx, _force=alg)
                @test testctx.header == [:a, :b]
                @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(9, 3)]
                @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(18, 3)]
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1

                testctx = TestContext()
                parse_file(IOBuffer("""a, b\n"foo"  , "bar"    \n"""), nothing, testctx, _force=alg)
                @test testctx.header == [:a, :b]
                @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(7, 3)]
                @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(16, 3)]
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
            end
        end
    end

    @testset "Newlines inside a quoted field are handled properly" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""a, b\n     "foo", "bar\n     acsas"\n"""), nothing, testctx, _force=alg)
                @test testctx.header == [:a, :b]
                @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(12, 3)]
                @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(19, 14)]
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1

                testctx = TestContext()
                parse_file(IOBuffer("""a, b\n     "foo", "bar\n\r     acsas"\n"""), nothing, testctx, _force=alg)
                @test testctx.header == [:a, :b]
                @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(12, 3)]
                @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(19, 15)]
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
            end
        end
    end

    @testset "Escaped quotes inside a quoted field are handled properly" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""a, b\n"foo"  ,"The cat said, ""meow"" "\n"""), nothing, testctx, _force=alg)
                @test testctx.header == [:a, :b]
                @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(7, 3)]
                @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(15, 23, false, true)]
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
            end
        end

        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""a, b\n"foo"  ,"The cat said, \\"meow\\" "\n"""), nothing, testctx, _force=alg, escapechar='\\')
                @test testctx.header == [:a, :b]
                @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(7, 3)]
                @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(15, 23, false, true)]
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
            end
        end
    end

    @testset "Characters outside of a quoted field should be marked as ValueParsingError" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""a, b\n"foo"  , "bar"         234235\n"""), nothing, testctx, _force=alg)
                @test testctx.header == [:a, :b]
                @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(7, 3)]
                @test testctx.results[1].row_statuses[1] & ChunkedCSV.RowStatus.ValueParsingError > 0
                @test ChunkedCSV.isflagset(testctx.results[1].column_indicators[1], 2)
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
            end
        end
    end
end

@testset "default_colname_prefix" begin
    testctx = TestContext()
    parse_file(IOBuffer("""
        1,2
        3,4
        """),
        [Int,Int],
        testctx,
        header=false,
        default_colname_prefix="#"
    )
    @test testctx.header == [Symbol("#1"), Symbol("#2")]


    testctx = TestContext()
    parse_file(IOBuffer("""
        1,2
        3,4
        """),
        nothing,
        testctx,
        header=false,
        default_colname_prefix="##"
    )
    @test testctx.header == [Symbol("##1"), Symbol("##2")]
end

@testset "escaped quote outside of quoted field" begin
    testctx = TestContext()
    parse_file(IOBuffer("""
        a|b
        1|"2\\""
        3|4
        """),
        nothing,
        testctx,
        escapechar='\\',
        delim='|',
    )
    @test testctx.header == [:a, :b]
    @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(5, 1), Parsers.PosLen(13, 1)]
    @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(8, 3, false, true), Parsers.PosLen(15, 1)]
    @test length(testctx.results[1].cols[1]) == 2
    @test length(testctx.results[1].cols[2]) == 2

    testctx = TestContext()
    parse_file(IOBuffer("""
        a|b
        1|"2\\"\""""),
        nothing,
        testctx,
        escapechar='\\',
        delim='|',
    )
    @test testctx.header == [:a, :b]
    @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(5, 1)]
    @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(8, 3, false, true)]
    @test length(testctx.results[1].cols[1]) == 1
    @test length(testctx.results[1].cols[2]) == 1
end

@testset "Opening and closing quotes" begin
    testctx = TestContext()
    parse_file(IOBuffer("""
        a,b
        1,S2E
        3,S"ES4"EE
        """),
        nothing,
        testctx,
        openquotechar='S',
        closequotechar='E',
    )
    @test testctx.header == [:a, :b]
    @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(5, 1), Parsers.PosLen(11, 1)]
    @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(8, 1), Parsers.PosLen(14, 6, false, true)]
    @test length(testctx.results[1].cols[1]) == 2
    @test length(testctx.results[1].cols[2]) == 2
end

@testset "Two escape characters before closing quote" begin
    for alg in [:serial, :singlebuffer, :doublebuffer]
        @testset "$alg" begin
            testctx = TestContext()
            parse_file(IOBuffer("""
                a,b
                "a","b\\\\"
                """),
                nothing,
                testctx,
                _force=alg,
                escapechar='\\'
            )
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(6, 1)]
            @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(10, 3, false, true)]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end
    end
end

@testset "BOM" begin
    for alg in [:serial, :singlebuffer, :doublebuffer]
        @testset "$alg" begin
            testctx = TestContext()
            parse_file(IOBuffer("""
                \xef\xbb\xbfa,b
                "a","b\\\\"
                """),
                nothing,
                testctx,
                _force=alg,
                escapechar='\\'
            )
            @test testctx.header == [:a, :b]
            @test testctx.results[1].cols[1].elements[1:1] == [Parsers.PosLen(6, 1)]
            @test testctx.results[1].cols[2].elements[1:1] == [Parsers.PosLen(10, 3, false, true)]
            @test length(testctx.results[1].cols[1]) == 1
            @test length(testctx.results[1].cols[2]) == 1
        end
    end
end

@testset "sentinels" begin
    for sentinel in ("", "NA", "NULL")
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg sentinel \"$(sentinel)\" int" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b,c
                    1,$(sentinel),3
                    3,4,$(sentinel)
                    $(sentinel),$(sentinel),$(sentinel)
                    """),
                    [Int,Int,Int],
                    testctx,
                    _force=alg,
                    sentinel=isempty(sentinel) ? missing : [sentinel],
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [Int, Int, Int]
                @test testctx.results[1].cols[1].elements[1:2] == [1,3]
                @test testctx.results[1].cols[2].elements[2] == 4
                @test testctx.results[1].cols[3].elements[1] == 3
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].column_indicators[1] == UInt8(1) << 1
                @test testctx.results[1].column_indicators[2] == UInt8(1) << 2
                @test testctx.results[1].column_indicators[3] == (UInt8(1) << 0) | (UInt8(1) << 1) | (UInt8(1) << 2)
                @test length(testctx.results[1].cols[1]) == 3
                @test length(testctx.results[1].cols[2]) == 3
                @test length(testctx.results[1].cols[3]) == 3
            end

            @testset "$alg sentinel \"$(sentinel)\" Char" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b,c
                    a,$(sentinel),b
                    c,d,$(sentinel)
                    $(sentinel),$(sentinel),$(sentinel)
                    """),
                    [Char,Char,Char],
                    testctx,
                    _force=alg,
                    sentinel=isempty(sentinel) ? missing : [sentinel],
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [Char,Char,Char]
                @test testctx.results[1].cols[1].elements[1:2] == ['a','c']
                @test testctx.results[1].cols[2].elements[2] == 'd'
                @test testctx.results[1].cols[3].elements[1] == 'b'
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].column_indicators[1] == UInt8(1) << 1
                @test testctx.results[1].column_indicators[2] == UInt8(1) << 2
                @test testctx.results[1].column_indicators[3] == (UInt8(1) << 0) | (UInt8(1) << 1) | (UInt8(1) << 2)
                @test length(testctx.results[1].cols[1]) == 3
                @test length(testctx.results[1].cols[2]) == 3
                @test length(testctx.results[1].cols[3]) == 3
            end

            @testset "$alg sentinel \"$(sentinel)\" decimal" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
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
                    _force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [FixedDecimal{Int32,1}, FixedDecimal{UInt32,2}, FixedDecimal{Int64,3}]
                @test testctx.results[1].cols[1].elements[1:6] == [FixedDecimal{Int32,1}(1.0),FixedDecimal{Int32,1}(0.3),FixedDecimal{Int32,1}(0),FixedDecimal{Int32,1}(0),FixedDecimal{Int32,1}(0),FixedDecimal{Int32,1}(0)]
                @test testctx.results[1].cols[2].elements[[2,4,6]] == [FixedDecimal{UInt32,2}(40), FixedDecimal{UInt32,2}(0), FixedDecimal{UInt32,2}(0)]
                @test testctx.results[1].cols[3].elements[[1,3,5]] == [FixedDecimal{Int64,3}(30), FixedDecimal{Int64,3}(0), FixedDecimal{Int64,3}(0)]
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[5] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[6] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[7] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].column_indicators[1] == UInt8(1) << 1
                @test testctx.results[1].column_indicators[2] == UInt8(1) << 2
                @test testctx.results[1].column_indicators[3] == UInt8(1) << 1
                @test testctx.results[1].column_indicators[4] == UInt8(1) << 2
                @test testctx.results[1].column_indicators[5] == UInt8(1) << 1
                @test testctx.results[1].column_indicators[6] == UInt8(1) << 2
                @test testctx.results[1].column_indicators[7] == (UInt8(1) << 0) | (UInt8(1) << 1) | (UInt8(1) << 2)
                @test length(testctx.results[1].cols[1]) == 7
                @test length(testctx.results[1].cols[2]) == 7
                @test length(testctx.results[1].cols[3]) == 7
            end

            @testset "$alg sentinel \"$(sentinel)\" datetime" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b,c
                    1990-03-04 00:00:00,$(sentinel),1990-03-05 00:00:00
                    1990-03-04 00:00:00,1990-03-06 00:00:00,$(sentinel)
                    1990-03-04 00:00:00,$(sentinel),1990-03-05 00:00:00+00:00
                    1990-03-04 00:00:00,1990-03-06 00:00:00GMT,$(sentinel)
                    1990-03-04,$(sentinel),1990-03-05
                    1990-03-04,1990-03-06,$(sentinel)
                    $(sentinel),$(sentinel),$(sentinel)
                    """),
                    [DateTime,DateTime,DateTime],
                    testctx,
                    sentinel=isempty(sentinel) ? missing : [sentinel],
                    _force=alg,
                )
                @test testctx.header == [:a, :b, :c]
                @test testctx.schema == [DateTime,DateTime,DateTime]
                @test testctx.results[1].cols[1].elements[1:6] == [DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4), DateTime(1990, 3, 4)]
                @test testctx.results[1].cols[2].elements[[2, 4, 6]] == [DateTime(1990, 3, 6), DateTime(1990, 3, 6), DateTime(1990, 3, 6)]
                @test testctx.results[1].cols[3].elements[[1, 3, 5]] == [DateTime(1990, 3, 5), DateTime(1990, 3, 5), DateTime(1990, 3, 5)]
                @test testctx.results[1].row_statuses[1] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[2] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[3] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[5] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[6] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].row_statuses[7] == ChunkedCSV.RowStatus.HasColumnIndicators
                @test testctx.results[1].column_indicators[1] == UInt8(1) << 1
                @test testctx.results[1].column_indicators[2] == UInt8(1) << 2
                @test testctx.results[1].column_indicators[3] == UInt8(1) << 1
                @test testctx.results[1].column_indicators[4] == UInt8(1) << 2
                @test testctx.results[1].column_indicators[5] == UInt8(1) << 1
                @test testctx.results[1].column_indicators[6] == UInt8(1) << 2
                @test testctx.results[1].column_indicators[7] == (UInt8(1) << 0) | (UInt8(1) << 1) | (UInt8(1) << 2)
                @test length(testctx.results[1].cols[1]) == 7
                @test length(testctx.results[1].cols[2]) == 7
                @test length(testctx.results[1].cols[3]) == 7
            end
        end
    end
end

@testset "Ending on an escapechar" begin
    for buffersize in (12, 13, 14, 15)
        # buffersize 12 ends the first buffer on letter S
        testctx = TestContext()
        parse_file(IOBuffer("""
            a,b
            0,z
            1,"S\"\"\"
            """),
            [Int,String],
            testctx,
            buffersize=buffersize,
            escapechar='"',
        )
        @test testctx.header == [:a, :b]
        @test testctx.schema == [Int,String]
        @test length(testctx.results) == 2
        @test testctx.results[1].cols[1][1] == 0
        @test testctx.results[2].cols[1][1] == 1
        @test testctx.results[1].cols[2][1] == PosLen(7,1)
        @test testctx.results[2].cols[2][1] == PosLen(4,3,false,true)

        testctx = TestContext()
        parse_file(IOBuffer("""
            a,b
            0,z
            1,"S\\\\\"
            """),
            [Int,String],
            testctx,
            buffersize=buffersize,
            escapechar='\\',
        )
        @test testctx.header == [:a, :b]
        @test testctx.schema == [Int,String]
        @test length(testctx.results) == 2
        @test testctx.results[1].cols[1][1] == 0
        @test testctx.results[2].cols[1][1] == 1
        @test testctx.results[1].cols[2][1] == PosLen(7,1)
        @test testctx.results[2].cols[2][1] == PosLen(4,3,false,true)

        testctx = TestContext()
        parse_file(IOBuffer("""
            a,b
            0,z
            1,zS\\"\\"
            """),
            [Int,String],
            testctx,
            buffersize=buffersize,
            escapechar='\\',
        )
        @test testctx.header == [:a, :b]
        @test testctx.schema == [Int,String]
        @test length(testctx.results) == 2
        @test testctx.results[1].cols[1][1] == 0
        @test testctx.results[2].cols[1][1] == 1
        @test testctx.results[1].cols[2][1] == PosLen(7,1)
        @test testctx.results[2].cols[2][1] == PosLen(3,6,false,false)
    end
end

@testset "Carriage return" begin
    @testset "ints" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            a,b
            0,1\r
            1,2\r
            2,3\r
            3,\r
            """),
            [Int,Int],
            testctx,
        )
        @test testctx.header == [:a, :b]
        @test testctx.schema == [Int,Int]
        @test length(testctx.results) == 1
        @test testctx.results[1].cols[1][1:4] == 0:3
        @test testctx.results[1].cols[2][1:3] == 1:3
        @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.HasColumnIndicators
        @test testctx.results[1].column_indicators[1] == UInt8(1) << 1
    end

    @testset "decimals" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            a,b
            0,1\r
            1,1.0\r
            2,10.0e-1\r
            3,\r
            """),
            [Int,FixedDecimal{Int,4}],
            testctx,
        )
        @test testctx.header == [:a, :b]
        @test testctx.schema == [Int,FixedDecimal{Int,4}]
        @test length(testctx.results) == 1
        @test testctx.results[1].cols[1][1:4] == 0:3
        @test testctx.results[1].cols[2][1:3] == fill(FixedDecimal{Int,4}(1), 3)
        @test testctx.results[1].row_statuses[4] == ChunkedCSV.RowStatus.HasColumnIndicators
        @test testctx.results[1].column_indicators[1] == UInt8(1) << 1
    end

    @testset "datetimes" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            a,b
            0,1969-07-20\r
            1,1969-07-20 00:00:00\r
            2,1969-07-20 00:00:00.00\r
            3,1969-07-20 00:00:00.000UTC\r
            4,1969-07-19 17:00:00.000America/Los_Angeles\r
            5,1969-07-20 00:00:00.00-0000\r
            6,\r
            """),
            [Int,DateTime],
            testctx,
        )
        @test testctx.header == [:a, :b]
        @test testctx.schema == [Int,DateTime]
        @test length(testctx.results) == 1
        @test testctx.results[1].cols[1][1:7] == 0:6
        @test testctx.results[1].cols[2][1:6] == fill(DateTime(1969, 7, 20), 6)
        @test testctx.results[1].row_statuses[7] == ChunkedCSV.RowStatus.HasColumnIndicators
        @test testctx.results[1].column_indicators[1] == UInt8(1) << 1
    end
end

@testset "Quoted fields" begin
    @testset "floats" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            a,b,c,d,e
            0,1,"1",1,"1"\r
            1,1.0,"1.0",1.0,"1.0"
            2,10.0e-1,"10.0e-1",10.0e-1,"10.0e-1"\r
            """),
            [Int,Float64,Float64,Float64,Float64],
            testctx,
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

    @testset "datetimes" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            a,b,c,d,e
            0,1969-07-20,"1969-07-20",1969-07-20,"1969-07-20"
            1,1969-07-20 00:00:00,"1969-07-20 00:00:00",1969-07-20 00:00:00,"1969-07-20 00:00:00"\r
            2,1969-07-20 00:00:00.00,"1969-07-20 00:00:00.00",1969-07-20 00:00:00.00,"1969-07-20 00:00:00.00"
            3,1969-07-20 00:00:00.000UTC,"1969-07-20 00:00:00.000UTC",1969-07-20 00:00:00.000UTC,"1969-07-20 00:00:00.000UTC"
            4,1969-07-19 17:00:00.000America/Los_Angeles,"1969-07-19 17:00:00.000America/Los_Angeles",1969-07-19 17:00:00.000America/Los_Angeles,"1969-07-19 17:00:00.000America/Los_Angeles"\r
            5,1969-07-20 00:00:00.00-0000,"1969-07-20 00:00:00.00-0000",1969-07-20 00:00:00.00-0000,"1969-07-20 00:00:00.00-0000"
            6,1969-07-20 00:00:00.00Z,"1969-07-20 00:00:00.00Z","1969-07-20 00:00:00.00Z","1969-07-20T00:00:00Z"
            """),
            [Int,DateTime,DateTime,DateTime,DateTime],
            testctx,
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
        parse_file(IOBuffer("""
            a,b,c,d,e
            0,1,"1",1,"1"\r
            1,1.0,"1.0",1.0,"1.0"
            2,10.0e-1,"10.0e-1",10.0e-1,"10.0e-1"\r
            """),
            [Int,FixedDecimal{Int,4},FixedDecimal{Int,4},FixedDecimal{Int,4},FixedDecimal{Int,4}],
            testctx,
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
end
