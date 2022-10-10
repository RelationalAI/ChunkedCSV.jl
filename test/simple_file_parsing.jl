using Test
using ChunkedCSV
using ChunkedCSV: TaskResultBuffer, ParsingContext
import Parsers

struct TestContext <: AbstractConsumeContext
    results::Vector{TaskResultBuffer}
    header::Vector{Symbol}
    schema::Vector{DataType}
    lock::ReentrantLock
    rownums::Vector{UInt32}
end
TestContext() = TestContext([], [], [], ReentrantLock(), [])
function ChunkedCSV.consume!(task_buf::TaskResultBuffer{N,M}, parsing_ctx::ParsingContext, row_num::UInt32, eol_idx::UInt32, ctx::TestContext) where {N,M}
    @lock ctx.lock begin
        push!(ctx.results, deepcopy(task_buf))
        isempty(ctx.header) && append!(ctx.header, copy(parsing_ctx.header))
        isempty(ctx.schema) && append!(ctx.schema, copy(parsing_ctx.schema))
        push!(ctx.rownums, row_num)
    end
end

@testset "Simple file parsing" begin
    @testset "simple file, single chunk" begin
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
                    _force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1:2] == [1,3]
                @test testctx.results[1].cols[2].elements[1:2] == [2,4]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
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

    @testset "skiprows" begin
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
                    skiprows=4,
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
            @testset "$alg string" begin
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
                    skiprows=4,
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
            @testset "$alg string" begin
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
                    skiprows=4,
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
                    hasheader=false,
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
                    hasheader=false,
                    skiprows=2,
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
                    hasheader=false,
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
                    hasheader=false,
                    skiprows=2,
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
                    hasheader=false,
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
                    hasheader=false,
                    skiprows=2,
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
                    skiprows=1,
                    hasheader=false,
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
                    hasheader=true,
                    skiprows=1,
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
end

@testset "RFC4180" begin
    # https://www.ietf.org/rfc/rfc4180.txt
    @testset "Each record is located on a separate line, delimited by a line break (CRLF)." begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("aaa,bbb,ccc\nzzz,yyy,xxx\n"), nothing, testctx, _force=alg, hasheader=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(1, 3), Parsers.PosLen(13, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(5, 3), Parsers.PosLen(17, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(9, 3), Parsers.PosLen(21, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("aaa,bbb,ccc\r\nzzz,yyy,xxx\r\n"), nothing, testctx, _force=alg, hasheader=false)
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
                parse_file(IOBuffer("aaa,bbb,ccc\nzzz,yyy,xxx"), nothing, testctx, _force=alg, hasheader=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(1, 3), Parsers.PosLen(13, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(5, 3), Parsers.PosLen(17, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(9, 3), Parsers.PosLen(21, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("aaa,bbb,ccc\r\nzzz,yyy,xxx"), nothing, testctx, _force=alg, hasheader=false)
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
                parse_file(IOBuffer("aaa,bbb,ccc"), nothing, testctx, _force=alg, hasheader=false)
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
                parse_file(IOBuffer("\"aaa\",\"bbb\",\"ccc\"\nzzz,yyy,xxx"), nothing, testctx, _force=alg, hasheader=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(2, 3), Parsers.PosLen(19, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(8, 3), Parsers.PosLen(23, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(14, 3), Parsers.PosLen(27, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("\"aaa\",\"bbb\",\"ccc\"\r\nzzz,yyy,xxx"), nothing, testctx, _force=alg, hasheader=false)
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
                parse_file(IOBuffer("\"aaa\",\"b\nbb\",\"ccc\"\nzzz,yyy,xxx"), nothing, testctx, _force=alg, hasheader=false)
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(2, 3), Parsers.PosLen(20, 3)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(8, 4), Parsers.PosLen(24, 3)]
                @test testctx.results[1].cols[3].elements[1:2] == [Parsers.PosLen(15, 3), Parsers.PosLen(28, 3)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
                @test length(testctx.results[1].cols[3]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("\"aaa\",\"b\r\nbb\",\"ccc\"\r\nzzz,yyy,xxx"), nothing, testctx, _force=alg, hasheader=false)
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
                parse_file(IOBuffer("\"aaa\",\"b\"\"bb\",\"ccc\""), nothing, testctx, _force=alg, hasheader=false)
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
