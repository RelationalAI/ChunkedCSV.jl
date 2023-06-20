using ChunkedCSV
using Test
using Parsers

@testset "newline" begin
    testctx = ChunkedCSV.TestContext()
    ChunkedCSV.parse_file(IOBuffer("a,b,c\ne,f,g\n"), [String, String, String], testctx, newlinechar=nothing)
    @test testctx.header == [:a, :b, :c]
    @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31, Parsers.PosLen31]
    @test testctx.strings[1] == [["e"], ["f"], ["g"]]

    testctx = ChunkedCSV.TestContext()
    ChunkedCSV.parse_file(IOBuffer("a,b,c\re,f,g\r"), [String, String, String], testctx, newlinechar=nothing)
    @test testctx.header == [:a, :b, :c]
    @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31, Parsers.PosLen31]
    @test testctx.strings[1] == [["e"], ["f"], ["g"]]

    testctx = ChunkedCSV.TestContext()
    parse_file(IOBuffer("a,b,c\r\ne,f,g\r\n"), [String, String, String], testctx, newlinechar=nothing)
    @test testctx.header == [:a, :b, :c]
    @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31, Parsers.PosLen31]
    @test testctx.strings[1] == [["e"], ["f"], ["g"]]
end

@testset "detect_delim" begin
    Q = UInt8('"')
    E = UInt8('\\')
    s = b"a,b,c\ne,f,g\ni,j,k\n"
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, E, true) == UInt8(',')

    # Only ChunkedCSV.CANDIDATE_DELIMS are considered
    s = b"a,a,a\na,a,a\na,a,a\n"
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, E, true) == UInt8(',')

    # Chars in quoted fields are not considered
    s = codeunits("a,b,c\ne,$(Char(Q))::::::::::::::::::::::::$(Char(Q)),g\ni,j,k\n")
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, E, true) == UInt8(',')

    s = codeunits("a,b,c\ne,$(Char(Q)):::::::::$(Char(E))$(Char(Q))$(Char(E))$(Char(Q))$(Char(E))$(Char(Q)):::::::::::::::$(Char(Q)),g\ni,j,k\n")
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, E, true) == UInt8(',')

    s = codeunits("a,b,c\ne,$(Char(Q)):::::::::$(Char(Q))$(Char(Q))$(Char(Q))$(Char(Q))$(Char(Q))$(Char(Q)):::::::::::::::$(Char(Q)),g\ni,j,k\n")
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, Q, true) == UInt8(',')

    s = b"a:b:c\ne:f:g\ni:j:k\n"
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, E, true) == UInt8(':')

    s = b"a,b,c\r\ne,f,g\r\ni:j:k\r\n"
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, E, true) == UInt8(',')

    # No valid delim candidate -- use default delim
    s = b"a!b!c\ne!f!g\ni!j!k\n"
    @assert !(UInt8('!') in ChunkedCSV.CANDIDATE_DELIMS)
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, E, true) == UInt8(',')

    # Header only -- take the max count delim
    s = b"a,b,c,d,e:f:g:h:i:j:k:l:m"
    @assert !(UInt8('!') in ChunkedCSV.CANDIDATE_DELIMS)
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, E, true) == UInt8(':')

    # One single complete row that is not a header -- take the max count delim
    s = b"a,b,c,d,e:f:g:h:i:j:k:l:m\n"
    @assert !(UInt8('!') in ChunkedCSV.CANDIDATE_DELIMS)
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, E, false) == UInt8(':')

    # Incomplete row which is not a header -- use default delim
    s = b"a,b,c,d,e:f:g:h:i:j:k:l:m"
    @assert !(UInt8('!') in ChunkedCSV.CANDIDATE_DELIMS)
    @test ChunkedCSV._detect_delim(s, 1, length(s), Q, Q, E, false) == UInt8(',')

    testctx = ChunkedCSV.TestContext()
    ChunkedCSV.parse_file(IOBuffer("a,b,c\ne,f,g\n"), [String, String, String], testctx, delim=nothing)
    @test testctx.header == [:a, :b, :c]
    @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31, Parsers.PosLen31]
    @test testctx.strings[1] == [["e"], ["f"], ["g"]]

    testctx = ChunkedCSV.TestContext()
    ChunkedCSV.parse_file(IOBuffer("a:b:c\ne:f:g\n"), [String, String, String], testctx, delim=nothing)
    @test testctx.header == [:a, :b, :c]
    @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31, Parsers.PosLen31]
    @test testctx.strings[1] == [["e"], ["f"], ["g"]]

    testctx = ChunkedCSV.TestContext()
    parse_file(IOBuffer("a,b:b,c\ne,f,g\n"), [String, String, String], testctx, delim=nothing)
    @test testctx.header == [:a, Symbol("b:b"), :c]
    @test testctx.schema == [Parsers.PosLen31, Parsers.PosLen31, Parsers.PosLen31]
    @test testctx.strings[1] == [["e"], ["f"], ["g"]]
end
