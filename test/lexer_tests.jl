using Test
using ChunkedCSV: ChunkedCSV, NewlineLexers
using SIMD: Vec, vload
import ScanByte

vec64(x::Char) = vec64(UInt8(x))
vec64(x::UInt8) = Vec(ntuple(_->VecElement(x), 64))
vec64(xs::AbstractVector{UInt8}) = vload(Vec{64,UInt8}, xs, 1)
vec64(s::String) = vec64(collect(codeunits(s)))

function setup_for_kernel(l, s; prev_escaped=UInt(0), prev_in_string=UInt(0))
    @assert ncodeunits(s) == 64
    l.prev_escaped = prev_escaped
    l.prev_in_string = prev_in_string
    l_copy1 = deepcopy(l)
    l_copy2 = deepcopy(l)
    bytes = collect(codeunits(s))
    newlines = NewlineLexers._find_newlines_kernel!(l, vec64(s))
    eols = Int32[]
    GC.@preserve bytes NewlineLexers.find_newlines!(l_copy1, bytes, eols)
    @test eols == findall(>(0), digits(newlines, base=2))
    empty!(eols)
    GC.@preserve bytes NewlineLexers._find_newlines_generic!(l_copy2, bytes, eols)
    @test eols == findall(>(0), digits(newlines, base=2))
    @test l_copy2.prev_escaped == l.prev_escaped
    @test l_copy2.prev_in_string == l.prev_in_string
    return newlines
end
function generic_lexer_setup(l, buf; prev_escaped=UInt(0), prev_in_string=UInt(0), first=firstindex(buf), last=lastindex(buf))
    eols = Int32[]
    l.prev_escaped = prev_escaped
    l.prev_in_string = prev_in_string
    l_copy1 = deepcopy(l)
    bytes = collect(codeunits(buf))
    GC.@preserve bytes NewlineLexers._find_newlines_generic!(l, bytes, eols, first, last)
    _eols = Int32[]
    GC.@preserve bytes NewlineLexers.find_newlines!(l_copy1, bytes, _eols, first, last)
    @test _eols == eols
    return eols
end

function find_newlines_setup(l, buf; prev_escaped=UInt(0), prev_in_string=UInt(0), first=firstindex(buf), last=lastindex(buf))
    eols = Int32[]
    l.prev_escaped = prev_escaped
    l.prev_in_string = prev_in_string
    bytes = collect(codeunits(buf))
    GC.@preserve bytes NewlineLexers.find_newlines!(l, bytes, eols, first, last)
    return eols
end

bit(i) = UInt(1) << (i - 1)
bits(is) = mapreduce(bit, |, is, init=UInt(0))
bits() = bits(Int[])

@testset "handle_file_end" begin
    # Lexer{Nothing,Nothing,Nothing} cannot end on a string
    l = NewlineLexers.Lexer(IOBuffer(), nothing)
    @assert eof(l.io)
    l.done = false
    eols = Int32[0]
    ChunkedCSV.handle_file_end!(l, eols, 1)
    @test l.done
    @test eols == Int32[0, 2]

    # Lexer{Q,Q,Q}
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))
    @assert eof(l.io)
    l.done = false
    eols = Int32[0]
    ChunkedCSV.handle_file_end!(l, eols, 1)
    @test l.done
    @test eols == Int32[0, 2]

    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))
    l.prev_in_string = typemax(UInt)
    l.prev_escaped = UInt(1)
    l.done = false
    eols = Int32[0]
    ChunkedCSV.handle_file_end!(l, eols, 1)
    @test l.done
    @test eols == Int32[0, 2]

    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))
    l.prev_in_string = typemax(UInt)
    l.prev_escaped = UInt(0)
    l.done = false
    @assert eof(l.io)
    @test_throws ChunkedCSV.UnmatchedQuoteError ChunkedCSV.handle_file_end!(l, Int32[], 1)

    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))
    l.prev_in_string = typemin(UInt)
    l.prev_escaped = UInt(1)
    l.done = false
    @assert eof(l.io)
    @test_throws ChunkedCSV.UnmatchedQuoteError ChunkedCSV.handle_file_end!(l, Int32[], 1)

    # Lexer{E,Q,Q}
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))
    @assert eof(l.io)
    l.done = false
    eols = Int32[0]
    ChunkedCSV.handle_file_end!(l, eols, 1)
    @test l.done
    @test eols == Int32[0, 2]

    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))
    l.prev_in_string = typemax(UInt)
    l.prev_escaped = UInt(1)
    l.done = false
    @assert eof(l.io)
    @test_throws ChunkedCSV.UnmatchedQuoteError ChunkedCSV.handle_file_end!(l, Int32[], 1)

    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))
    l.prev_in_string = typemax(UInt)
    l.prev_escaped = UInt(0)
    l.done = false
    @assert eof(l.io)
    @test_throws ChunkedCSV.UnmatchedQuoteError ChunkedCSV.handle_file_end!(l, Int32[], 1)
end

@testset "Lexer" begin

@testset "prefix_xor" begin
    @test NewlineLexers.prefix_xor(UInt(0)) == UInt(0)
    @test NewlineLexers.prefix_xor(UInt(1)) == typemax(UInt)
    @test NewlineLexers.prefix_xor(bits(64)) == bits(64)
    @test NewlineLexers.prefix_xor(bits([63,64])) == bits(63)
    @test NewlineLexers.prefix_xor(bits(63)) == bits([63,64])
    @test NewlineLexers.prefix_xor(UInt(2)) == typemax(UInt) ⊻ UInt(1)
    @test NewlineLexers.prefix_xor(bits([2, 63])) == typemax(UInt) ⊻ bits([1, 63, 64])
    @test NewlineLexers.prefix_xor(UInt(6)) == UInt(2)
    @test NewlineLexers.prefix_xor(UInt(12)) == UInt(4)
    @test NewlineLexers.prefix_xor(typemax(UInt)) == 0x5555_5555_5555_5555
    @test NewlineLexers.prefix_xor(
        0b0000000000001000000001000000000000000000001000000000000000000010) ==
        0b0000000000000111111111000000000000000000000111111111111111111110
    @test NewlineLexers.prefix_xor(
        0b0111111111111000011111111000000000000001111110000000011110000110) ==
        0b0010101010101000001010101000000000000000101010000000001010000010
    @test NewlineLexers.prefix_xor(
        0b0100000000001000010000001000000000000001000010000000010010000110) ==
        0b0011111111111000001111111000000000000000111110000000001110000010
    @test NewlineLexers.prefix_xor(
        0b0001111100001111111000110000011111111100110111111100001100010000) ==
        0b1111010100000101010111101111110101010100010010101011111011110000
end

@testset "_icmp_eq_u64" begin
    @test NewlineLexers._icmp_eq_u64(vec64(0x00), vec64(0x00)) == typemax(UInt64)
    @test NewlineLexers._icmp_eq_u64(vec64(0x01), vec64(0x00)) == typemin(UInt64)

    s =  "_123456789_123456789_123456789_123456789_123456789_123456789_123"
    i = 0b1000000000100000000010000000001000000000100000000010000000001000
    @assert length(s) == 64
    @test NewlineLexers._icmp_eq_u64(vec64(reverse(s)), vec64('_')) == i
end

@testset "_scanbyte_bytes" begin
    l = NewlineLexers.Lexer(IOBuffer(), nothing)
    @test NewlineLexers._scanbyte_bytes(l) == UInt8('\n')
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))
    @test NewlineLexers._scanbyte_bytes(l) == Val(ScanByte.ByteSet((UInt8('"'), UInt8('\n'))))
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))
    @test NewlineLexers._scanbyte_bytes(l) == Val(ScanByte.ByteSet((UInt8('\\'), UInt8('"'), UInt8('\n'))))
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('['), UInt8(']'))
    @test NewlineLexers._scanbyte_bytes(l) == Val(ScanByte.ByteSet((UInt8('\\'), UInt8('['), UInt8(']'), UInt8('\n'))))
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('['), UInt8(']'), UInt8('\r'))
    @test NewlineLexers._scanbyte_bytes(l) == Val(ScanByte.ByteSet((UInt8('\\'), UInt8('['), UInt8(']'), UInt8('\r'))))
end

@testset "_find_newlines_kernel!(l::Lexer{E,Q,Q}, ...)" begin
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))

    # No control chars
    s = "_123456789_123456789_123456789_123456789_123456789_123456789_123"
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # All newlines
    s = "\n" ^ 64
    newlines = setup_for_kernel(l, s)
    @test newlines == typemax(UInt)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # End with a newline
    s = "_123456789_123456789_123456789_123456789_123456789_123456789_12\n"
    newlines = setup_for_kernel(l, s)
    @test newlines == UInt(1) << 63
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # End and begin with a newline
    s = "\n123456789_123456789_123456789_123456789_123456789_123456789_12\n"
    newlines = setup_for_kernel(l, s)
    @test newlines == bits([1, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # End and begin with a newline with an escape in the middle
    s = "\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_12\n"
    newlines = setup_for_kernel(l, s)
    @test newlines == bits([1, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Starts in a string
    s = "\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_12\n"
    newlines = setup_for_kernel(l, s, prev_in_string = typemax(UInt))
    @test newlines == bit(26)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Starts with a quote
    s = "\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s)
    @test newlines == bit(27)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Starts inside a string with a quote
    s = "\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_in_string=typemax(UInt))
    @test newlines == bits([2, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Starts inside a string with an escaped quote
    s = "\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_in_string=typemax(UInt), prev_escaped=UInt(1))
    @test newlines == bit(27)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Starts with an escaped quote
    s = "\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_escaped=UInt(1))
    @test newlines == bits([2, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Starts with an escaped quote and then a regular quote
    s = "\"\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_\n"
    newlines = setup_for_kernel(l, s, prev_escaped=UInt(1))
    @test newlines == bit(28)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Last byte is an unquoted newline
    s = "\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_\"\n"
    newlines = setup_for_kernel(l, s)
    @test newlines == bits([27,64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # First byte is an unquoted newline
    s = "\n\"123456789_123456789\"1234\n6789\"123456789_123456789_123456789_\n\""
    newlines = setup_for_kernel(l, s)
    @test newlines == bits([1, 27])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Ends with an escape
    s = "\"123456789_123456789_123456789_123456789_123456789_123456789_12\\"
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == true
    @test l.prev_in_string == typemax(UInt)

    # Ends with two escapes
    s = "\"123456789_123456789_123456789_123456789_123456789_123456789_1\\\\"
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Ends with three escapes
    s = "\"123456789_123456789_123456789_123456789_123456789_123456789_\\\\\\"
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == true
    @test l.prev_in_string == typemax(UInt)

    # Ends with a quote
    s = "_123456789_123456789_123456789_123456789_123456789_123456789_12\""
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Ends with two quotes
    s = "_123456789_123456789_123456789_123456789_123456789_123456789_1\"\""
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Ends with three quotes
    s = "_123456789_123456789_123456789_123456789_123456789_123456789_\"\"\""
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)
end

@testset "_find_newlines_kernel!(l::Lexer{Q,Q,Q}, ...)" begin
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))

    # No control chars
    s = "_123456789_123456789_123456789_123456789_123456789_123456789_123"
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # All newlines
    s = "\n" ^ 64
    newlines = setup_for_kernel(l, s)
    @test newlines == typemax(UInt)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # End with a newline
    s = "_123456789_123456789_123456789_123456789_123456789_123456789_12\n"
    newlines = setup_for_kernel(l, s)
    @test newlines == UInt(1) << 63
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # End and begin with a newline
    s = "\n123456789_123456789_123456789_123456789_123456789_123456789_12\n"
    newlines = setup_for_kernel(l, s)
    @test newlines == bits([1, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # End and begin with a newline with an escape in the middle
    s = "\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_12\n"
    newlines = setup_for_kernel(l, s)
    @test newlines == bits([1, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Starts in a string
    s = "\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_12\n"
    newlines = setup_for_kernel(l, s, prev_in_string = typemax(UInt))
    @test newlines == bit(26)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Starts with a quote
    s = "\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s)
    @test newlines == bit(27)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Starts inside a string with a quote
    s = "\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_in_string=typemax(UInt))
    @test newlines == bits([2, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Starts inside a string with an escaped quote
    s = "\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_in_string=typemax(UInt), prev_escaped=UInt(1))
    @test newlines == bit(27)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Starts with an escaped quote
    s = "\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_escaped=UInt(1))
    @test newlines == bits([2, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Starts with an escaped quote and then a regular quote
    s = "\"\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_\n"
    newlines = setup_for_kernel(l, s, prev_escaped=UInt(1))
    @test newlines == bit(28)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Last byte is an unquoted newline
    s = "\"\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_\"\n"
    newlines = setup_for_kernel(l, s)
    @test newlines == bits([27, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # First byte is an unquoted newline
    s = "\n\"123456789_123456789\"1234\n6789\"123456789_123456789_123456789_\n\""
    newlines = setup_for_kernel(l, s)
    @test newlines == bits([1, 27])
    @test l.prev_escaped == true            # different from the the e != q case "First byte is an unquoted newline"
    @test l.prev_in_string == typemax(UInt) # different from the the e != q case "First byte is an unquoted newline"

    # Ends with an escape
    s = "_123456789_123456789_123456789_123456789_123456789_123456789_12\""
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == true
    @test l.prev_in_string == typemin(UInt)

    # Ends with two escapes
    s = "_123456789_123456789_123456789_123456789_123456789_123456789_1\"\""
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Ends with three escapes
    s = "_123456789_123456789_123456789_123456789_123456789_123456789_\"\"\""
    newlines = setup_for_kernel(l, s)
    @test newlines == typemin(UInt)
    @test l.prev_escaped == true            # different from the the e != q case "Ends with three quotes"
    @test l.prev_in_string == typemin(UInt) # different from the the e != q case "Ends with three quotes"

    # Previous chunk ended in a string with the last byte being an escape/quote
    s = "1\n123456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_in_string=typemax(UInt), prev_escaped=UInt(1))
    @test newlines == bits([2, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Newline after ambiguous escape 1/2
    s = "\n0123456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_in_string=typemax(UInt), prev_escaped=UInt(1))
    @test newlines == bits([1, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Newline after ambiguous escape 2/2
    s = "\n\"\"23456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_in_string=typemax(UInt), prev_escaped=UInt(1))
    @test newlines == bits([1, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Newline after ambiguous escape 3/5
    s = "\n\n\"\"3456789_123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_in_string=typemax(UInt), prev_escaped=UInt(1))
    @test newlines == bits([1, 2, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Newline after ambiguous escape 4/5
    s = "\n\"\"\"3456789\"123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_in_string=typemax(UInt), prev_escaped=UInt(1))
    @test newlines == bits([1, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Newline after ambiguous escape 5/5
    s = "\n\n\"\"\"456789\"123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_in_string=typemax(UInt), prev_escaped=UInt(1))
    @test newlines == bits([1, 2, 64])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    s = "\n\n\"\"\"456789\"123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_escaped=UInt(1))
    @test newlines == bits([27])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    s = "\n\"\"\"3456789\"123456789\"1234\n6789\"123456789_123456789_123456789_1\n"
    newlines = setup_for_kernel(l, s, prev_escaped=UInt(1))
    @test newlines == bits([27])
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)
end

@testset "_find_newlines_generic!(l::Lexer{E,OQ,CQ}, ...)" begin
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('['), UInt8(']'))

    # Empty input
    @test_throws AssertionError NewlineLexers._find_newlines_generic!(l, UInt8[], Int32[])

    # Single char: newline
    out = generic_lexer_setup(l, "\n")
    @test out == Int32[1]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Single char: escapechar
    out = generic_lexer_setup(l, "\\")
    @test isempty(out)
    @test l.prev_escaped == false # escapechar outside of string is ignored if E != Q
    @test l.prev_in_string == typemin(UInt)

    # Single char: open quote
    out = generic_lexer_setup(l, "[")
    @test isempty(out)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Single char: close quote
    out = generic_lexer_setup(l, "]")
    @test isempty(out)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Single char: other
    out = generic_lexer_setup(l, "1")
    @test isempty(out)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Multiple chars: newline
    out = generic_lexer_setup(l, "\n\n\n", first=2, last=3)
    @test out == Int32[2,3]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Boundaries are respected
    out = generic_lexer_setup(l, "\n\n\n", first=2, last=2)
    @test out == Int32[2]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    out = generic_lexer_setup(l, "[\n]", first=2, last=2)
    @test out == Int32[2]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Ends on an escapechar
    out = generic_lexer_setup(l, "123456789_1[\\")
    @test isempty(out)
    @test l.prev_escaped == true
    @test l.prev_in_string == typemax(UInt)

    @testset "Basic string test" begin
        l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('['), UInt8(']'))

        out = generic_lexer_setup(l, "[1234\n6789]")
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "[][][][][][][][][][][][][]")
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "[][][][][][][][][][][][][]", first=2, last=2)
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "[][][][][][][][][][][][][]", first=3, last=3)
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "[][][][][][][][][][][][][]", first=2, last=4)
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "[][][][][][][][][][][][][]", first=2, last=5)
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)
    end

    @testset "Starts in a string" begin
        l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('['), UInt8(']'))

        out = generic_lexer_setup(l, "]", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\\", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "123456", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "123456]", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "123456\\", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemax(UInt)
    end

    @testset "Starts on escape" begin
        l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('['), UInt8(']'))

        out = generic_lexer_setup(l, "]123456", prev_in_string=typemax(UInt), prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\\\\]123456", prev_in_string=typemax(UInt), prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)
    end
end

@testset "_find_newlines_generic!(l::Lexer{E,Q,Q}, ...)" begin
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))

    # Empty input
    @test_throws AssertionError NewlineLexers._find_newlines_generic!(l, UInt8[], Int32[])

    # Single char: newline
    out = generic_lexer_setup(l, "\n")
    @test out == Int32[1]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Single char: escapechar
    out = generic_lexer_setup(l, "\\")
    @test isempty(out)
    @test l.prev_escaped == false # escapechar outside of string is ignored if E != Q
    @test l.prev_in_string == typemin(UInt)

    # Single char: single quote
    out = generic_lexer_setup(l, "\"")
    @test isempty(out)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Single char: other
    out = generic_lexer_setup(l, "1")
    @test isempty(out)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Multiple chars: newline
    out = generic_lexer_setup(l, "\n\n\n", first=2, last=3)
    @test out == Int32[2,3]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Boundaries are respected
    out = generic_lexer_setup(l, "\n\n\n", first=2, last=2)
    @test out == Int32[2]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    out = generic_lexer_setup(l, "[\n]", first=2, last=2)
    @test out == Int32[2]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Ends on an escapechar
    out = generic_lexer_setup(l, "123456789_1\"\\")
    @test isempty(out)
    @test l.prev_escaped == true
    @test l.prev_in_string == typemax(UInt)

    @testset "Basic string test" begin
        l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))

        out = generic_lexer_setup(l, "\"1234\n6789\"")
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"")
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", first=2, last=2)
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", first=2, last=4)
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", first=2, last=5)
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)
    end

    @testset "Starts in a string" begin
        l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('['), UInt8(']'))

        out = generic_lexer_setup(l, "]", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\\", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "123456", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "123456]", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "123456\\", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemax(UInt)
    end

    @testset "Starts on escape" begin
        l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))

        out = generic_lexer_setup(l, "\"123456", prev_in_string=typemax(UInt), prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\\\\\"123456", prev_in_string=typemax(UInt), prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)
    end
end

@testset "_find_newlines_generic!(l::Lexer{Q,Q,Q}, ...)" begin
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))

    # Empty input
    @test_throws AssertionError NewlineLexers._find_newlines_generic!(l, UInt8[], Int32[])

    # Single newline
    out = generic_lexer_setup(l, "\n")
    @test out == Int32[1]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Single escapechar/quote
    out = generic_lexer_setup(l, "\"")
    @test isempty(out)
    @test l.prev_escaped == true
    @test l.prev_in_string == typemin(UInt)

    # Odd sequence of quotes
    out = generic_lexer_setup(l, "\"\"\"")
    @test isempty(out)
    @test l.prev_escaped == true
    @test l.prev_in_string == typemin(UInt)

    # Inside of string end on what might be an escape
    out = generic_lexer_setup(l, "\"a\"")
    @test isempty(out)
    @test l.prev_escaped == true
    @test l.prev_in_string == typemax(UInt)

    # Inside of string end on what might be an escape (odd sequence)
    out = generic_lexer_setup(l, "\"a\"\"\"")
    @test isempty(out)
    @test l.prev_escaped == true
    @test l.prev_in_string == typemax(UInt)

    # Inside of string end on what cannot be an escape
    out = generic_lexer_setup(l, "\"ab")
    @test isempty(out)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemax(UInt)

    # Ending on an even seqeunce of quotes 1/2
    out = generic_lexer_setup(l, "ab\"\"")
    @test isempty(out)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Ending on an even seqeunce of quotes 2/2
    out = generic_lexer_setup(l, "ab\"\"\"\"")
    @test isempty(out)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Single other char
    out = generic_lexer_setup(l, "1")
    @test isempty(out)
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Multiple chars: newline
    out = generic_lexer_setup(l, "\n\n\n", first=2, last=3)
    @test out == Int32[2,3]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Boundaries are respected 1/2
    out = generic_lexer_setup(l, "\n\n\n", first=2, last=2)
    @test out == Int32[2]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    # Boundaries are respected 2/2
    out = generic_lexer_setup(l, "\"\n\"", first=2, last=2)
    @test out == Int32[2]
    @test l.prev_escaped == false
    @test l.prev_in_string == typemin(UInt)

    @testset "Basic string test" begin
        l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))

        out = generic_lexer_setup(l, "\"1234\n6789\"")
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"")
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", first=2, last=2)
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", first=2, last=4)
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", first=2, last=5)
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)
    end

    @testset "Starts in a string" begin
        l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))

        out = generic_lexer_setup(l, "\"", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\"\"", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\"a", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "123456", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "1234\n6\"", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "1234\n6\"a\"", prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemin(UInt)
    end

    @testset "Starts on escape" begin
        l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))

        out = generic_lexer_setup(l, "\"", prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"\"", prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"a", prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"\"a", prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\"\"\"a", prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"\"123\n6\"", prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "123456", prev_escaped=UInt(1))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)
    end

    @testset "Starts on escape and in string" begin
        l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))

        out = generic_lexer_setup(l, "\"", prev_escaped=UInt(1), prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\"\"", prev_escaped=UInt(1), prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\"a", prev_escaped=UInt(1), prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\"\"a", prev_escaped=UInt(1), prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"\"a\"", prev_escaped=UInt(1), prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == true
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\"\"\"a", prev_escaped=UInt(1), prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemax(UInt)

        out = generic_lexer_setup(l, "\"\"345\n7\"", prev_escaped=UInt(1), prev_in_string=typemax(UInt))
        @test out == Int32[6]
        @test l.prev_escaped == true
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "123456", prev_escaped=UInt(1), prev_in_string=typemax(UInt))
        @test isempty(out)
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\n23456", prev_escaped=UInt(1), prev_in_string=typemax(UInt))
        @test out == Int32[1]
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)

        out = generic_lexer_setup(l, "\n\"\"456\n", prev_escaped=UInt(1), prev_in_string=typemax(UInt))
        @test out == Int32[1, 7]
        @test l.prev_escaped == false
        @test l.prev_in_string == typemin(UInt)
    end
end

_f(x) = x === nothing ? "nothing" : repr(Char(x))
@testset "find_newlines!" begin
    @testset "Lexer{$(_f(e)), $(_f(oq)), $(_f(cq)), $(_f(nl))}" for (e, oq, cq, nl) in (
        ('"', '"', '"', '\n'),
        ('\\', '"', '"', '\n'),
        ('\\', '[', ']', '\n'),
        (nothing, nothing, nothing, '\n'),
    )
        has_no_real_quotes = isnothing(e)
        if has_no_real_quotes
            l = NewlineLexers.Lexer(IOBuffer(), nothing, nl)
            e = "E"
            oq = "O"
            cq = "C"
        else
            l = NewlineLexers.Lexer(IOBuffer(), e, oq, cq, nl)
        end

        @testset "Errors" begin
            @test_throws ArgumentError find_newlines_setup(l, "")
            @test_throws ArgumentError find_newlines_setup(l, "1", first=0)
            @test_throws ArgumentError find_newlines_setup(l, "1", last=0)
            @test_throws ArgumentError find_newlines_setup(l, "1", first=2)
            @test_throws ArgumentError find_newlines_setup(l, "1", last=2)
            @test_throws ArgumentError find_newlines_setup(l, "11", first=2, last=1)
        end

        eols = find_newlines_setup(l, "$nl")
        @test eols == [1]

        # "P...P\"\"\"\n\"\"\"\"\"\n\"\"\"\"\"\"\"\n\"\nP...P"
        # "P...P\"\\\"\n\\\"\\\"\"\n\"\\\"\\\"\\\"\n\"\nP...P"
        # "P...P[\\]\n\\[\\]]\n[\\]\\[\\]\n]\nP...P"
        # "P...POEC\nEOECC\nOECEOEC\nC\nP...P"
        for pad in 0:129
            eols = find_newlines_setup(l, ("P"^pad) * "$oq$e$cq$nl$e$oq$e$cq$cq$nl$oq$e$cq$e$oq$e$cq$nl$cq$nl" * ("P"^pad))
            @test eols == (pad .+ (has_no_real_quotes ? [4, 10, 18, 20] : [10, 20]))
        end

        for len in 1:513
            eols = find_newlines_setup(l, ("$nl"^len))
            @test eols == 1:len
        end
    end
end

end # @testset Lexer
