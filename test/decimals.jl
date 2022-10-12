using Parsers
using Test
using ChunkedCSV: _typeparser


const DEFAULT_OPTIONS = Parsers.Options(delim=',', quoted=true)
const DEFAULT_OPTIONS_GROUPMARK = Parsers.Options(delim=',', quoted=true, groupmark=' ')
_typeparser2(::Type{T}, f, buf, r=RoundNearest, options=DEFAULT_OPTIONS) where {T} = _typeparser(T, f, buf, 1, length(buf), UInt8(first(buf)), Int16(0), options, r)

@testset "decimals" begin
    function exhaustive_tests()
        @testset "$T" for T in (Int8, Int16)
            @testset "$f" for f in (0, 1, 2)
                for i in typemin(T):typemax(T)
                    expected = i * 10^f
                    bs = UInt8.(collect(string(i)))
                    (expected > typemax(T) || expected < typemin(T)) && continue
                    @testset "i" begin @test _typeparser2(T, f, bs)[1] == expected end
                    @testset "i.00" begin @test _typeparser2(T, f, vcat(bs, UInt8['.', '0', '0']))[1] == expected end
                    @testset "ie0"  begin @test _typeparser2(T, f, vcat(bs, UInt8['e', '0']))[1] == expected end
                    if f == 1 && length(bs) > f + 1 + (i < 0)
                        @testset "i.f" begin
                            bss = copy(bs)
                            n = length(bss)
                            splice!(bss, n:n-1, UInt8('.'))
                            @test _typeparser2(T, f, bss)[1] == i
                        end
                    end
                end
            end
        end
    end
    exhaustive_tests()

    @test _typeparser2(Int32, 4, "0")[1] == 0
    @test _typeparser2(Int32, 4, "0.0")[1] == 0
    @test _typeparser2(Int32, 4, "0.0e0")[1] == 0
    @test _typeparser2(Int32, 4, "0.0e+0")[1] == 0
    @test _typeparser2(Int32, 4, "0.0e-0")[1] == 0
    @test _typeparser2(Int32, 4, "-0")[1] == 0
    @test _typeparser2(Int32, 4, "-0.0")[1] == 0
    @test _typeparser2(Int32, 4, "-0.0e0")[1] == 0
    @test _typeparser2(Int32, 4, "-0.0e+0")[1] == 0
    @test _typeparser2(Int32, 4, "-0.0e-0")[1] == 0
    @test _typeparser2(Int32, 4, "1")[1] == 10000
    @test _typeparser2(Int32, 4, "1.0")[1] == 10000
    @test _typeparser2(Int32, 4, "1.0e+0")[1] == 10000
    @test _typeparser2(Int32, 4, "1.0e-0")[1] == 10000
    @test _typeparser2(Int32, 4, "0.1e+1")[1] == 10000
    @test _typeparser2(Int32, 4, "0.01e+2")[1] == 10000
    @test _typeparser2(Int32, 4, "0.00000000001e+11")[1] == 10000
    @test _typeparser2(Int32, 4, "10.0e-1")[1] == 10000
    @test _typeparser2(Int32, 4, "100.0e-2")[1] == 10000
    @test _typeparser2(Int32, 4, "100000000000.0e-11")[1] == 10000
    @test _typeparser2(Int32, 4, "-1")[1] == -10000
    @test _typeparser2(Int32, 4, "-1.0")[1] == -10000
    @test _typeparser2(Int32, 4, "-1.0e+0")[1] == -10000
    @test _typeparser2(Int32, 4, "-1.0e-0")[1] == -10000
    @test _typeparser2(Int32, 4, "-0.1e+1")[1] == -10000
    @test _typeparser2(Int32, 4, "-0.01e+2")[1] == -10000
    @test _typeparser2(Int32, 4, "-0.00000000001e+11")[1] == -10000
    @test _typeparser2(Int32, 4, "-10.0e-1")[1] == -10000
    @test _typeparser2(Int32, 4, "-100.0e-2")[1] == -10000
    @test _typeparser2(Int32, 4, "-100000000000.0e-11")[1] == -10000

    @testset "$T" for T in (Int32, Int64)
        @testset "decimal position" begin
            @test _typeparser2(T, 2, "123")[1]   == 123_00
            @test _typeparser2(T, 2, "0.123")[1] == 0_12
            @test _typeparser2(T, 2, ".123")[1]  == 0_12
            @test _typeparser2(T, 2, "1.23")[1]  == 1_23
            @test _typeparser2(T, 2, "12.3")[1]  == 12_30
            @test _typeparser2(T, 2, "123.")[1]  == 123_00
            @test _typeparser2(T, 2, "123.0")[1] == 123_00

            @test _typeparser2(T, 2, "-123")[1]   == -123_00
            @test _typeparser2(T, 2, "-0.123")[1] == -0_12
            @test _typeparser2(T, 2, "-.123")[1]  == -0_12
            @test _typeparser2(T, 2, "-1.23")[1]  == -1_23
            @test _typeparser2(T, 2, "-12.3")[1]  == -12_30
            @test _typeparser2(T, 2, "-123.")[1]  == -123_00
            @test _typeparser2(T, 2, "-123.0")[1] == -123_00
        end

        @testset "scientific notation" begin
            @test _typeparser2(T, 4, "12e0")[1]   == 00012_0000
            @test _typeparser2(T, 4, "12e3")[1]   == 12000_0000
            @test _typeparser2(T, 4, "12e-3")[1]  == 00000_0120
            @test _typeparser2(T, 4, "1.2e0")[1]  == 00001_2000
            @test _typeparser2(T, 4, "1.2e3")[1]  == 01200_0000
            @test _typeparser2(T, 4, "1.2e-3")[1] == 00000_0012
            @test _typeparser2(T, 4, "1.2e-4")[1] == 00000_0001

            @test _typeparser2(T, 4, "-12e0")[1]   == -00012_0000
            @test _typeparser2(T, 4, "-12e3")[1]   == -12000_0000
            @test _typeparser2(T, 4, "-12e-3")[1]  == -00000_0120
            @test _typeparser2(T, 4, "-1.2e0")[1]  == -00001_2000
            @test _typeparser2(T, 4, "-1.2e3")[1]  == -01200_0000
            @test _typeparser2(T, 4, "-1.2e-3")[1] == -00000_0012

            @test _typeparser2(T, 2, "999e-1")[1] == 99_90
            @test _typeparser2(T, 2, "999e-2")[1] == 09_99
            @test _typeparser2(T, 2, "999e-3")[1] == 01_00
            @test _typeparser2(T, 2, "999e-4")[1] == 00_10
            @test _typeparser2(T, 2, "999e-5")[1] == 00_01
            @test _typeparser2(T, 2, "999e-6")[1] == 00_00

            @test _typeparser2(T, 2, "-999e-1")[1] == -99_90
            @test _typeparser2(T, 2, "-999e-2")[1] == -09_99
            @test _typeparser2(T, 2, "-999e-3")[1] == -01_00
            @test _typeparser2(T, 2, "-999e-4")[1] == -00_10
            @test _typeparser2(T, 2, "-999e-5")[1] == -00_01
            @test _typeparser2(T, 2, "-999e-6")[1] == -00_00

            @test _typeparser2(T, 4, "9"^96 * "e-100")[1] == 0_001
        end

        @testset "round to nearest" begin
            @test _typeparser2(T, 2, "0.444")[1] == 0_44
            @test _typeparser2(T, 2, "0.445")[1] == 0_44
            @test _typeparser2(T, 2, "0.446")[1] == 0_45
            @test _typeparser2(T, 2, "0.454")[1] == 0_45
            @test _typeparser2(T, 2, "0.455")[1] == 0_46
            @test _typeparser2(T, 2, "0.456")[1] == 0_46

            @test _typeparser2(T, 2, "-0.444")[1] == -0_44
            @test _typeparser2(T, 2, "-0.445")[1] == -0_44
            @test _typeparser2(T, 2, "-0.446")[1] == -0_45
            @test _typeparser2(T, 2, "-0.454")[1] == -0_45
            @test _typeparser2(T, 2, "-0.455")[1] == -0_46
            @test _typeparser2(T, 2, "-0.456")[1] == -0_46

            @test _typeparser2(T, 2, "0.009")[1]  ==  0_01
            @test _typeparser2(T, 2, "-0.009")[1] == -0_01

            @test _typeparser2(T, 4, "1.5e-4")[1] == 0_0002
        end

        @testset "groupmark" begin
            @test _typeparser2(T, 4, "-1 000.0", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == -10000000
            @test _typeparser2(T, 2, "1 0 0.444", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == 100_44
            @test _typeparser2(T, 2, "1 0 0.445", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == 100_44
            @test _typeparser2(T, 2, "1 0 0.446", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == 100_45
            @test _typeparser2(T, 2, "1 0 0.454", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == 100_45
            @test _typeparser2(T, 2, "1 0 0.455", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == 100_46
            @test _typeparser2(T, 2, "1 0 0.456", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == 100_46

            @test _typeparser2(T, 2, "-9 9 00.444", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == -9900_44
            @test _typeparser2(T, 2, "-9 9 00.445", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == -9900_44
            @test _typeparser2(T, 2, "-9 9 00.446", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == -9900_45
            @test _typeparser2(T, 2, "-9 9 00.454", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == -9900_45
            @test _typeparser2(T, 2, "-9 9 00.455", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == -9900_46
            @test _typeparser2(T, 2, "-9 9 00.456", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == -9900_46

            @test _typeparser2(T, 2, "9 9 9 9.009", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1]  ==  9999_01
            @test _typeparser2(T, 2, "-9 9 9 9.009", RoundNearest, DEFAULT_OPTIONS_GROUPMARK)[1] == -9999_01
        end

        @testset "round to zero" begin
            @test _typeparser2(T, 2, "0.444", RoundToZero)[1] == 0_44
            @test _typeparser2(T, 2, "0.445", RoundToZero)[1] == 0_44
            @test _typeparser2(T, 2, "0.446", RoundToZero)[1] == 0_44
            @test _typeparser2(T, 2, "0.454", RoundToZero)[1] == 0_45
            @test _typeparser2(T, 2, "0.455", RoundToZero)[1] == 0_45
            @test _typeparser2(T, 2, "0.456", RoundToZero)[1] == 0_45

            @test _typeparser2(T, 2, "-0.444", RoundToZero)[1] == -0_44
            @test _typeparser2(T, 2, "-0.445", RoundToZero)[1] == -0_44
            @test _typeparser2(T, 2, "-0.446", RoundToZero)[1] == -0_44
            @test _typeparser2(T, 2, "-0.454", RoundToZero)[1] == -0_45
            @test _typeparser2(T, 2, "-0.455", RoundToZero)[1] == -0_45
            @test _typeparser2(T, 2, "-0.456", RoundToZero)[1] == -0_45

            @test _typeparser2(T, 2, "0.009", RoundToZero)[1]  == 0_00
            @test _typeparser2(T, 2, "-0.009", RoundToZero)[1] == 0_00

            @test _typeparser2(T, 4, "1.5e-4", RoundToZero)[1] == 0_0001
        end
    end
end