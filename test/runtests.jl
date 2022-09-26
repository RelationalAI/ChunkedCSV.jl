using Test

@testset "ChunkedCSV.jl" begin
    include("decimals.jl")
    include("simple_file_parsing.jl")
    include("exception_handling.jl")
end