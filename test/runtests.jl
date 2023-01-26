using Test

Threads.nthreads() == 1 && @warn "Running tests with a single thread -- won't be able to spot concurrency issues"

@testset "ChunkedCSV.jl" begin
    include("decimals.jl")
    include("guess_datetime.jl")
    include("simple_file_parsing.jl")
    include("exception_handling.jl")
end
