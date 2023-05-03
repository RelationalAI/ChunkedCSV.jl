using Test

Threads.nthreads() == 1 && @warn "Running tests with a single thread -- won't be able to spot concurrency issues"

@testset "ChunkedCSV.jl" begin
    include("decimals.jl")
    include("guess_datetime.jl")
    include("buffered_vector_tests.jl")
    include("consume_context_tests.jl")
    include("lexer_tests.jl")
    include("simple_file_parsing.jl")
    include("exception_handling.jl")
    include("task_result_buffer_tests.jl")
    include("misc_tests.jl")
end

#=
using Coverage
using ChunkedCSV
pkg_path = pkgdir(ChunkedCSV)
coverage = process_folder(joinpath(pkg_path, "src"));
open(joinpath(pkg_path, "lcov.info"), "w") do io
    LCOV.write(io, coverage)
end;
run(`find $pkg_path -name "*.cov" -type f -delete`);
=#