# ChunkedCSV.jl

The main goal of this package is to offer a parser for CSV files that can be used for streaming purposes. 
This means that it can work with data that is too large to fit into memory or in situations where we only need to process parts of the input data at a time. 
The parser reads the data in chunks, and it can parse each chunk in parallel while simultaneously prefetching the next one to increase throughput. 
Additionally, the parsed data is passed to a user-defined function that can consume it in any way required.

Moreover, the package can automatically handle [`Parsers.jl`](https://github.com/JuliaData/Parsers.jl)-compatible types without dynamic dispatch overhead and with a low compilation overhead. 
This is done without specializing on input schema, and the memory usage is upper bound by the user, who can control the chunk size. 
All internally allocated buffers are reused, so if the number of values per MiB of input is roughly constant, allocations should be minimal.

## Overview

The package builds on [`ChunkedBase.jl`](https://github.com/JuliaData/ChunkedBase.jl), which provides a framework for parser packages, enabling them to customize three main aspects of the parsing process: how the data is parsed into a result buffer, what is the layout of the result buffer, and how the result buffer is consumed. `ChunkedBase.jl` itself handles IO, splitting chunks by newlines and distributing them among multiple workers in parallel, where they are parsed by this package and consumed.

For the parsing part, this package uses the `Parsers.jl` package, and special care is taken to avoid dynamic dispatch overhead when parsing user-provided schemas without specializing on them. 
See the `src/populate_result_buffer.jl` file for more details about how this is done.

We currently only expose one result buffer type, `TaskResultBuffer`, where parsed data are stored by column and missing values are encoded in a densely packed bit matrix. 
We also store a `RowStatus` bitset for each row, so we know if a row had any parsing errors or missing values. 
See the `src/result_buffer.jl` file for more details.

How the result buffer is consumed is up to the user, who can provide custom callback function that will be called in parallel for each chunk of data that is parsed. More on this in the "How to use" section below.

## How to use

The main entry point is the `parse_file` function, which accepts a path or an `IO` as the input, a `schema` which describes the types of the columns in the CSV, and a user-defined "consume context" type.

Once the user provides this context, a subtype of `AbstractConsumeContext`, it will be used internally to dispatch on a custom `consume!(::AbstractConsumeContext,::ParsedPayload)` method. 
This method will be called in parallel for each chunk of data that is parsed, and it will receive a `ParsedPayload` object that contains the parsed result buffer in the `results` field and other relevant metadata for the corresponding chunk.

When no schema is provided, all columns are assumed to be `String`s and when no consume context is given, the `DebugContext` will be used. 
This context will print a short summary for each chunk and will also print the first couple of rows with errors in each chunk.

```julia
using ChunkedCSV

parse_file(
    IOBuffer("""
        a,b
        1,2
        3,4
        5,6
        7,8
        9,10
        """
    )
)
# [ Info: Start row: 1, nrows: 5, Task (runnable) @0x00007fd36f5fc010 ❚
```

The user can provide a schema for the columns, and the parser will try to parse the data into the provided types. 
There are many options to tweak the behavior of the parser, e.g. `buffersize` tells the parser to operate on 8 byte chunks of input at a time. 
It is important that the largest row in the file you want to ingest is smaller than `buffersize` otherwise you'll get an error.

```julia
# the input is very small, we force the parser to use parallelism even if it is not needed
parse_file(
    IOBuffer("""
        a,b
        1,2
        3,4
        5,6
        7,8
        9,10
        """
    ), 
    [Int, Int], 
    buffersize=8, 
    force=:parallel,
)
# [ Info: Start row: 1, nrows: 1, Task (runnable) @0x00007f6806c41f50 ❚
# [ Info: Start row: 4, nrows: 1, Task (runnable) @0x00007f6806c41f50 ❚
# [ Info: Start row: 2, nrows: 2, Task (runnable) @0x00007f6806f58650 ❚
# [ Info: Start row: 5, nrows: 1, Task (runnable) @0x00007f6806e8e400 ❚

# Rows 1 and 3 will fail to parse as `Int`s as they have a decimal point
parse_file(
    IOBuffer("""
        a,b
        1,2.0
        3,4
        5.0,6
        7,8
        9,10
        """
    ),
    [Int, Int]
)
# ┌ Info: Start row: 1, nrows: 5, Task (runnable) @0x00007fd36f5fc010 ❚
# │ Row count by status: ('✓', 3) | ('?', 2) | ('<', 0) | ('>', 0) | ('!', 2) | ('T', 0) | ('#', 0)
# │ Example rows with errors:
# │       (1): "1,2.0"
# └       (3): "5.0,6"
```
Note that the "Row count by status" uses the `RowStatus` information we store in `TaskResultBuffer`s to show that there were 3 (`✓`) rows that were parsed successfully, 2 rows have missing values for at least one field (`?`) and 2 rows had at least one parsing error (`!`). 
See the docstring for `TaskResultBuffer` for more information.

`parse_file` also accepts a number of keyword arguments that can be used to tweak the behavior of the parser by customizing how `Parsers.jl` is used internally.

```julia
parse_file(
    IOBuffer("""
        # a comment
        i;f;s
        1_000;2,99;"A string"
        2_000;4,99;"A string with an \\\"escaped\\\" quote"
        3_000_000;6,99;"Usually, you'd use \\\" for the escapechar"
        40;8,99;"But not in this example"
        5_000_000;10,99;"No sir"
        """
    ),
    [Int, Float64, String],
    delim=';',
    decimal=',',
    groupmark='_',
    escapechar='\\',
    comment='#',
)
# [ Info: Start row: 1, nrows: 5, Task (runnable) @0x000000010931c010 ❚
```
See the docstring for more details.

### Example: Custom `AbstractConsumeContext` for interactive use

It could be handy to have the option to provide an anonymous function to be called on each chunk. 
We can do that by creating a custom `AbstractConsumeContext` subtype that stores a function and dispatching on it in the `consume!` method.

```julia
using ChunkedCSV

struct ClosureContext{F} <: AbstractConsumeContext
    f::F
end
ChunkedCSV.consume!(ctx::ClosureContext, payload::ParsedPayload) = ctx.f(payload)
```
For increased ergonomics, we can also define a convenience method that calls `parse_file` with the custom context using the `do` syntax.

```julia
ChunkedCSV.parse_file(f::Function, input, schema; kwargs...) = parse_file(input, schema, ClosureContext(f); kwargs...)

io = IOBuffer("""
    a,b
    1,2
    3,4
    5,6
    7,8
    9,10
    """
)

parse_file(io, [Int, Int], buffersize=8) do payload
    println("Hi there! We're at start row: $(payload.row_num), nrows: $(payload.len) in this chunk")
end
# Hi there! We're at start row: 1, nrows: 1 in this chunk
# Hi there! We're at start row: 2, nrows: 2 in this chunk
# Hi there! We're at start row: 4, nrows: 1 in this chunk
# Hi there! We're at start row: 5, nrows: 1 in this chunk
```

## Notable differences to other CSV parsers

When compared to established packages like [`CSV.jl`](https://github.com/JuliaData/CSV.jl), our package has a few notable differences:
* We don't do any type inference for column types, so the user has to provide a schema. Otherwise, all columns will be parsed as `String`s.
* We don't give the user back any table-like object. Instead all, results are only temporarily available in the `consume!` method of the `AbstractConsumeContext` provided. One could use this package to build a table-like object inside of the context object, but this is not provided out out of the box.
* We pre-lex the chunks of data so that parallel parsing can be safe. `CSV.jl` does the often faster and riskier thing, where parser tasks jump directly in the file at various offsets, without knowing the record boundaries, and try to recover.
* At the time of writing, `CSV.jl` doesn't support multithreaded parsing of *chunks*, while we do.
* The package plays nicely with `PrefetchedDownloadStream.jl` from [`CloudStore.jl`](https://github.com/JuliaServices/CloudStore.jl), which is an IO stream that prefetches chunks of data from Amazon S3/Azure Blob Storage. This is useful for parsing large files from the cloud without having to download the whole file first, even if it is compressed.

## Advanced usage

### Learning the schema before creating an `AbstractConsumeContext`

Sometimes, one needs to know the schema that will used to parse the file before creating a consume context. For this reason we split the `parse_file` function into two parts.
First, we provide a `setup_parser` function which has similar interface as `parse_file` above, but doesn't require any consume context and doesn't do any parsing. 
It will validate user input, ingest enough data chunks to reach the first valid row in the input, and then examine the first row to ensure we have a valid header and schema. 
It will then return multiple objects:
```julia
(should_close, parsing_ctx, chunking_ctx, lexer) = setup_parser(input, schema; kwargs...)
```
- `should_close` is a `Bool` which is `true` if we opened an `IO` object we need to close later
- `parsing_ctx` is a `ChunkedCSV.ParsingContext` which contains the header, schema, and settings needed for `Parsers.jl`
- `chunking_ctx` is a `ChunkedBase.ChunkingContext` which holds the ingested data, newline positions and other things required by `ChunkedBase.jl` internally
- `lexer` is a `NewlineLexers.Lexer` which is used to find newlines in the ingested chunks and which is also needed by `ChunkedBase.jl`

Since the parsing context contains both the schema and the header, you can use this information to create a custom consume context.
Once you have your `consume_ctx`, you can call `parse_file` with the state returned by `setup_parser`.
```julia
consume_ctx = MyCustomContext(parsing_ctx.schema, parsing_ctx.header)
parse_file(parsing_ctx, consume_ctx, chunking_ctx, lexer)
```

