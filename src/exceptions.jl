abstract type FatalLexingError <: Exception end
Base.showerror(io::IO, e::FatalLexingError) = print(io, e.msg)

# TODO: Add some data to help debug the problematic file, like the first row with an escape character
#       and/or the quote character.
struct NoValidRowsInBufferError <: FatalLexingError
    msg::String
    buffersize::UInt32

    function NoValidRowsInBufferError(buffersize::UInt32)
        return new(
            string(
                "CSV parse job failed on lexing newlines. There was no linebreak (outside a quoted string field) ",
                "in the entire buffer of $(buffersize) bytes. ",
                "This could happen if your buffer is too small, or if your `openquotechar`, `closequotechar` or `escapechar` ",
                "is not set properly, or if the file is not a proper CSV file according to RFC4180.",
            ),
            buffersize,
        )
    end
end

struct UnmatchedQuoteError <: FatalLexingError
    msg::String

    function UnmatchedQuoteError()
        new(
            string(
                "CSV parse job failed on lexing newlines. The file has ended with an unmatched quote. ",
                "This could happen if your `openquotechar`, `closequotechar`, ",
                "or `escapechar` is not set properly, or if the file is not a proper CSV file according ",
                "to RFC4180.",
            ),
        )
    end
end

struct HeaderParsingError <: Exception
    msg::String
end
Base.showerror(io::IO, e::HeaderParsingError) = print(io, e.msg)
