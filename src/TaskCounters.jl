module TaskCounters

export TaskCounter, set!, dec!

mutable struct TaskCounter
    n::Int
    cond::Threads.Condition
    exception::Union{Nothing,Exception}
end
TaskCounter() = TaskCounter(0, Threads.Condition(ReentrantLock()), nothing)

Base.lock(t::TaskCounter) = lock(t.cond)
Base.unlock(t::TaskCounter) = unlock(t.cond)

function set!(t::TaskCounter, n::Int)
    n > 0 || throw(ArgumentError("n must be positive"))
    Base.@lock t begin
        @assert t.n == 0 "Trying to `set!` a non-zero `TaskCounter`, $(t.n)"
        t.n = n
    end
    return nothing
end

function dec!(t::TaskCounter, n::Int=1)
    n > 0 || throw(ArgumentError("n must be positive"))
    Base.@lock t begin
        @assert t.n >= n "Trying decrement a `TaskCounter` past zero, `$(t.n)` -> `$(t.n - n)`"
        t.n -= n
        notify(t.cond)
    end
    return nothing
end

function Base.wait(t::TaskCounter)
    Base.@lock t begin
        while true
            !isopen(t) && throw(t.exception)
            t.n == 0 && break
            wait(t.cond)
        end
    end
    return nothing
end

function Base.close(t::TaskCounter, e::Exception)
    Base.@lock t begin
        t.exception = e
        notify(t.cond, e, all=true, error=true)
    end
end

function Base.isopen(t::TaskCounter)
    return Base.@lock t t.exception === nothing
end

end # module