# applicative-go
An applicative functor for data processing pipelines, using go1.18 generics. 

The applicative package is primarily an exploration of golang generics
(parametric polymorphism) introduced in the Go 1.18 release.

An applicative functor is a mathematical concept and functional
programming pattern. In engineering, it often helps to have
formal, mathematical descriptions of problem domain. Making use
of these formalism to have an organized/structured approach is
very different from considering all programming as mathematics.

We are interested here in the particular domain of stream processing,
or data processing pipelines. This domain is one born from practical
application and does not have "standard" formal descriptions.
The purpose of this library is not to capture applicative functors as
a programming abstraction, but rather to explore its usefulness
in structuring this problem domain.

To get started, check out example/wordcount/wordcount.go

It contains a simple data processing pipeline.

You can run it in "demo" mode:

```
>go1.18beta build example/wordcount/wordcount.go
>wordcount
```

It will perform a "static analysis" on the pipeline.
Then it will run the pipeline on some sample data.

Or you can run it in "server" mode:

```
>wordcount -mode=server
```

This will run a HTTP server at `localhost:8080`.
Call it in a separate tab:
```
curl localhost:8080/in -H "process: wordcount" --data "hello world"
```

