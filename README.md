# applicative-go
An applicative functor for data processing pipelines, using go1.18 generics. 

You need go1.18beta to try this. See https://go.dev/doc/tutorial/generics how to get it.

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

