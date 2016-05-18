Shortest Path (Bellman Ford algorithm) on Hadoop MapReduce
==========================================================

Assignment for CUHK csci course

The single source shortest path algorithm on MapReduce is described on:
Jimmy Lin and Chris Dyer, "Data-Intensive Text Processing with MapReduce"
And is known as "parallel Dikstra" in the CSCI4180 course.

With a little understanding on graph algorithms, I know it is actually
"Bellman Ford" algorithm (though [parallel dikstra] sounds better)

So, most optimization to Bellman Ford could be applied, which I applied 1
here: only vertex updated last time are checked.

## Compile
```
javac -cp lib:$HADOOP_CLASSPATH ParallelDijkstra.java -d build
jar -cvf build.jar -C build .
```

## Syntax:
```
hadoop jar build.jar parallelSP.ParallelSP parallelSP/ParallelSP [source vertex]
[max iteration] [file in] [directory out]
```

## file in Format
```
[from vertex] [to vertex] [edge weight]
[from vertex] [to vertex] [edge weight]
...
```
