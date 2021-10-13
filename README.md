# Table-Computing 

Welcome to the Table-Computing GitHub.

Table-Computing (Simplified as TC) is a distributed light weighted, high performance and low latency stream processing and data analysis framework.
Relational operation, simple to use, write less.
From our using experience TC can achieve milliseconds latency and 10+ times faster than Flink for complicated use cases.
For the same streaming task we use TC achieved 10+ times computing resource saving.

## Why we develop this framework 
Relational operation is an effective tool to process and analyze data, SQL is a widely used implementation of relational operation. 
But SQL is not Turing-compete, we need UDF/Stored-procedure/UDAF/UDTF etc. to solve complicated business scenario. 
If we need complicated WHERE criteria, JOIN criteria, a new Scalar Function, Transform Function, Aggregation Function, Window Function etc. we cannot use SQL easily do this.
SQL is also not very efficient for complicated case, whether SQL can high-powered execute depend on the SQL plan optimizer has optimized the use case which we 
are using in the complicated business scenario. But more complicated scenario more difficult to guarantee every SQL use case had been optimized by the optimizer.
Besides SQL that we can also use Flink DataStream/DataSet but we need very long code to implement a complex data processing task and we also need 
to design the Execution-graph this is a complex art we need compound the operator or disjoint them then observe whether the adjusted graph is more efficient 
and the task delay is acceptable, if not where is the bottleneck of this Execution-graph and how to resolve. Think that complex task usually include dozens 
of operators which have lots of combinations, trying those maybe-efficient combinations is a heavy work.

## Example
Computes the last hour top 100 sales volume ranking list every half hour
```
<dependency>
    <groupId>com.alibaba</groupId>
    <artifactId>table-computing</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
MysqlDimensionTable mysqlDimensionTable = new MysqlDimensionTable("jdbc:mysql://localhost:3306/e-commerce",
        "commodity",
        "userName",
        "password",
        Duration.ofHours(1),
        new ColumnTypeBuilder()
        .column("id", Type.INT)
        .column("name", Type.VARCHAR)
        .column("price", Type.INT)
        .build(),
        "id"
        );

Map<String, Type> columnTypeMap = new ColumnTypeBuilder()
        .column("__time__", Type.BIGINT)
        .column("id", Type.BIGINT)
        .column("commodity_id", Type.INT)
        .column("count", Type.INT)
        .build();

KafkaStreamTable kafkaStreamTable = new KafkaStreamTable(bootstrapServers,
        "consumerGroupId",
        topic,
        0,
        columnTypeMap);
kafkaStreamTable.start();

StreamProcessing sp = new StreamProcessing();
String[] hashBy = new String[]{"commodity_id"};
Rehash rehashForSlideWindow = sp.rehash("uniqueNameForSlideWindow", hashBy);
String[] returnedColumns = new String[]{"commodity_id",
        "sales_volume",
        "saleroom",
        "window_start"};
SlideWindow slideWindow = new SlideWindow(Duration.ofHours(1),
        Duration.ofMinutes(30),
        hashBy,
        "__time__",
        new AggTimeWindowFunction() {
            @Override
            public Comparable[] agg(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd) {
                return new Comparable[]{
                        partitionByColumns.get(0),
                        AggregationUtil.sumInt(rows, "count"),
                        AggregationUtil.sumInt(rows, "total_price"),
                        windowStart
                };
            }
        }, returnedColumns);
slideWindow.setWatermark(Duration.ofSeconds(2));

hashBy = new String[]{"window_start"};
Rehash rehashForSessionWindow = sp.rehash("uniqueNameForSessionWindow", hashBy);
SessionWindow sessionWindow = new SessionWindow(Duration.ofSeconds(1),
        hashBy,
        "window_start",
        new TimeWindowFunction() {
            @Override
            public List<Comparable[]> transform(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd) {
                int[] top100 = WindowUtil.topN(rows, "sales_volume", 100);
                List<Comparable[]> ret = new ArrayList<>(100);
                for (int i = 0; i < top100.length; i++) {
                    ret.add(rows.get(top100[i]).getAll());
                }
                return ret;
            }
        }, returnedColumns);
sessionWindow.setWatermark(Duration.ofSeconds(3));

sp.compute(new Compute() {
    @Override
    public void compute(int myThreadIndex) throws InterruptedException {
        Table table = kafkaStreamTable.consume();
        TableIndex tableIndex = mysqlDimensionTable.curTable();
        table = table.leftJoin(tableIndex.getTable(), new JoinCriteria() {
            @Override
            public List<Integer> theOtherRows(Row thisRow) {
                // Use tableIndex.getRows but not mysqlDimensionTable.curTable().getRows. Consider the second
                // mysqlDimensionTable.curTable() may correspond to the newly reloaded dimension table which
                // is not consistent with the first mysqlDimensionTable.curTable() and tableIndex.getTable()
                return tableIndex.getRows(thisRow.getInteger("commodity_id"));
            }},
            new As().
                as("id", "order_id").
                build(),
            new As().
                as("name", "commodity_name").
                as("price", "commodity_price").
                build());
        List<Table> tables = rehashForSlideWindow.rehash(table, myThreadIndex);
        table = slideWindow.slide(tables);
        tables = rehashForSessionWindow.rehash(table, myThreadIndex);
        table = sessionWindow.session(tables);
        if (table.size() > 0) {
            table.print();
            //you can elegantly finish the streaming task when terminate condition is satisfied
            Thread.currentThread().interrupt();
        }
    }
});
```
Distributed deploy your table-computing task:

java -Dself=localhost:8888 -Dall=localhost:8888,localhost:9999 -jar my_task.jar -cp table-computing-1.0.0.jar;my_other_dependencies...

java -Dself=localhost:9999 -Dall=localhost:8888,localhost:9999 -jar my_task.jar -cp table-computing-1.0.0.jar;my_other_dependencies...



## Optimize：
1. Use only 1 thread concurrency to test the 1 thread throughput, then use upstream data volume divide 1 thread throughput to get the
 StreamProcessing concurrent thread number. The thread number should not be too large since thread race will lead to unnecessary
 resource consumption which maybe give rise to OOM (no enough CPU time to release the unused memory)
2. -Xmx parameter should be appropriate. Since the table data are all store on the off-heap memory to improve performance too large
 -Xmx will lead to belatedly memory release which may give rise to OOM, while too small -Xmx will lead to too frequently GC to reduce 
 the throughput.
3. Not only the old GC stop the world young GC also stop the world transiently, more threads means more garbage generation
 means more GC means more often STW means thread CPU usage cannot be raised, you may find use more StreamProcessing 
 thread cannot increase the throughput now you should start a new JVM (can be on the same machine use localhost:anotherPort). 
 Actually use N thread can not get N times throughput you may need start a new JVM, you can also use `top -H -p pid` to 
 see whether the compute-X named threads CPU usage approximate 100% to make your decision. 



## Notice：
1. For no continuous data case the AbstractStreamTable will return an empty table after sleep 100ms (default)
 to trigger computing, else the watermark data/window data/rehashed or rebalanced to other server/thread data will never be computed
2. Reading dimension table thread will block until the dimension table finished loading



## Copyright and License
Table-Computing is provided under the [Apache-2.0 license](LICENSE).
