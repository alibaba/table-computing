# Table-Computing 

Welcome to the Table-Computing GitHub.

Table-Computing (Simplified as TC) is a distributed light weighted, high performance and low latency stream processing and data analysis framework.
From our using experience TC can achieve milliseconds latency and 10+ times faster than Flink for complicated use cases.
For the same streaming task we use TC achieved 10+ times computing resource saving.



## Example
Computes the last hour top 100 sales volume ranking list every half hour
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
        table = table.select(new ScalarFunction() {
            @Override
            public Comparable[] returnOneRow(Row row) {
                TableIndex tableIndex = mysqlDimensionTable.curTable();
                // Use tableIndex.getRow but not mysqlDimensionTable.curTable().getRow. Consider that in some case
                // you may need to call mysqlDimensionTable.curTable() twice but the second call may correspond
                // to the newly reloaded dimension table which is not consistent with the first mysqlDimensionTable.curTable()
                Row commodity = tableIndex.getRow(row.getInteger("commodity_id"));
                return new Comparable[]{
                        commodity.getString("name"),
                        commodity.getInteger("price"),
                        row.getInteger("count") * commodity.getInteger("price")
                };
            }
        }, true, "commodity_name", "commodity_price", "total_price");
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



## Notice：
1. For no continuous data case the AbstractStreamTable will return an empty table after sleep 100ms (default)
 to trigger computing, else the watermark data/window data/rehashed or rebalanced to other server/thread data will never be computed
2. Reading dimension table thread will block until the dimension table finished loading



## Copyright and License
Table-Computing is provided under the [Apache-2.0 license](LICENSE).