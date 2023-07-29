package org.jjyc;

import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.spark.actions.RewriteDataFilesSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataRewriter {
    private static Logger logger = LoggerFactory.getLogger(DataRewriter.class);
    private final Table table;
    private final String strategy;
    Map<String, String> options;
    String[] columns;
    SortOrder sortOrder;
    private final SparkActions actions;

    private DataRewriter(DataRewriterBuilder builder) {
        this.table = builder.table;
        if (Objects.isNull(builder.strategy)) {
            this.strategy = "binPack";
        } else {
            this.strategy = builder.strategy;
        }
        if (Objects.nonNull(builder.sortOrderColumns)) {
            this.columns = builder.sortOrderColumns.split(",");
            if (this.strategy.equalsIgnoreCase("sort")) {
                SortOrder.Builder sortOrderBuilder = SortOrder.builderFor(this.table.schema());
                for(int i = 0; i < this.columns.length; i ++) {
                    sortOrderBuilder.asc(this.columns[i]);
                }
                this.sortOrder = sortOrderBuilder.build();
            }
        }

        if (Objects.nonNull(builder.options)) {
            builder.options.entrySet().removeIf((entry) -> Objects.isNull(entry.getValue()));
            this.options = builder.options;
        }

        this.actions = SparkActions.get(builder.spark);
    }

    public void doRewriteDataFiles() {
        long rowCountBeforeRewrite = this.getRowNumber();
        logger.info("the number of row before rewriting: {}", rowCountBeforeRewrite);
        RewriteDataFilesSparkAction action = this.actions.rewriteDataFiles(this.table);
        logger.info("options for rewriting action: ");
        if (Objects.nonNull(this.options)) {
            this.options.forEach((key, value) -> {
                logger.info("{} : {}", key, value);
                action.option(key, value);
            });
        }

        switch (this.strategy) {
            case "binPack": {
                logger.info("using binPack");
                action.binPack();
                break;
            }
            case "sort": {
                logger.info("using sort");
                if (this.sortOrder == null) {
                    action.sort();
                } else {
                    action.sort(this.sortOrder);
                }
                break;
            }
            case "zOrder": {
                logger.info("using zOrder");
                action.zOrder(this.columns);
                break;
            }
            default: {
                throw new IllegalArgumentException("Strategy only support binPack, sort and zOrder.");
            }
        }
        logger.info("begin compacting for table: {}, strategy: {}", this.table, this.strategy);
        RewriteDataFiles.Result result = action.execute();
        int rewrittenDataFilesCount = result.rewrittenDataFilesCount();
        logger.info("the number of data files rewritten: {}", rewrittenDataFilesCount);
        long rowCountAfterRewrite = this.getRowNumber();
        logger.info("the number of row after rewriting: {}", rowCountAfterRewrite);
        if (rowCountBeforeRewrite != rowCountAfterRewrite) {
            logger.info("The row count before rewriting is not equal to the row count after rewriting, rollback to last snapshot");
            this.RollBackToLastSnapshot();
        } else {
            logger.info("compaction successful");
        }

    }

    public long getRowNumber() {
        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(this.table);
        CloseableIterable<Record> records = scanBuilder.build();
        long sum = 0L;

        CloseableIterator<Record> it = records.iterator();

        while (it.hasNext()){
            it.next();
            sum++;
        }

        return sum;
    }

    public void RollBackToLastSnapshot() {
        Snapshot snapshot = this.table.currentSnapshot();
        if (Objects.nonNull(snapshot)) {
            Long currentSnapshot = snapshot.snapshotId();
            Long lastSnapshotId = snapshot.parentId();
            if (lastSnapshotId != null) {
                logger.info("current snapshot id: {}, rollback to last snapshot id: {}", currentSnapshot, lastSnapshotId);
                ManageSnapshots manageSnapshots = this.table.manageSnapshots();
                manageSnapshots.rollbackTo(lastSnapshotId).commit();
                Snapshot snapshot1 = this.table.currentSnapshot();
                long resultSnapshotId = snapshot1.snapshotId();
                logger.info("rollback successful, current snapshot id: {}", resultSnapshotId);
            } else {
                logger.info("oldest snapshot now, cannot operate rollback");
            }
        } else {
            logger.info("{} is a new table, cannot operate rollback", this.table.name());
        }

    }

    public static class DataRewriterBuilder {
        SparkSession spark;
        Table table;
        String strategy;
        Map<String, String> options;
        String sortOrderColumns;

        public DataRewriterBuilder(SparkSession spark, Map<String, String> options) {
            this.spark = spark;
            this.options = options;
        }

        public DataRewriterBuilder setTable(Table table) {
            this.table = table;
            return this;
        }

        public DataRewriterBuilder setStrategy(String strategy) {
            this.strategy = strategy;
            return this;
        }

        public DataRewriterBuilder setSortOrderColumns(String sortOrderColumns) {
            this.sortOrderColumns = sortOrderColumns;
            return this;
        }

        public DataRewriter build() {
            return new DataRewriter(this);
        }
    }
}
