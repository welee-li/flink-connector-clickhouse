//package com.glab.flink.connector.clickhouse.table.internal.executor;
//
//import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
//import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
//import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
//import com.google.common.util.concurrent.AbstractExecutionThreadService;
//import org.apache.commons.io.IOExceptionWithCause;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeutils.TypeSerializer;
//import org.apache.flink.table.data.RowData;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import ru.yandex.clickhouse.ClickHouseConnection;
//import ru.yandex.clickhouse.ClickHousePreparedStatement;
//
//import java.io.IOException;
//import java.sql.SQLException;
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//public class ClickHouseUpsertExecutor implements ClickHouseExecutor {
//    private static final long serialVersionUID = 1l;
//
//    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseUpsertExecutor.class);
//
//    private ClickHousePreparedStatement insertStmt;
//
//    private ClickHousePreparedStatement updateStmt;
//
//    private ClickHousePreparedStatement deleteStmt;
//
//    private final String insertSql;
//
//    private final String updateSql;
//
//    private final String deleteSql;
//
//    private final ClickHouseRowConverter converter;
//
//    private final List<RowData> insertBatch;
//
//    private final List<RowData> updateBatch;
//
//    private final List<RowData> deleteBatch;
//
//    private ExecuteBatchService service;
//
//    private final Duration flushInterval;
//
//    private final int maxRetries;
//
//    private RuntimeContext context;
//
//    private TypeInformation<RowData> rowDataTypeInformation;
//
//    private TypeSerializer<RowData> typeSerializer;
//
//    private boolean objectReuseEnabled = false;
//
//    public ClickHouseUpsertExecutor(String insertSql, String updateSql, String deleteSql, ClickHouseRowConverter converter, ClickHouseOptions options) {
//        this.insertSql = insertSql;
//        this.updateSql = updateSql;
//        this.deleteSql = deleteSql;
//        this.converter = converter;
//        this.flushInterval = options.getFlushInterval();
//        this.maxRetries = options.getMaxRetries();
//        this.insertBatch = new ArrayList<>();
//        this.updateBatch = new ArrayList<>();
//        this.deleteBatch = new ArrayList<>();
//
//        System.out.println("--------------------------");
//    }
//
//
//    @Override
//    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
//        this.insertStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.insertSql);
//        this.updateStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.updateSql);
//        this.deleteStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.deleteSql);
//        this.service = new ExecuteBatchService();
//        this.service.startAsync();
//    }
//
//    @Override
//    public void prepareStatement(ClickHouseConnectionProvider clickHouseConnectionProvider) throws SQLException {
//        this.insertStmt = (ClickHousePreparedStatement) clickHouseConnectionProvider.getConnection().prepareStatement(this.insertSql);
//        this.updateStmt = (ClickHousePreparedStatement) clickHouseConnectionProvider.getConnection().prepareStatement(this.updateSql);
//        this.deleteStmt = (ClickHousePreparedStatement) clickHouseConnectionProvider.getConnection().prepareStatement(this.deleteSql);
//        this.service = new ExecuteBatchService();
//        this.service.startAsync();
//    }
//
//    @Override
//    public void setRuntimeContext(RuntimeContext context) {
//    }
//
//    @Override
//    public void addBatch(RowData rowData) throws IOException {
//        switch (rowData.getRowKind()) {
//            case INSERT:
//                this.insertBatch.add(rowData);
//                return;
//            case DELETE:
//                this.deleteBatch.add(rowData);
//                return;
//            case UPDATE_AFTER:
//                this.updateBatch.add(rowData);
//                return;
//            case UPDATE_BEFORE:
//                return;
//        }
//        throw new UnsupportedOperationException(
//                String.format("Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.", new Object[]{rowData.getRowKind()}));
//    }
//
//    @Override
//    public synchronized void executeBatch() throws IOException {
//        if (this.service.isRunning()) {
//            notifyAll();
//        } else {
//            throw new IOException("executor unexpectedly terminated", this.service.failureCause());
//        }
//    }
//
//    @Override
//    public void closeStatement() throws SQLException {
//        if (this.service != null) {
//            this.service.stopAsync().awaitTerminated();
//        } else {
//            LOG.warn("executor closed before initialized");
//        }
//
//        List<ClickHousePreparedStatement> clickHousePreparedStatements = Arrays.asList(new ClickHousePreparedStatement[]{this.insertStmt, this.updateStmt, this.deleteStmt});
//        for (ClickHousePreparedStatement stmt : clickHousePreparedStatements) {
//            if (stmt != null) {
//                stmt.close();
//            }
//        }
//    }
//
//    @Override
//    public String getState() {
//        return ClickHouseUpsertExecutor.this.service.state().toString();
//    }
//
//    private class ExecuteBatchService extends AbstractExecutionThreadService {
//
//        private ExecuteBatchService() {
//        }
//
//        @Override
//        protected void run() throws Exception {
//            while (isRunning()) {
//                synchronized (ClickHouseUpsertExecutor.this) {
//                    ClickHouseUpsertExecutor.this.wait(ClickHouseUpsertExecutor.this.flushInterval.toMillis());
//                    if (ClickHouseUpsertExecutor.this.insertBatch.size() > 0) {
//                        processInsertBatch(ClickHouseUpsertExecutor.this.insertStmt, ClickHouseUpsertExecutor.this.insertBatch);
//                    }
//                    if (ClickHouseUpsertExecutor.this.updateBatch.size() > 0) {
//                        processUpdateBatch(ClickHouseUpsertExecutor.this.updateStmt, ClickHouseUpsertExecutor.this.updateBatch);
//                    }
//
//                    //processBatch(ClickHouseUpsertExecutor.this.deleteStmt, ClickHouseUpsertExecutor.this.deleteBatch);
//                }
//            }
//        }
//
//        private void processInsertBatch(ClickHousePreparedStatement stmt, List<RowData> batch) throws Exception {
//            if (!batch.isEmpty()) {
//                for (RowData rowData : batch) {
//                    ClickHouseUpsertExecutor.this.converter.toClickHouse(rowData, stmt);
//                    stmt.addBatch();
//                }
//                attemptExecuteInsertBatch(stmt, batch);
//            }
//        }
//
//        private void attemptExecuteInsertBatch(ClickHousePreparedStatement stmt, List<RowData> batch) throws Exception {
//            for (int i = 0; i < ClickHouseUpsertExecutor.this.maxRetries; i++) {
//                try {
//                    stmt.executeBatch();
//                    batch.clear();
//                    break;
//                } catch (SQLException e) {
//                    ClickHouseUpsertExecutor.LOG.error("ClickHouse executeBatch error, retry times = {}", Integer.valueOf(i), e);
//                    if (i >= ClickHouseUpsertExecutor.this.maxRetries) {
//
//                        ClickHouseUpsertExecutor.LOG.error("----写入数据不成功-----");
//                        StringBuilder s = new StringBuilder();
//                        ClickHouseUpsertExecutor.this.insertBatch.forEach(x -> s.append(x.toString()));
//                        ClickHouseUpsertExecutor.LOG.error(ClickHouseUpsertExecutor.this.insertBatch.size() + "++++++" + s.toString());
//                        ClickHouseUpsertExecutor.this.insertBatch.clear();
//                        throw new IOException(e);
//                    }
//                    try {
//                        Thread.sleep(1000 * i);
//                    } catch (InterruptedException ex) {
//                        Thread.currentThread().interrupt();
//                        throw new IOException("unable to flush; interrupted while doing another attempt", e);
//                    }
//                }
//            }
//        }
//
//
//        private void processUpdateBatch(ClickHousePreparedStatement stmt, List<RowData> batch) throws Exception {
//            if (!batch.isEmpty()) {
//                for (RowData rowData : batch) {
//                    ClickHouseUpsertExecutor.this.converter.toClickHouse(rowData, stmt);
//                    stmt.addBatch();
//                }
//                //attemptExecuteUpdateBatch(stmt, batch);
//            }
//        }
//
//    }
//}
