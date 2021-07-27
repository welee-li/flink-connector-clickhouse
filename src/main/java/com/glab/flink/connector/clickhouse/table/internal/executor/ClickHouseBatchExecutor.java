package com.glab.flink.connector.clickhouse.table.internal.executor;

import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseBatchExecutor implements ClickHouseExecutor {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);

    private ClickHousePreparedStatement stmt;

    private ClickHouseConnectionProvider connectionProvider;

    private RuntimeContext context;

    private TypeInformation<RowData> rowDataTypeInformation;

    private final String sql;

    private final ClickHouseRowConverter converter;

    private List<RowData> batch;

    private final Duration flushInterval;

    private final int batchSize;

    private final int maxRetries;

    private final boolean ignoreDelete;

    private TypeSerializer<RowData> typeSerializer;

    private boolean objectReuseEnabled = false;

    private ExecuteBatchService service;

    public ClickHouseBatchExecutor(String sql,
                                   ClickHouseRowConverter converter,
                                   Duration flushInterval,
                                   int batchSize,
                                   int maxRetries,
                                   boolean ignoreDelete,
                                   TypeInformation<RowData> rowDataTypeInformation) {
        this.sql = sql;
        this.converter = converter;
        this.flushInterval = flushInterval;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.ignoreDelete = ignoreDelete;
        this.rowDataTypeInformation = rowDataTypeInformation;
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        this.batch = new ArrayList<>();
        this.stmt = (ClickHousePreparedStatement) connection.prepareStatement(this.sql);
        this.service = new ExecuteBatchService();
        this.service.startAsync();
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException {
        this.batch = new ArrayList<>();
        this.connectionProvider = connectionProvider;
        this.stmt = (ClickHousePreparedStatement) connectionProvider.getConnection().prepareStatement(this.sql);

        this.service = new ExecuteBatchService();
        this.service.startAsync();
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
        this.context = context;
        this.typeSerializer = this.rowDataTypeInformation.createSerializer(context.getExecutionConfig());
        this.objectReuseEnabled = context.getExecutionConfig().isObjectReuseEnabled();
    }

    @Override
    public synchronized void addBatch(RowData rowData) throws IOException {
        if (rowData.getRowKind() == RowKind.INSERT || rowData.getRowKind() == RowKind.UPDATE_AFTER) {

            this.batch.add(this.objectReuseEnabled ? this.typeSerializer.copy(rowData) : rowData);

        } else if (rowData.getRowKind() == RowKind.DELETE) {
            //如果数据字段个数>=3，则支持修改数据删除标识
            //请确保表最后两个字段分别为sign(删除标识),version(版本号)
            if (this.ignoreDelete) {
                this.batch.add(this.objectReuseEnabled ? this.typeSerializer.copy(rowData) : rowData);
            } else {
                if (rowData.getArity() >= 3) {
                    GenericRowData rowData1 = (GenericRowData) rowData;
                    rowData1.setField(rowData1.getArity() - 2, (byte) (-1 & 0xFF));
                    rowData1.setRowKind(RowKind.INSERT);
                    this.batch.add(this.objectReuseEnabled ? this.typeSerializer.copy(rowData1) : rowData1);
                } else {
                    LOG.warn("-------不支持数据删除，原因：表字段数小于3格式不对---------");
                }
            }
        } else {
            LOG.error("不支持的RowData类型");
        }
    }

    @Override
    public synchronized void executeBatch() throws IOException {
        if (this.service.isRunning()) {
            notifyAll();
        } else {
            throw new IOException("executor unexpectedly terminated", this.service.failureCause());
        }
    }

    @Override
    public void closeStatement() throws SQLException {
        if (this.service != null) {
            this.service.stopAsync().awaitTerminated();
        } else {
            LOG.warn("executor closed before initialized");
        }

        if (this.stmt != null) {
            this.stmt.close();
            this.stmt = null;
        }
    }

    @Override
    public String getState() {
        return ClickHouseBatchExecutor.this.service.state().toString();
    }

    private class ExecuteBatchService extends AbstractExecutionThreadService {
        private ExecuteBatchService() {
        }

        @Override
        protected void run() throws Exception {
            while (isRunning()) {
                synchronized (ClickHouseBatchExecutor.this) {
                    ClickHouseBatchExecutor.this.wait(ClickHouseBatchExecutor.this.flushInterval.toMillis());
                    if (!ClickHouseBatchExecutor.this.batch.isEmpty()) {
                        for (RowData rowData : ClickHouseBatchExecutor.this.batch) {
                            ClickHouseBatchExecutor.this.converter.toClickHouse(rowData, ClickHouseBatchExecutor.this.stmt);
                            ClickHouseBatchExecutor.this.stmt.addBatch();
                        }
                        attemptExecuteBatch();
                    }
                }
            }
        }

        private void attemptExecuteBatch() throws Exception {
            for (int idx = 1; idx <= ClickHouseBatchExecutor.this.maxRetries; idx++) {
                try {
                    ClickHouseBatchExecutor.this.stmt.executeBatch();
                    ClickHouseBatchExecutor.this.batch.clear();
                    break;
                } catch (ClickHouseException e1) {
                    ClickHouseBatchExecutor.LOG.error("ClickHouse error", e1);
                    //当出现ClickHouse exception, code: 27 ...DB::Exception: Cannot parse input 即这条数据是错误时,略过此次插入
                    int errorCode = e1.getErrorCode();
                    if (errorCode == 27) {
                        ClickHouseBatchExecutor.this.stmt.clearBatch();
                        ClickHouseBatchExecutor.this.batch.clear();
                        break;
                    }
                } catch (SQLException e2) {
                    ClickHouseBatchExecutor.LOG.error("ClickHouse executeBatch error, retry times = {}", Integer.valueOf(idx), e2);
                    if (idx >= ClickHouseBatchExecutor.this.maxRetries) {
                        throw new IOException(e2);
                    }
                    try {
                        Thread.sleep((1000 * idx));
                    } catch (InterruptedException e3) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", e3);
                    }
                }
            }
        }
    }
}
