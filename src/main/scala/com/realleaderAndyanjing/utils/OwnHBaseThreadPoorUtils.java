package com.realleaderAndyanjing.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 自定义线程池HBase工具类
 *
 * @Time 2020-03-24
 */
public class OwnHBaseThreadPoorUtils implements scala.Serializable {
    private static Configuration configuration = HBaseConfiguration.create();
    TableName tableName;
    HTableDescriptor hTableDescriptor; //将表名传递给HTableDescriptor，这就是对表的描述之类的
    static Connection connection = null; //连接池，这是在Hbase1.X以后推荐的版本
    static ExecutorService executor; //线程池，开多了不好管理，开少了相当于没开

    static { //静态函数，程序运行时有点慢，但是之后运行较快，所有操作共用一个连接池和配置文件
        try {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hiwes");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 240000);
//            configuration.set("hbase.master", "hiwes:60000");

            //开启线程池，并以此开启连接池
            executor = Executors.newFixedThreadPool(8);
            connection = ConnectionFactory.createConnection(configuration, executor);
        } catch (Exception e) {
        }
    }

    //构造函数，设定要操作的表的名称
    public OwnHBaseThreadPoorUtils(String tableName, String[] cfs) {
        // 设置表名
        this.tableName = TableName.valueOf(tableName);
        this.hTableDescriptor = new HTableDescriptor(this.tableName);
        // 设置列簇
        for (int i = 0; i < cfs.length; i++)
            this.hTableDescriptor.addFamily(new HColumnDescriptor(cfs[i]));
    }

    /**
     * 创建表，在外部已经包含表名和cfs。
     *
     * @return 是否创建成功
     */
    public boolean createTable() {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(hTableDescriptor.getTableName())) {
                System.out.println("___table is Exists!");
            } else {
                admin.createTable(hTableDescriptor);
                System.out.println("___create table successfully");
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除表，在外部已经包含表名和cfs。
     *
     * @return 是否删除成功
     */
    public boolean deleteTable() {
        try (Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(hTableDescriptor.getTableName())) {
                System.out.println("___table is not Exists!");
            } else {
                admin.disableTable(hTableDescriptor.getTableName());
                admin.deleteTable(hTableDescriptor.getTableName());
                System.out.println("___delete table successfully");
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 判断表是否存在。
     *
     * @return 表是否存在
     */
    public boolean tableIsExist() {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(hTableDescriptor.getTableName())) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 单条写入数据。
     *
     * @return 是否写入成功
     * @throws IOException
     */
    public boolean putRow(String rowkey, String cf, String qualifier, String data) {
        try (Table table = connection.getTable(hTableDescriptor.getTableName())) {
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(data));
            table.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 批处理写入数据,以Puts的形式插入数据到Hbase中
     *
     * @return 是否写入成功
     * @throws IOException
     */
    public boolean putRows(List<Put> puts) {
        if (puts == null) {
            System.out.println("___puts is null");
        }
        try (Table table = connection.getTable(hTableDescriptor.getTableName())) {
            table.put(puts);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 指定读取一条数据
     *
     * @return 返回具体数据
     */
    public String getValue(String rowkey, String cf, String qualifier) {
        String result = null;
        try (Table table = connection.getTable(hTableDescriptor.getTableName())) {
            Get get = new Get(Bytes.toBytes(rowkey));
            Result resultInt = table.get(get);
            byte[] value = resultInt.getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
            result = Bytes.toString(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 指定rowkey读取一行数据
     *
     * @return 返回Reuslt
     */
    public Result getRow(String rowkey) {
        try (Table table = connection.getTable(hTableDescriptor.getTableName())) {
            Get get = new Get(Bytes.toBytes(rowkey));
            return table.get(get);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * 根据rowkey和限定条件读取一行数据
     *
     * @param rowKey
     * @param filterList
     * @return
     */
    public Result getRow(String rowKey, FilterList filterList) {
        try (Table table = connection.getTable(hTableDescriptor.getTableName())) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(filterList);
            return table.get(get);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * 全表扫描
     *
     * @return 全表扫描结果ResultScanner
     */
    public ResultScanner getScanner() {
        try (Table table = connection.getTable(hTableDescriptor.getTableName())) {
            Scan scan = new Scan();
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * 批量检索数据
     *
     * @return ResultScanner实例
     */
    public ResultScanner getScanner(String startRow, String endRow) {
        try (Table table = connection.getTable(hTableDescriptor.getTableName())) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(endRow));
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }


    /**
     * 设置过滤器，批量检索数据
     *
     * @return ResultScanner实例
     */
    public ResultScanner getScanner(String startRow, String endRow, FilterList filterList) {
        try (Table table = connection.getTable(hTableDescriptor.getTableName())) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(endRow));
            scan.setFilter(filterList);
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * 删除HBase一行记录
     *
     * @return 是否删除成功
     */
    public boolean deleteRow(String rowKey) {
        try (Table table = connection.getTable(hTableDescriptor.getTableName())) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            return true;
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return false;
    }


    /**
     * 删除HBase表指定列族
     *
     * @return 是否删除成功
     */
    public boolean deleteColumnFamily(String cfName) {
        try (Admin admin = connection.getAdmin()) {
            admin.deleteColumn(hTableDescriptor.getTableName(), Bytes.toBytes(cfName));
            return true;
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return false;
    }

    /**
     * 删除HBase表指定列
     *
     * @param rowKey    主键
     * @param cfName    列族名
     * @param qualifier 列名
     * @return 是否删除成功
     */
    public boolean deleteQualifier(String rowKey, String cfName, String qualifier) {
        try (Table table = connection.getTable(hTableDescriptor.getTableName())) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
            table.delete(delete);
            return true;
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return false;
    }


    /**
     * 关闭线程池与连接池
     */
    public void close() {
        try {
            if (null != connection) {
                connection.close();
            }
            if (null != executor) {
                executor.shutdown();
            }
        } catch (IOException e) {
        }
    }

    //将数据从Hadoop取出来，然后存入Hbase中
    public void saveDataFromHadoopToHbase(String family, String path) {
        //我们将许多的.log文件放在文件夹里面，因此我们需要遍历
        URI uri;
        FileSystem fileSystem;
        FSDataInputStream fsDataInputStream = null;
        BufferedReader bufferedReader = null;
        FileStatus[] fileStatus = null;
        try {
            uri = new URI(path);
            fileSystem = FileSystem.get(uri, configuration);
            //遍历hadoop文件
            fileStatus = fileSystem.listStatus(new Path(uri));
            for (FileStatus file : fileStatus) {
                //判断是否为日志文件
                if (file.getPath().getName().contains(".log")) {
                    //将日志文件的内容存入流中
                    fsDataInputStream = fileSystem.open(file.getPath());
                    bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
                    //调用函数将其存入Hbase中
                    this.insertBufferToHBase(family, bufferedReader);
                }
            }
        } catch (URISyntaxException e) {
        } catch (IOException e) {
        } finally {
            //关闭相关的数据流与各种缓存
            try {
                if (null == fileStatus) {
                }
                if (null == fsDataInputStream) {
                    fsDataInputStream.close();
                }
                if (null == bufferedReader) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
            }
        }
    }

    //将Buffer数据插入Hbase中
    private void insertBufferToHBase(String cf, BufferedReader bufferedReader) throws IOException {
        int bestBathPutSize = 3177; //todo 根据hbase缓冲区大小设定

        List<Mutation> mutations = new ArrayList<>(); //这里不理解可以将Mutation改为Put，Put是继承了Mutation的
        //相应的，也可以改成Table
        BufferedMutator table = connection.getBufferedMutator(hTableDescriptor.getTableName());

        try {
            while (true) {
                try {
                    //这是自定义函数，将数据转换成Put的形式
                    Put put = dataToPut(cf, bufferedReader.readLine());
                    //保证添加的数据不为空
                    if (put != null)
                        mutations.add(put);
                    //如果数据量达到一定程度，就先存入
                    if (mutations.size() == bestBathPutSize) {
                        table.mutate(mutations);
                        table.flush();
                        mutations.clear();
                    }
                } catch (IndexOutOfBoundsException e) {
                    break;
                }
            }
        } catch (IOException e) {
        } finally {
            //一定要记得关闭相关的流，文件等，不然线程池可能会受到影响
            if (null != table) {
                try {
                    table.close();
                } catch (IOException e) {
                }
            }
            if (null != bufferedReader) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                }
            }
        }
    }

    //数据清洗，将符合规范的数据变成Put
    private Put dataToPut(String family, String data) {
        if (data == null) {
            throw new IndexOutOfBoundsException();
        }
        String[] fragment = data.split(" ");
        Put put = null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        if (fragment.length != 0) {
            try {
                simpleDateFormat.parse(fragment[0]);
                put = new Put(Bytes.toBytes(fragment[0] + fragment[1] + 5));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(fragment[2]), Bytes.toBytes(fragment[3]));
                put.setDurability(Durability.SKIP_WAL);
            } catch (ParseException e) {
            }
        }
        return put;
    }


    public static void main(String[] args) {
        String tableName = "test";
        String[] cfs = {"info"};
        OwnHBaseThreadPoorUtils hbutils = new OwnHBaseThreadPoorUtils(tableName, cfs);
    }
}
