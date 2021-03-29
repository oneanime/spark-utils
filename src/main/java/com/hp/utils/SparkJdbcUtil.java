package com.hp.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SparkJdbcUtil {
    private static Properties prop = null;
    private static final String CONFIG_FILE_PATH = "jdbc.properties";

    static {
        InputStream in = null;
        try {
            prop = new Properties();
            in = SparkJdbcUtil.class.getClassLoader().getResourceAsStream(CONFIG_FILE_PATH);
            prop.load(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert in != null;
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 查出来的字段如果和bean有出入，可以在查询sql语句中用as指定别名
     * @param spark
     * @param sql
     * @return
     */
    public static Dataset<Row> MysqlSource(SparkSession spark, String sql) {
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("driver", prop.getProperty("mysql.driver"))
                .option("url", prop.getProperty("mysql.url"))
//                .option("dbtable", tableName)
                .option("user", prop.getProperty("mysql.username"))
                .option("password", prop.getProperty("mysql.password"))
                .option("query", sql)
                .load();
        return jdbcDF;
    }

    /**
     * 如果字段名和数据库中对不上，可以使用.toDF(字段名)，重新确定字段名
     * @param ds
     * @param tableName
     * @param mode
     */
    public static void MysqlSink(Dataset ds, String tableName, SaveMode mode) {
        ds.write().mode(mode)
                .format("jdbc")
                .option("driver", prop.getProperty("mysql.driver"))
                .option("url", prop.getProperty("mysql.url"))
                .option("dbtable", tableName)
                .option("user", prop.getProperty("mysql.username"))
                .option("password", prop.getProperty("mysql.password"))
                .save();

    }
}
