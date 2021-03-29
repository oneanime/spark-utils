import com.hp.bean.UserActionInfo;
import com.hp.utils.SparkJdbcUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

public class MysqlTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(MysqlTest.class.getName()).setMaster("local[*]");
        SparkSession spark = new SparkSession.Builder().config(conf).enableHiveSupport().getOrCreate();

/*        StructType structType = new StructType();
        structType.add("user_id", DataTypes.IntegerType,true)
                .add("item_id", DataTypes.LongType,true)
                .add("category",DataTypes.IntegerType,true)
                .add("behavior",DataTypes.StringType,true)
                .add("ts",DataTypes.LongType,true);*/

        Dataset<UserActionInfo> actionInfoDS = SparkJdbcUtil.MysqlSource(spark, "select user_id as userId,item_id as itemId,behavior,category,cast(ts as decimal) as timestamp from user_action_info")
                .as(Encoders.bean(UserActionInfo.class));
        actionInfoDS.show();
        Dataset<Row> acDF = actionInfoDS.toDF("user_id", "item_id", "behavior", "category", "ts");
        acDF.show();
        SparkJdbcUtil.MysqlSink(acDF, "sink_test", SaveMode.Append);


    }
}
