package xl.flink.java;


import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * @author 夏龙
 * @date 2020-09-18
 */
public class MysqljdbcTest {
    public static DataSet<Row> jdbcRead(ExecutionEnvironment env){
        DataSet<Row> inputMysql = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                //配置数据库连接信息
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/test?characterEncoding=utf-8&serverTimezone=UTC")
                .setUsername("root")
                .setPassword("145112")
                .setQuery("select * from importantip")
                //设置查询的列的类型，根据实际情况定
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO))
                .finish());

        return inputMysql;
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Row> inputMysql = jdbcRead(env);
        inputMysql.print();
    }
}


