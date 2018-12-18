package cn.ctwsm.gdpi.cmd;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class TableCreator {

    /**
     * 检查并创建任务表
     * @param tablePrefix
     * @param connection
     * @return
     */
    public static String checkAndCreateTaskTable(String tablePrefix, Connection connection){

        String tableSuffix = "_TASK";
        String tableFullName = tablePrefix.toUpperCase() + tableSuffix;
        return checkAndCreateTable(tableFullName, SQL.CREATE_TASK_TABLE_SQL, connection);

    }

    /**
     * 检查并创建工作表
     * @param tablePrefix
     * @param connection
     * @return
     */
    public static String checkAndCreateJobTable(String tablePrefix, Connection connection){

        String tableSuffix = "_JOB";
        String tableFullName = tablePrefix.toUpperCase() + tableSuffix;
        return checkAndCreateTable(tableFullName, SQL.CREATE_JOB_TABLE_SQL, connection);

    }

    /**
     * 检查并创建MSISDN数据表 格式为 PREFIX_MSISDN_20181201
     * @param tablePrefix
     * @param connection
     * @return
     */
    public static String checkAndCreateMsisdnDataTable(String tablePrefix, Connection connection){
        String tableSuffix = "_MSISDN_" + DateFormatUtils.format(System.currentTimeMillis(), "yyyyMMdd");
        String tableFullName = tablePrefix.toUpperCase() + tableSuffix;
        return checkAndCreateTable(tableFullName, SQL.CREATE_MSISDN_DATA_TABLE_SQL, connection);
    }

    /**
     * 检查并创建数据表，表名格式为  PREFIX_IMEI_20181201
     * @param tablePrefix
     * @param connection
     * @return
     */
    public static String checkAndCreateImeiDataTable(String tablePrefix, Connection connection){

        String tableSuffix = "_IMEI_" + DateFormatUtils.format(System.currentTimeMillis(), "yyyyMMdd");
        String tableFullName = tablePrefix.toUpperCase() + tableSuffix;
        return checkAndCreateTable(tableFullName, SQL.CREATE_IMEI_DATA_TABLE_SQL, connection);

    }

    private static String checkAndCreateTable(String fullTableName,String createTableSqlBase ,Connection connection){
        String sql = String.format(SQL.CHECK_TABLE_SQL, fullTableName);
        //检查表是否存在
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next() && resultSet.getInt(1) == 0){
                sql = String.format(createTableSqlBase, fullTableName);
                Statement createStatement = connection.createStatement();
                createStatement.execute(sql);
                createStatement.close();
            }
            resultSet.close();
        }catch (Exception e){
            fullTableName = null;
        }finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return fullTableName;
    }

}
