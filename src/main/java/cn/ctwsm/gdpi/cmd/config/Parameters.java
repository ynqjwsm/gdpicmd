package cn.ctwsm.gdpi.cmd.config;

import lombok.Data;

/**
 * 参数类，该类用于封装从CMD传入的参数
 */
@Data
public class Parameters {

    //线程设置
    private Integer threadCount = 8;

    //采集完后是否删除文件
    private Boolean needDelete = Boolean.FALSE;

    //源文件位置
    private String sourceFolder;

    //mdn采集开关
    private Boolean collectMdn = Boolean.FALSE;

    //IMEI采集开关
    private Boolean collectImei = Boolean.TRUE;


    //数据库Url
    private String jdbcUrl;
    //数据库用户名
    private String jdbcUsername;
    //数据库密码
    private String jdbcPassword;
    //数据库表名前缀
    private String tableNamePrefix;


}
