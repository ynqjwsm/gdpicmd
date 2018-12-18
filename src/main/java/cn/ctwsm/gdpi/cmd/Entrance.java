package cn.ctwsm.gdpi.cmd;

import com.alibaba.druid.pool.DruidDataSource;
import com.sun.org.apache.xpath.internal.operations.Bool;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class Entrance {

    private static CommandLine parseArg(String[] args) throws ParseException{
        //define args
        Options options = new Options();
        options.addOption("h", "help",false, "display help menu.");
        options.addOption("t", "thread",true, "define the number of concurrent threads.");
        options.addOption("s", "source",true, "define the source folder.");
        options.addOption("d", "delete",false, "delete source file after parse.");
        options.addOption("db", "database",true, "jdbc url.");
        options.addOption("u", "user",true, "database username.");
        options.addOption("p", "pass",true, "database password.");
        options.addOption("n", "table",true, "define the table name prefix.");
        options.addOption("o", "output",true, "define the output folder.");
        options.addOption("m", "prefix",true, "define the prefix of mdn,must split by commons.");

        //parse args
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        //display tips
        if (commandLine.hasOption("help") || commandLine.hasOption("h")) {
            HelpFormatter hf = new HelpFormatter();
            hf.setWidth(110);
            hf.printHelp("dpi-cmd", options, true);
            System.exit(-1);
        }

        //check args.
        String[][] required = new String[][]{
                {"s", "source folder must be provided!"},
                {"db", "database url must be provided!"},
                {"u", "database username must be provided!"},
                {"p", "database password must be provided!"}
        };
        for(String[] pair : required){
            if (!commandLine.hasOption(pair[0])) {
                throw new IllegalArgumentException(pair[1]);
            }
        }

        return commandLine;
    }


    public static void main(String[] args) {

//        log.warn("12312");

        CommandLine commandLine = null;
        try {
            commandLine = parseArg(args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //Get configurations
        //thread count
        Boolean needDelete = commandLine.hasOption("d") ? Boolean.TRUE : Boolean.FALSE;
        Integer threadCount = commandLine.hasOption("t") ? Integer.parseInt(commandLine.getOptionValue("t")) : 8;
        String jdbcUrl = commandLine.getOptionValue("db");
        String jdbcUsername = commandLine.getOptionValue("u");
        String jdbcPassword = commandLine.getOptionValue("p");
        String source = commandLine.getOptionValue("s");
        String tableNamePrefix = commandLine.getOptionValue("n");
        String dataTableName = null;
        String taskTableName = null;
        String jobTableName = null;
        //mdn采集选项
        Boolean collectMdn = commandLine.hasOption("m");
        String[] mdnCollection = collectMdn ? commandLine.getOptionValues("m") : new String[]{};
        //任务编号
        String taskId = UUID.randomUUID().toString();

//        System.out.println(jdbcUrl);
//        System.exit(0);
        //将子目录下的文件全部添加到列表中
        List<File> files = new LinkedList<File>();
        Stack<File> folders = new Stack<File>();
        folders.push(new File(source));
        while (!folders.empty()){
            File currentDir = folders.pop();
            for (File file : currentDir.listFiles()){
                if(file.isDirectory()){
                    folders.push(file);
                    continue;
                }
                if(file.getName().endsWith(".tar.gz") && hasOkFile(file)){
                    files.add(file);
                }
            }
        }

        //如果没有文件，退出
        if(files.isEmpty()) return;

        //初始化数据库连接池
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName("oracle.jdbc.OracleDriver");
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(jdbcUsername);
        dataSource.setPassword(jdbcPassword);
        dataSource.setInitialSize(threadCount + 1);
        dataSource.setMaxActive(threadCount * 2);


        try {
            dataSource.init();
            dataTableName = TableCreator.checkAndCreateImeiDataTable(tableNamePrefix, dataSource.getConnection());
            taskTableName = TableCreator.checkAndCreateTaskTable(tableNamePrefix, dataSource.getConnection());
            jobTableName = TableCreator.checkAndCreateJobTable(tableNamePrefix, dataSource.getConnection());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        long current = System.currentTimeMillis();
        long totalFileSize = 0L;

        AtomicInteger counter = new AtomicInteger(0);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (File file : files){
            totalFileSize += FileUtils.sizeOf(file);
            executorService.submit(new Worker(file, counter, dataSource, 500, dataTableName, jobTableName, taskId, needDelete));
        }

        int taskCount = files.size();
        while (taskCount != counter.get()){
            try {
                System.out.println("Progress: " + counter.get() + "/" + taskCount);
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long totalUsedMills = System.currentTimeMillis() - current;
        System.out.println("Use " + totalUsedMills + " mills.");

        //写入taskLog
        try {
            insertTaskItem(taskTableName, taskId, totalFileSize, files.size(), totalUsedMills, new Date(System.currentTimeMillis()), dataSource.getConnection());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        executorService.shutdown();
        dataSource.close();

    }

    public static Boolean insertTaskItem(String taskTableName,
                                        String taskId, Long fileSize, Integer fileCount, Long processMills, Date beginTime, Connection connection){
        Boolean success = Boolean.FALSE;
        java.sql.Date sqlDate = new java.sql.Date(beginTime.getTime());
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(String.format(SQL.INSERT_TASK_TABLE_SQL, taskTableName));
            preparedStatement.setString(1, taskId);
            preparedStatement.setLong(2, fileSize);
            preparedStatement.setInt(3, fileCount);
            preparedStatement.setLong(4, processMills);
            preparedStatement.setDate(5, sqlDate);
            preparedStatement.setInt(6, 1);
            preparedStatement.execute();
            preparedStatement.close();
            success = Boolean.TRUE;
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return success;

    }

    /**
     * 检测是否有OK文件，以确保文件完整
     * @param file
     * @return
     */
    public static boolean hasOkFile(File file){
        String fullPath = FilenameUtils.getFullPath(file.getAbsolutePath());
        String fullFileName = FilenameUtils.getName(file.getAbsolutePath());
        String okFile =  fullFileName.substring(0, fullFileName.length() - 6) + "ok";
        return new File(fullPath + okFile).exists();
    }

}
