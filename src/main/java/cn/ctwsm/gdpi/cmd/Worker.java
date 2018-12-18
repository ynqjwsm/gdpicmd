package cn.ctwsm.gdpi.cmd;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker implements Runnable {

    private File sourceFile;
    private AtomicInteger asyncTaskCounter;
    private DataSource dataSource;
    private Boolean deleteFlag = Boolean.FALSE;

    private String tableName;
    private Integer batchSize;

    //for log
    private String jobLogTableName;
    private String taskId;
    private Long processMills;
    private Long insertMills;
    private String fileName;
    private Long fileSize;
    private Integer sourceRecordCount;
    private Integer resultCount;
    private Date beginTime;
    private Boolean success = Boolean.FALSE;

    private HashSet<PhoneIdentity> list = new HashSet<PhoneIdentity>();

    public Worker(File sourceFile, AtomicInteger asyncTaskCounter, DataSource dataSource,
                  Integer batchSize, String tableName, String jobLogTableName, String taskId, Boolean needDelete){
        this.sourceFile = sourceFile;
        this.asyncTaskCounter = asyncTaskCounter;
        this.dataSource = dataSource;
        this.batchSize = batchSize;
        this.tableName = tableName;
        this.jobLogTableName = jobLogTableName;
        this.taskId = taskId;
        this.deleteFlag = needDelete;
    }

    public void run() {
        this.beginTime = new Date(System.currentTimeMillis());
        String jobId = UUID.randomUUID().toString();
        try {
            //for log
            this.fileName = sourceFile.getName();
            this.fileSize = FileUtils.sizeOf(sourceFile);
            //------
            byte[] sourceData = FileUtils.readFileToByteArray(sourceFile);
            this.parseData(sourceData);
            sourceData = null;
            this.writeToDatabase();
            this.success = Boolean.TRUE;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (deleteFlag){
                this.deleteOkFile(sourceFile);
                FileUtils.deleteQuietly(sourceFile);
            }
            asyncTaskCounter.addAndGet(1);
            try {
                this.insertJobItem(jobLogTableName, jobId, taskId, fileName, fileSize, sourceRecordCount, resultCount, processMills, insertMills, beginTime, success, dataSource.getConnection());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }


    private void writeToDatabase(){
        this.resultCount = list.size();
        long currMills = System.currentTimeMillis();
        try (Connection connection = dataSource.getConnection()){
            String sql = String.format(SQL.INSERT_IMEI_DATA_SQL, tableName);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            connection.setAutoCommit(Boolean.FALSE);
            int recordCount = 0;
            for(PhoneIdentity phoneIdentity : list){
                preparedStatement.setString(1, phoneIdentity.getAccount());
                preparedStatement.setString(2, phoneIdentity.getVisitTime());
                preparedStatement.setString(3, phoneIdentity.getImei());
                preparedStatement.addBatch();
                recordCount++;
                if(recordCount % batchSize == 0){
                    preparedStatement.executeBatch();
                    connection.commit();
                }
            }
            if(recordCount % batchSize != 0){
                preparedStatement.executeBatch();
                connection.commit();
            }
            connection.setAutoCommit(Boolean.TRUE);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            this.insertMills = System.currentTimeMillis() - currMills;
        }
    }

    private void parseData(byte[] source){
        long currMills = System.currentTimeMillis();
        int linesCount = 0;
        TarArchiveInputStream tarArchiveInputStream = null;
        GzipCompressorInputStream gzipCompressorInputStream = null;
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(source);
            gzipCompressorInputStream = new GzipCompressorInputStream(byteArrayInputStream);
            tarArchiveInputStream = new TarArchiveInputStream(gzipCompressorInputStream);
            ArchiveEntry archiveEntry = null;
            while ((archiveEntry = tarArchiveInputStream.getNextEntry()) != null){
                LineIterator lineIterator = IOUtils.lineIterator(tarArchiveInputStream, "UTF-8");
                while (lineIterator.hasNext()){
                    String line = lineIterator.nextLine();
                    //split line with char
                    String[] lineGroups = StringUtils.splitPreserveAllTokens(line, (char) 5);
                    if(lineGroups.length != 12) continue;
                    if(StringUtils.isAnyBlank(lineGroups[0], lineGroups[7], lineGroups[11])){
                        continue;
                    }
                    if(!StringUtils.contains(lineGroups[7], "=")){
                        continue;
                    }
                    String account = lineGroups[0].trim();
                    String time = lineGroups[11].trim();
                    //时间不规范则跳过
                    if(time.length() != 14) continue;
                    String url = lineGroups[7].trim();
                    List<String> imsiLike = findImeiLike(url);
                    for (String imeiLike : imsiLike){
                        if(DeviceUtil.valiedImei(imeiLike)){
                            this.list.add(new PhoneIdentity(account, time, imeiLike));
                        }
                    }
                    //文件行计数器
                    linesCount ++;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            this.processMills = System.currentTimeMillis() - currMills;
            this.sourceRecordCount = linesCount;
        }
    }

    //匹配IMEI非正则表达式版本
    public static List<String> findImeiLike(String url) {
        List<String> imeiLike = new ArrayList<String>();
        char[] charArray = url.toCharArray();
        char[] imei = new char[15];
        boolean inData = Boolean.FALSE;
        int imeiIndex = 0;
        for(int index = 0; index < charArray.length; index++){
            if(imeiIndex == 15 && ('&' == charArray[index] || index == (charArray.length - 1))){
                imeiLike.add(new String(imei));
                inData = Boolean.FALSE;
                imeiIndex = 0;
                continue;
            }
            if (inData){
                if (imeiIndex < 15 && Character.isDigit(charArray[index])){
                    imei[imeiIndex++] = charArray[index];
                }else {
                    imeiIndex = 0;
                    inData = Boolean.FALSE;
                }
            }
            if('=' == charArray[index]){
                imeiIndex = 0;
                inData = Boolean.TRUE;
            }
        }
        return imeiLike;
    }

    //匹配IMEI非正则表达式版本
    public static Map<String, List<String>> findImeiLikeAndMsisdn(String url) {
        Map<String, List<String>> result = new HashMap<String, List<String>>();
        List<String> imeiLike = new ArrayList<String>();
        List<String> msisdnLike = new ArrayList<String>();
        char[] charArray = url.toCharArray();
        //imei control
        char[] imei = new char[15];
        boolean inData = Boolean.FALSE;
        int imeiIndex = 0;
        for(int index = 0; index < charArray.length; index++){
            if(imeiIndex == 15 && ('&' == charArray[index] || index == (charArray.length - 1))){
                imeiLike.add(new String(imei));
                inData = Boolean.FALSE;
                imeiIndex = 0;
                continue;
            }
            if (inData){
                if (imeiIndex < 15 && Character.isDigit(charArray[index])){
                    imei[imeiIndex++] = charArray[index];
                }else {
                    imeiIndex = 0;
                    inData = Boolean.FALSE;
                }
            }
            if('=' == charArray[index]){
                imeiIndex = 0;
                inData = Boolean.TRUE;
            }
        }
        result.put("msisdn", msisdnLike);
        result.put("imei", imeiLike);
        return result;
    }

    public void insertJobItem(String jobTableName,
                                         String jobId, String taskId, String fileName, Long fileSize, Integer sourceRecordCount, Integer resultCount, Long processMills, Long insertMills, Date beginTime, Boolean success, Connection connection){
        java.sql.Date sqlDate = new java.sql.Date(beginTime.getTime());
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(String.format(SQL.INSERT_JOB_TABLE_SQL, jobTableName));
            preparedStatement.setString(1, jobId);
            preparedStatement.setString(2, taskId);
            preparedStatement.setString(3, fileName);
            preparedStatement.setLong(4, fileSize);
            preparedStatement.setInt(5, sourceRecordCount);
            preparedStatement.setInt(6, resultCount);
            preparedStatement.setLong(7, processMills);
            preparedStatement.setLong(8, insertMills);
            preparedStatement.setDate(9, sqlDate);
            preparedStatement.setInt(10, success ? 1 : 0);
            preparedStatement.execute();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 删除文件的时同时删除对应的校验文件
     * @param file
     * @return
     */
    public boolean deleteOkFile(File file){
        String fullPath = FilenameUtils.getFullPath(file.getAbsolutePath());
        String fullFileName = FilenameUtils.getName(file.getAbsolutePath());
        String okFile =  fullFileName.substring(0, fullFileName.length() - 6) + "ok";
        return FileUtils.deleteQuietly(new File(fullPath + okFile));
    }

    /**
     * 将String分割为二维字符数组，提升比对效率
     * @param source
     * @return
     */
    private char[][] msisdnParamToCharSource(String source){
        String[] mdnPrefixGroup = StringUtils.split(source, ",");
        char[][] result = new char[mdnPrefixGroup.length][3];
        for (int index = 0 ;index < mdnPrefixGroup.length; index++){
            result[index] = mdnPrefixGroup[index].toCharArray();
        }
        return result;
    }

    /**
     * 比对前缀是否在清单内
     * @param data
     * @param prefixLen
     * @param source
     * @return
     */
    private boolean msisdnPrefixCheck(char[] data, int prefixLen, char[][] source){
        int arrLen = source.length;
        for(int index = 0; index < arrLen; index++){
            if (data[0] == source[index][0]
                    && data[1] == source[index][1]
                    && data[2] == source[index][2]
            ){
                return true;
            }
        }
        return false;
    }

}
