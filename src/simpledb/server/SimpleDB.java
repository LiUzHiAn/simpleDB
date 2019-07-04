package simpledb.server;

import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;

/**
 * @program: simpleDB
 * @description:
 * @author: LiuZhian
 * @create: 2019-07-03 23:07
 **/
public class SimpleDB {
    public static String LOG_FILE = "simpledb.log";

    private static FileMgr fileMgr;
    private static LogMgr logMgr;


    /**
     * 初始化数据库系统
     *
     * @param dirName 数据库保存的目录名
     */
    public static void init(String dirName) {
        initFileAndLogMgr(dirName);
        boolean isNew = fileMgr.isNew();
        if (isNew) {
            System.out.println("creating a new database");
        } else {
            System.out.println("recovering the existing database");
        }
    }

    /**
     * 创建文件管理对象、日志管理对象
     * @param dirName
     */
    private static void initFileAndLogMgr(String dirName) {
        initFileMgr(dirName);
        logMgr=new LogMgr(LOG_FILE);
    }

    /**
     * 创建文件管理对象
     * @param dirName
     */
    private static void initFileMgr(String dirName)
    {
        fileMgr=new FileMgr(dirName);
    }

    public static FileMgr getFileMgr() {
        return fileMgr;
    }
    public static LogMgr getLogMgr() {
        return logMgr;
    }
}
