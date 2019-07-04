package simpledb.log;

import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

import java.util.Iterator;

import static simpledb.file.Page.*;

/**
 * @program: simpleDB
 * @description: 日志管理单元，负责将操作数据库的日志记录保存到日志文件中
 * A log record can be any sequence of integer and string values.
 * 日志管理单元不管日志具体记录了什么信息，它只负责将日志信息持久化。
 * 具体解析日志记录的工作交给恢复单元RecoveryMgr来完成
 * @author: LiuZhian
 * @create: 2019-07-04 17:16
 **/
public class LogMgr implements Iterable<BasicLogRecord> {

    public static final int LAST_POS=0;    // 保存最后一条记录的内容位置的指针，就是页的前4个字节（int）
    private String logFileName;            // 日志文件名
    private Page myPage = new Page();      // 保存log记录的页
    private Block currentBlk;              // 当前块
    private int currentPos;                // 当前记录的指针位置

    /**
     * 为一个具体的日志文件创建一个日志管理对象，如果该日志文件不存在则创建一个新的空块
     * 该构造函数必须在FileMgr类的唯一对象被创建后在能调用，因为会涉及到一些I/O操作
     * @param logFileName
     */
    public LogMgr(String logFileName) {
        this.logFileName = logFileName;
        // 当前日志文件块数
        int logSize = SimpleDB.getFileMgr().size(logFileName);
        if (logSize == 0) {
            appendNewBlock();
        } else {
            // 获取到当前块
            currentBlk = new Block(logFileName, logSize - 1);
            // 将当前块中的内容读到页中的缓冲字节数组中
            myPage.read(currentBlk);

            currentPos = getLastRecortPos()+INT_SIZE;
        }
    }

    /**
     * 获得最后一条log记录的内容结尾位置
     * @return
     */
    private int getLastRecortPos() {
        return myPage.getInt(LAST_POS);
    }

    private  void setLastRecordPos(int pos)
    {
        myPage.setInt(LAST_POS,pos);
    }
    /**
     * Clear the current page, and append it to the log file.
     */
    private void appendNewBlock() {
        setLastRecordPosition(0);
        currentPos=INT_SIZE;
        currentBlk=myPage.append(logFileName);
    }


    /**
     * 插入一条log记录.
     * 一条日志记录包含任意长度的字节数组（int或string类型），注意，为了方便逆序遍历log几率，
     * 在每条log的尾部加上一个int数字来表示上一条log的位置
     *
     * @param rec
     * @return 返回该日志的编号 log sequence number
     */
    public int append(Object[] rec) {
        int recSize = INT_SIZE;  // 该条日志的长度（包括最后一个int）
        for (Object obj : rec) {
            recSize += sizeOf(obj);
        }
        if (recSize + currentPos >= BLOCK_SIZE) // 超过了一个块的大小
        {
            flush();            // flush当前块到disk上去
            appendNewBlock();   // 追加一个新的块作为当前块
        }
        for (Object obj : rec) {
            appendVal(obj);
        }
        finalizeRecord();
        return currentLSN();
    }

    /**
     * 在当前页上建立一条维护log记录的链，
     * 缓冲器的前4个字节（第一个int）表示最后一个log记录的内容结束位置
     * 每一条log记录末尾也有一个int来指示上一条log内容的结束位置
     */
    private void finalizeRecord() {
        myPage.setInt(currentPos,getLastRecortPos());
        setLastRecordPos(currentPos);
        currentPos+=INT_SIZE;
    }

    /**
     * 将一个值添加到page的缓存中，添加的位置由currentPos标定
     * @param obj
     */
    private void appendVal(Object obj) {
        if(obj instanceof String)
        {
            myPage.setString(currentPos,(String)obj);
        }
        else
            myPage.setInt(currentPos,(Integer)obj);
        currentPos+=sizeOf(obj);
    }

    /**
     * 统计一个整形或字符串持久化到文件中的长度（字符串首部包括一个指示长度的整数）
     *
     * @param val
     * @return 对象的字节数
     */
    private int sizeOf(Object val) {
        if (val instanceof String) {
            String sval = (String) val;
            return STR_SIZE(sval.length());
        } else {
            return INT_SIZE;
        }
    }

    /**
     * 确保用户指定LSN的log记录被写入了磁盘上，更早的记录肯定也已经被写入disk
     * @param lsn
     */
    public void flush(int lsn) {
        if(lsn>= currentLSN())
            flush();
    }

    /**
     * 将当前页中的内容写入日志文件磁盘块上去
     */
    private void flush()
    {
        myPage.write(currentBlk);   // 将页中缓冲区写到磁盘块上去
    }

    public Iterator<BasicLogRecord> iterator() {
        flush();  // 将当前块中的内容写入到文件
        return new LogIterator(currentBlk);   // 从当前block开始的所有日志记录迭代
    }

    /**
     * 在页的最开始一个int字节写最后一条log记录的内容结束位置
     * @param pos
     */
    private void setLastRecordPosition(int pos) {
        myPage.setInt(LAST_POS,pos);
    }

    /**
     * 返回最近的一条log的LSN（log sequence number）
     * 在这里，LSN只简单地用块号去实现，因此所有在一个块内的log信息拥有相同的LSN。
     * typically，常见的LSN实现方法是块号+当前log的起始位置
     * @return
     */
    private int currentLSN(){
        return currentBlk.getBlockNum();
    }

}
