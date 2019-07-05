package simpledb.buffer;

import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

/**
 * @program: simpleDB
 * @description:
 * @author: LiuZhian
 * @create: 2019-07-05 20:27
 **/
public class BufferTest {
    public static void main(String[] args) {
        SimpleDB.init("studentDB");


        BufferMgr bufferMgr = SimpleDB.getBufferMgr();
        LogMgr logMgr=SimpleDB.getLogMgr();

//        Page page = new Page();
//        page.setInt(20,123);
//        page.setString(50,"hello");
//        Block blk = new Block("junk", 0);
//        page.write(blk);
        Page page = new Page();
        Block blk = new Block("junk", 0);
        page.setInt(20,123);
        page.setString(50,"hello");
        page.write(blk);


        Buffer buff = bufferMgr.pin(blk);
        int n = buff.getInt(20);
        String str = buff.getString(50);
        System.out.println("The values are " + n + " and " + str);

        int myTxNum=1;
        Object[] logRec=new Object[]{"junk",0,50,str};
        int lsn=logMgr.append(logRec);
        buff.setString(50,"world",myTxNum,lsn);
        bufferMgr.flushAll(myTxNum);
        bufferMgr.unpin(buff);

    }
}
