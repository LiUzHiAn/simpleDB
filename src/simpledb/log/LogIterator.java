package simpledb.log;

import simpledb.file.Block;
import simpledb.file.Page;

import javax.xml.crypto.dsig.keyinfo.PGPData;
import java.util.Iterator;
import java.util.function.Consumer;

import static simpledb.file.Page.INT_SIZE;

/**
 * @program: simpleDB
 * @description: 逆序遍历日志文件的所有日志记录
 * @author: LiuZhian
 * @create: 2019-07-04 21:04
 **/
public class LogIterator implements Iterator<BasicLogRecord> {
    private Block blk;              // 对应的块
    private Page page=new Page();   // 对应的页
    private int currentRec;         // 当前记录

    LogIterator(Block blk)
    {
        this.blk=blk;
        page.read(blk);
        currentRec=page.getInt(LogMgr.LAST_POS);  // 构造函数中最后一条记录为当前记录
    }

    @Override
    public boolean hasNext() {
        return currentRec>0 || blk.getBlockNum()>0;
    }

    @Override
    public BasicLogRecord next() {
       if (currentRec==0)
           moveToNextBlock();

       currentRec= page.getInt(currentRec);
       return new BasicLogRecord(page,currentRec+INT_SIZE);
    }

    private void moveToNextBlock() {
        blk=new Block(blk.getFileNama(),blk.getBlockNum()-1);
        page.read(blk);
        currentRec = page.getInt(LogMgr.LAST_POS);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
