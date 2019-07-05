package simpledb.buffer;

import jdk.nashorn.internal.ir.ReturnNode;
import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

/**
 * 缓存池中的一个缓存单元，可以看做就是一个页，其中包含了某个块的信息。
 * 这些信息包括：
 * 块号、该块被固定（pinned）到缓存的时间、该块被解固（unpinned）的时间
 * 该缓存单元中的信息是否被修改，如果被修改还需要将修改的日志保存到日志文件，
 * 并将修改的信息写回到磁盘上。
 *
 * @program: simpleDB
 * @description: 块缓存
 * @author: LiuZhian
 * @create: 2019-07-05 15:55
 **/
public class Buffer {
    private Page contens = new Page();
    private Block blk = null;
    private int pins=0;                 // 当前缓冲单元被pin的次数
    private int modifiedBy=-1;          //  表示是哪个事务修改的，-1表示未修改
    private int logSequenceNum=-1;      //  -1表示无需写log记录

    /**
     * 该构造函数将被BufferMgr显示调用，
     * 并且该构造函数调用之前，SimpleDB类中的initFileAndLogMgr()方法必须已经被调用
     * 因为Buffer依赖于Log
     */
    public Buffer() {

    }

    /**
     * 获取缓冲单元offset位置开始的int数据，其实就是对访问page对象的方法
     * @param offset
     * @return
     */
    public int getInt(int offset) {
        return contens.getInt(offset);

    }
    /**
     * 获取缓冲单元offset位置开始的String数据，其实就是对访问page对象的方法
     * @param offset
     * @return
     */
    public String getString(int offset) {
        return contens.getString(offset);
    }

    /**
     * 在指定offset位置写数据，该方法假定相关事务已经写好了一个日志记录
     * 该buffer保存事务id和log记录的lsn
     * @param offset
     * @param val
     * @param txnum 修改事务id
     * @param lsn 对应log记录的lsn,-1表示无需保存log记录
     */
    public void setInt(int offset, int val, int txnum, int lsn) {
        modifiedBy=txnum;
        if(lsn>=0)
        {
            logSequenceNum=lsn;
        }
        contens.setInt(offset,val);
    }
    /**
     * 在指定offset位置写数据，该方法假定相关事务已经写好一个日志记录。
     * 该buffer保存事务id和log记录的lsn
     * @param offset
     * @param val
     * @param txnum 修改事务id
     * @param lsn 对应log记录的lsn,-1表示无需保存log记录
     */
    public void setString(int offset, String val, int txnum, int lsn) {
        modifiedBy=txnum;
        if(lsn>=0)
        {
            logSequenceNum=lsn;
        }
        contens.setString(offset,val);
    }

    /**
     * 返回该buffer上对应固定的block对象的引用
     * @return
     */
    public Block block() {
        return blk;
    }

    /**
     * 如果该缓冲区对应的页是脏页（也就是被修改过），则写回到磁盘。
     * 注意，在写回到磁盘前，必须将日志记录也追加到日志文件中
     */
    public void flush()
    {
        if(modifiedBy>=0)
        {
            SimpleDB.getLogMgr().flush(logSequenceNum);
            contens.write(blk);
            modifiedBy=-1;  // 写回磁盘成功后，别忘了把dirty位重新置为-1
        }
    }

    void pin()
    {
        pins++;
    }
    void unpin()
    {
        pins--;
    }

    /**
     * 返回当前缓冲是否固定了块
     * @return
     */
    boolean isPinned()
    {
        return pins>0;
    }

    /**
     * 返回当前缓冲区是否被指定的事务id号对应的事务修改过
     * @param txNum
     * @return
     */
    boolean isModifiedBy(int txNum)
    {
        return this.modifiedBy==txNum;
    }

    /**
     * 将指定块中的内容保存到缓冲区的页中。
     * 如果当前页为脏页，必须将页中的当前内容写回到磁盘
     * @param b
     */
    void assignToBlock(Block b)
    {
        flush();
        blk=b;
        contens.read(blk);
        pins=0;
    }
    /**
     * Initializes the buffer's page according to the specified formatter,
     * and appends the page to the specified file.
     * If the buffer was dirty, then the contents
     * of the previous page are first written to disk.
     * @param filename the name of the file
     * @param fmtr a page formatter, used to initialize the page
     */
    public void assignToNew(String filename, PageFormatter fmtr) {
        flush();
        fmtr.format(contens);
        blk=contens.append(filename);
        pins=0;
    }
}
