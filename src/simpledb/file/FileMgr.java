package simpledb.file;

import sun.rmi.runtime.NewThreadAction;

import static simpledb.file.Page.BLOCK_SIZE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: simpleDB
 * @description: 数据库系统将数据存储在指定目录下的指定文件中。
 * 结合OS具体执行相应的IO操作，提供给Page类的read,write和append方法
 * read将文件中的一个block读取到page的字节数组中
 * write将page中的字节数组写回到文件中的指定block
 * append将申请一个新的块追加在文件尾部，并将page中的数组内容作写入该块
 * @author: LiuZhian
 * @create: 2019-07-03 23:08
 **/
public class FileMgr {
    private File dbDirectory;
    private boolean isNew;
    // 已经打开的文件channel
    private Map<String, FileChannel> openFiles = new HashMap<>();

    /**
     * Creates a file manager for the specified database.
     * Files for all temporary tables (i.e. tables beginning with "temp") are deleted.
     *
     * @param dbName 指定数据库文件存放的目录
     */
    public FileMgr(String dbName) {
        // 默认根目录为用户目录
        String homeDir = System.getProperty("user.home");
        dbDirectory = new File(homeDir, dbName);
        // 如果该目录不存在，会创建
        isNew = !dbDirectory.exists();


        // 创建新目录时失败
        if (isNew && !dbDirectory.mkdir())
            throw new RuntimeException("cannot make " + dbName);

        // 移除临时表
        for (String fileName : dbDirectory.list()) {
            if (fileName.startsWith("temp")) {
                new File(dbDirectory, fileName).delete();
            }
        }
    }

    /**
     * 将块中的内容读取到字节缓冲区(Page的成员变量)
     *
     * @param blk
     * @param bb
     */
    public synchronized void read(Block blk, ByteBuffer bb) {
        bb.clear();
        try {
            FileChannel fc = getFile(blk.getFileNama());
            fc.read(bb, blk.getBlockNum() * BLOCK_SIZE);
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }


    /**
     * 将字节缓冲区(Page的成员变量)中的内容写回块中
     *
     * @param blk
     * @param bb
     */
    public synchronized void write(Block blk, ByteBuffer bb) {
        bb.rewind();
        try {
            FileChannel fc = getFile(blk.getFileNama());
            fc.write(bb, blk.getBlockNum() * BLOCK_SIZE);
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    /**
     * 将字节缓冲区(Page的成员变量)中的内容追加到指定文件后,返回新的块引用
     *
     * @param fileName 文件名
     * @param bb       字节缓冲区
     * @return 新创建的块的引用
     */
    public synchronized Block append(String fileName, ByteBuffer bb) {
       int newBlkNum=size(fileName);  // 从0开始编号，所以刚好是当前块的数量
        Block blk=new Block(fileName, newBlkNum);
        write(blk,bb);
        return blk;
    }

    /**
     * 返回一个文件的块数量
     * @param fileName
     * @return
     */
    public synchronized int size(String fileName) {
        FileChannel fc = null;
        try {
            fc = getFile(fileName);
            return (int) fc.size() / BLOCK_SIZE;
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + fileName);
        }

    }

    /**
     * 返回指定文件的channel，这些channel都保存在hash表中。
     * 如果该文件未被打开，则打开并加入hash表
     *
     * @param fileName 打开的文件
     * @return FileChannel对象
     * @throws FileNotFoundException
     */
    private FileChannel getFile(String fileName) throws FileNotFoundException {
        FileChannel fc = openFiles.get(fileName);
        if (fc == null) {
            File dbTable = new File(dbDirectory, fileName);
            // 相对于rw模式，还要求对文件的内容或元数据的每个更新都同步写入到底层存储设备
            // “s” portion specifies that the OS should not delay disk I/O in order to optimize disk
            //performance; instead, every write operation must be written immediately to the disk
            RandomAccessFile f = new RandomAccessFile(dbTable, "rws");
            fc = f.getChannel();
            openFiles.put(fileName, fc);
        }
        return fc;
    }

    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean aNew) {
        isNew = aNew;
    }

}
