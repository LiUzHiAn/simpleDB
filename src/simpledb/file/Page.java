package simpledb.file;

import simpledb.server.SimpleDB;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @author LiuZhian
 * @program: simpleDB
 * @description: 磁盘块中的具体内容，即在内存中的页，一个页实质上就是长度为BLOCK_SIZE大小的字节数组
 * Page类中包含 get/set 字节数组的方法，也包含将字节数组 读/写 回磁盘的方法。
 * 以下是如何配合Block类和Page类使用的两个用例：
 * 1. 将文件junk的第6个块上第792 offset的int数字加1
 * 2. 将字符串"hello"添加在一个页的第20 offset开始的位置，
 * 并将该页的内容追加到文件junk的一个新分配的block中，
 * 再将新分配的的block中的内容读取到一个新的页中
 * <pre>
 * Page p1 = new Page();
 * Block blk = new Block("junk", 6);
 * p1.read(blk);
 * int n = p1.getInt(792);
 * p1.setInt(792, n+1);
 * p1.write(blk);
 *
 * Page p2 = new Page();
 * p2.setString(20, "hello");
 * blk = p2.append("junk");
 * Page p3 = new Page();
 * p3.read(blk);
 * String s = p3.getString(20);
 * </pre>
 * @time 2019-07-03 22:14
 **/
public class Page {

    /**
     * 这里BLOCK_SIZE参数设置的较小，是为了创建数据库时尽量多分配block，方便测试
     * 常见的BLOCK_SIZE大小为4K，且为2的次方
     */
    public static final int BLOCK_SIZE = 400;

    /**
     * 一个Int变量的字节数，其实就是4字节
     */
    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    /**
     * 块中的具体内容，字节数组存放
     * The code for Page uses the allocateDirect method, which tells the
     * compiler to use one of the operating system‟s I/O buffers to hold the byte array
     */
    private ByteBuffer contents = ByteBuffer.allocateDirect(BLOCK_SIZE);
    private FileMgr fileMgr = SimpleDB.getFileMgr();

    /**
     * The maximum size, in bytes, of a string of length n.
     * A string is represented as the encoding of its characters,
     * preceded by an integer denoting the number of bytes in this encoding.
     * If the JVM uses the US-ASCII encoding, then each char
     * is stored in one byte, so a string of n characters
     * has a size of 4+n bytes.
     *
     * @param n the size of the string
     * @return the maximum number of bytes required to store a string of size n
     */
    public static int STR_SIZE(int n) {
        float bytesPerChar = Charset.defaultCharset().newEncoder().maxBytesPerChar();
        return INT_SIZE + (n * (int) bytesPerChar);
    }

    public Page() {
    }

    /**
     * 将一个块中的内容读到Page中，这部分由辅助类FileManager结合OS完成
     *
     * @param blk 磁盘块的引用对象
     */
    public synchronized void read(Block blk) {
        fileMgr.read(blk, contents);
    }

    /**
     * 将Page中的内容写回到块中，这部分由辅助类FileManager结合OS完成
     *
     * @param blk 磁盘块的引用对象
     */
    public synchronized void write(Block blk) {
        fileMgr.write(blk, contents);
    }

    /**
     * 将Page中的内容追加到一个指定的文件后，
     *
     * @param fileName 待追加的文件名
     * @return 追加后的那个块的引用
     */
    public synchronized Block append(String fileName) {
        return fileMgr.append(fileName, contents);
    }

    /**
     * 读取页中指定offset开始的int数字，用户应该对传来的offset参数负责
     * 如果offset位置开始读不到int，结果不可预期
     *
     * @param offset
     * @return
     */
    public synchronized int getInt(int offset) {
        contents.position(offset);
        return contents.getInt();
    }

    /**
     * 设置页中指定offset开始的int数字为val
     *
     * @param offset
     * @param val
     */
    public synchronized void setInt(int offset, int val) {
        contents.position(offset);
        contents.putInt(val);
    }

    /**
     * 读取页中指定offset开始的字符串，用户应该对传来的offset参数负责
     * 如果offset位置开始读不到int，结果不可预期
     *
     * @param offset
     * @return
     */
    public synchronized String getString(int offset) {
        contents.position(offset);
        // 一个字符串在底层的编码中格式设置为：字符串长度(一个int类型)+各字符的ascii码
        int len = contents.getInt();
        byte[] bytes = new byte[len];
        contents.get(bytes);
        return new String(bytes);
    }

    /**
     * 设置页中指定offset开始的字符串
     *
     * @param offset
     * @param val
     */
    public synchronized void setString(int offset, String val) {
        contents.position(offset);
        byte[] bytes = val.getBytes();
        // 先放len，再放ASCII码
        contents.putInt(bytes.length);
        contents.put(bytes);
    }


}
