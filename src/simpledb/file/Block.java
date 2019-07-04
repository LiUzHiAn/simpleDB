package simpledb.file;

import java.util.Objects;

/**
 * @program simpleDB
 * @description 对应磁盘块的引用，包含一个文件名和逻辑块号，块内的具体内容由Page类管理
 * @author LiuZhian
 * @create 2019-07-03 22:00
 **/
public class Block {
    private String fileNama;
    private int blockNum;

    public Block(String fileNama, int blockNum) {
        this.fileNama = fileNama;
        this.blockNum = blockNum;
    }

    public String getFileNama() {
        return fileNama;
    }

    public void setFileNama(String fileNama) {
        this.fileNama = fileNama;
    }

    public int getBlockNum() {
        return blockNum;
    }

    public void setBlockNum(int blockNum) {
        this.blockNum = blockNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Block block = (Block) o;
        return blockNum == block.blockNum &&
                Objects.equals(fileNama, block.fileNama);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileNama, blockNum);
    }

    @Override
    public String toString() {
        return "Block{" +
                "fileNama='" + fileNama + '\'' +
                ", blockNum=" + blockNum +
                '}';
    }
}
