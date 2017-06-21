package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zouzl
 * @create 2017-05-09-20:56
 */
public class IndexFile {
    public static final byte WRITESUCCESS = -1;
    public static final byte WRITEFAILURE = -1;
    public static final byte WRITEFULL = -3;
    public static final byte INDEX_FLAG=-1;

    private boolean IS_FULL=false;
    private String filename;
    private int filesize;
    private long fileFromOffset;
    private FileChannel fileChannel;
    private File file;
    private MappedByteBuffer mappedByteBuffer;
    private AtomicInteger wrotePosition=new AtomicInteger(0);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public IndexFile(String filename, int filesize) throws IOException {
        init(filename,filesize);
    }
    public void init(String filename, int filesize) throws IOException {
        this.filename = filename;
        this.filesize = filesize;
        this.file = new File(filename);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, filesize);
            //wrotePosition.set(mappedByteBuffer.position());
            //System.out.println(file.length());
            ok=true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
        executor.execute(new Sync());
    }

    public Message getMessageByOffset(int offset) {
        mappedByteBuffer.position(offset);
        byte msg_flag=mappedByteBuffer.get();
        if (msg_flag!=DefaultBytesMessage.MSG_FLAG)
            return null;

        byte flag = mappedByteBuffer.get();
        DefaultBytesMessage message=new DefaultBytesMessage();
        while ( flag==DefaultBytesMessage.HEAD_FLAG||
                flag==DefaultBytesMessage.PROP_FLAG||
                flag==DefaultBytesMessage.BODY_FLAG){

            if (flag==DefaultBytesMessage.HEAD_FLAG){//headers
                //取key
                byte valueType= mappedByteBuffer.get();
                int lenOfKey=(int)mappedByteBuffer.get();
                ByteBuffer buffer=mappedByteBuffer.slice();
                buffer.limit(lenOfKey);
                ByteBuffer keyBuffer =buffer.slice();
                String key=new String(keyBuffer.array());
                int p=mappedByteBuffer.position();
                mappedByteBuffer.position(p+lenOfKey);

                //取value
                if (valueType==DefaultBytesMessage.VALUE_TYPE_STRING){
                    int lenOfvalue=(int)mappedByteBuffer.get();
                    buffer=mappedByteBuffer.slice();
                    buffer.limit(lenOfvalue);
                    ByteBuffer valueBuffer =buffer.slice();
                    String value=new String(valueBuffer.array());

                    message.putHeaders(key,value);
                }
                if (valueType==DefaultBytesMessage.VALUE_TYPE_INT){
                    mappedByteBuffer.get();
                    int value=mappedByteBuffer.getInt();

                    message.putHeaders(key,value);
                }
                if (valueType==DefaultBytesMessage.VALUE_TYPE_LONG){
                    mappedByteBuffer.get();
                    long value=mappedByteBuffer.getLong();

                    message.putHeaders(key,value);
                }
                if (valueType==DefaultBytesMessage.VALUE_TYPE_LONG){
                    mappedByteBuffer.get();
                    double value=mappedByteBuffer.getDouble();

                    message.putHeaders(key,value);
                }
                //判断下一轮循环
                ByteBuffer temp=mappedByteBuffer.slice();
                byte check_flag = temp.get();
                if(check_flag==DefaultBytesMessage.HEAD_FLAG||
                        check_flag==DefaultBytesMessage.PROP_FLAG||
                        check_flag==DefaultBytesMessage.BODY_FLAG){
                    flag=mappedByteBuffer.get();
                }else if (check_flag==DefaultBytesMessage.MSG_FLAG){
                    //碰到了下一条信息，该条信息已经结束
                    return message;
                }
            }
            if (flag==DefaultBytesMessage.PROP_FLAG){//headers
                //取key
                byte valueType= mappedByteBuffer.get();
                int lenOfKey=(int)mappedByteBuffer.get();
                ByteBuffer buffer=mappedByteBuffer.slice();
                buffer.limit(lenOfKey);
                ByteBuffer keyBuffer =buffer.slice();
                String key=new String(keyBuffer.array());
                int p=mappedByteBuffer.position();
                mappedByteBuffer.position(p+lenOfKey);

                //取value
                if (valueType==DefaultBytesMessage.VALUE_TYPE_STRING){
                    int lenOfvalue=(int)mappedByteBuffer.get();
                    buffer=mappedByteBuffer.slice();
                    buffer.limit(lenOfvalue);
                    ByteBuffer valueBuffer =buffer.slice();
                    String value=new String(valueBuffer.array());

                    message.putProperties(key,value);
                }
                if (valueType==DefaultBytesMessage.VALUE_TYPE_INT){
                    mappedByteBuffer.get();
                    int value=mappedByteBuffer.getInt();

                    message.putProperties(key,value);
                }
                if (valueType==DefaultBytesMessage.VALUE_TYPE_LONG){
                    mappedByteBuffer.get();
                    long value=mappedByteBuffer.getLong();

                    message.putProperties(key,value);
                }
                if (valueType==DefaultBytesMessage.VALUE_TYPE_LONG){
                    mappedByteBuffer.get();
                    double value=mappedByteBuffer.getDouble();

                    message.putProperties(key,value);
                }
                //判断下一轮循环
                ByteBuffer temp=mappedByteBuffer.slice();
                byte check_flag = temp.get();
                if(check_flag==DefaultBytesMessage.HEAD_FLAG||
                        check_flag==DefaultBytesMessage.PROP_FLAG||
                        check_flag==DefaultBytesMessage.BODY_FLAG){
                    flag=mappedByteBuffer.get();
                }else if (check_flag==DefaultBytesMessage.MSG_FLAG){
                    //碰到了下一条信息，该条信息已经结束
                    return message;
                }
            }
            if (flag==DefaultBytesMessage.BODY_FLAG){
                int len_body=mappedByteBuffer.getInt();
                ByteBuffer buffer=mappedByteBuffer.slice();
                buffer.limit(len_body);
                ByteBuffer bodybuffer=buffer.slice();
                message.setBody(bodybuffer.array());

                //判断下个循环是什么
                ByteBuffer temp=mappedByteBuffer.slice();
                byte check_flag = temp.get();
                if(check_flag==DefaultBytesMessage.HEAD_FLAG||
                        check_flag==DefaultBytesMessage.PROP_FLAG||
                        check_flag==DefaultBytesMessage.BODY_FLAG){
                    flag=mappedByteBuffer.get();
                }else if (check_flag==DefaultBytesMessage.MSG_FLAG){
                    //碰到了下一条信息，该条信息已经结束
                    return message;
                }
            }
        }
        return message;
    }

    public long getMessageOffset(int offset) {
        byte flag=mappedByteBuffer.get(offset);
        if (flag==INDEX_FLAG){
            return mappedByteBuffer.getLong(offset+1);
        }
        return -1;
    }

    public class Sync implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (mappedByteBuffer != null) {
                    try {

                            //当写满的时候强制刷盘
                            mappedByteBuffer.force();
                            //System.out.println("强制刷新更改到磁盘。。。。");

                    } catch (Exception e) {
                        break;
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        break;
                    }
                } else {
                    break;
                }
            }

        }

    }
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
            }
        }
    }
    public boolean isFull() {

        return IS_FULL;
    }
    public boolean isFull(int increment) {
        if (this.filesize < wrotePosition.get() + increment) {
            return true;
        }
        return false;
    }
    public long write(byte[] data){
        int increment = data.length;
        if (isFull(increment)) {
            this.IS_FULL=true;
            return fileFromOffset+filesize;
        }
        mappedByteBuffer.position(wrotePosition.get());
        mappedByteBuffer.put(data);
        wrotePosition.getAndAdd(increment);
        return fileFromOffset+(long) wrotePosition.get();
    }
    public ByteBuffer selectMappedBuffer(int pos, int size) {

        if ((pos + size) <= wrotePosition.get()) {
            //ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            this.mappedByteBuffer.position(pos).limit(pos+size);
            ByteBuffer buffer=this.mappedByteBuffer.slice();
            return buffer;
        }
        return null;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public void force(){this.mappedByteBuffer.force();
    }
}



