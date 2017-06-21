package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    private MappedFileQueue mappedFileQueue =null;
    private Map<String,IndexFileQueue> indexBuckets=null;
    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();
    private Integer filesize =1024*1024*1024;

    private IndexFileQueue getIndexFileQueue(String bucket, String path){
        if (indexBuckets==null)
            indexBuckets=new HashMap<>();
        IndexFileQueue indexFileQueue=indexBuckets.get(bucket);
        if (indexFileQueue==null){
            indexFileQueue=new IndexFileQueue(path+ File.separator+bucket,filesize).init();
            if (indexFileQueue!=null){
                indexBuckets.put(bucket,indexFileQueue);
            }
        }
        return indexFileQueue;
    }

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    public synchronized void putMessage(String bucket, Message message, KeyValue properties) {
        if (mappedFileQueue ==null){
            mappedFileQueue =new MappedFileQueue(properties.getString("STORE_PATH"),filesize);
        }
        if (indexBuckets==null){
            indexBuckets=new HashMap<>();
        }
        if (!indexBuckets.containsKey(bucket)){
            indexBuckets.put(bucket,new IndexFileQueue(properties.getString("STORE_PATH")+ File.separator+bucket,filesize));
        }
        long offset= mappedFileQueue.getCommonWritePositon();
        //添加消息Bytes
        DefaultBytesMessage msg=(DefaultBytesMessage)message;
        byte[] msgBytes=convertMessageToByte(msg.getHeaderBytes(),msg.getPropertyBytes(),msg.getBody());
        boolean result=mappedFileQueue.offer(msgBytes);
        //信息添加成功才添加offset
        if (result){
            IndexFileQueue indexQueue = indexBuckets.get(bucket);
            ByteBuffer tmp=ByteBuffer.allocate(1+8);
            tmp.put(new byte[]{IndexFile.INDEX_FLAG});
            tmp.putLong(offset);
            indexQueue.offer(tmp.array());
        }
    }


    public synchronized Message pullMessage(String queue, String bucket,KeyValue properties) {
        IndexFileQueue index = getIndexFileQueue(bucket,properties.getString("STORE_PATH"));
        if (index==null){
            return null;
        }
        //queue是每个consumer一对一绑定的
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int indexOffset = offsetMap.getOrDefault(bucket,0);
        //if (indexOffset>index.getWritePositon()){
        //    return null;
        //}
        //获取消息索引
        long msgOffset=index.findMsgOffsetByIndex(indexOffset);
        //根据消息索引获取消息
        if (mappedFileQueue ==null){
            mappedFileQueue =new MappedFileQueue(properties.getString("STORE_PATH"),1024*1024*1024).init();
        }
        if (msgOffset==-1)
            return null;
        else{
            Message message= mappedFileQueue.getMessageByOffset(msgOffset);
            offsetMap.put(bucket,indexOffset+9);
            return message;
        }
    }


    public byte[] convertMessageToByte(byte[] headers, byte[] props, byte[] body){

        byte [] message=new byte[1+headers.length+props.length+body.length];
        byte[] msgflag=new byte[]{DefaultBytesMessage.MSG_FLAG};
        System.arraycopy(msgflag,0,message,0,msgflag.length);
        System.arraycopy(headers,0,message,msgflag.length,headers.length);
        System.arraycopy(props,0,message,msgflag.length+headers.length,props.length);
        System.arraycopy(body,0,message,msgflag.length+headers.length+props.length,body.length);
        return message;
    }
}
