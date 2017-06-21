package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.util.AllUtils;

public class DefaultBytesMessage implements BytesMessage {
    public static byte VALUE_TYPE_INT=1;
    public static byte VALUE_TYPE_LONG=2;
    public static byte VALUE_TYPE_DOUBLE=3;
    public static byte VALUE_TYPE_STRING=4;

    public static int LEN_TYPE=1;
    public static int LEN_KEY=1;
    public static int LEN_VALUE=1;
    public static int LEN_BODY=1;

    public static int LEN_INT=4;
    public static int LEN_LONG=8;
    public static int LEN_DOUBLE=8;

    public static byte HEAD_FLAG=-1;
    public static byte PROP_FLAG=-2;
    public static byte BODY_FLAG=-3;
    public static byte MSG_FLAG=-4;

    private KeyValue headers = new DefaultKeyValue();
    private byte[] heads={};
    private KeyValue properties;
    private byte[] props={};
    private byte[] body={};
    public DefaultBytesMessage(){

    }
    public DefaultBytesMessage(byte[] body) {
        setBody(body);
        //this.body = body;
    }
    public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] bodys) {
        if (body.length==0)
            this.body=new byte[]{BODY_FLAG};
        int srcLen=this.body.length;
        int index=srcLen;
        byte[] newArray = new byte[srcLen+LEN_BODY+bodys.length];
        System.arraycopy(body,0,newArray,0,srcLen);

        newArray[index++]=(byte) bodys.length;
        System.arraycopy(bodys,0,newArray,index,bodys.length);
        this.body = newArray;
        return this;
    }
    public void setBodyAndNoFlag(byte[] body){
        this.body = body;
    }
    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        //放入map当中
        headers.put(key, value);
        appendHeader(key,AllUtils.intToByteArray(value),VALUE_TYPE_INT,LEN_INT);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        appendHeader(key,AllUtils.longToBytes(value),VALUE_TYPE_LONG,LEN_LONG);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, value);
        appendHeader(key,AllUtils.doubleToByte(value),VALUE_TYPE_DOUBLE,LEN_DOUBLE);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        appendHeader(key,value.getBytes(),VALUE_TYPE_STRING,value.getBytes().length);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        appendProps(key,AllUtils.intToByteArray(value),VALUE_TYPE_INT,LEN_INT);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        appendProps(key,AllUtils.longToBytes(value),VALUE_TYPE_LONG,LEN_LONG);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        appendProps(key,AllUtils.doubleToByte(value),VALUE_TYPE_DOUBLE,LEN_DOUBLE);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        appendProps(key,value.getBytes(),VALUE_TYPE_STRING,value.getBytes().length);
        return this;
    }


    public byte[] getHeaderBytes() {
        return heads;
    }


    public byte[] getPropertyBytes() {
        return props;
    }

    private void appendHeader(String key,byte[] value,byte typeOfvalue,int lenOfvalue){
        //拼接header的byte数组
        if (heads.length==0)
            heads=new byte[]{HEAD_FLAG};
        int srcLen=heads.length;
        byte[] newArray = new byte[srcLen+LEN_TYPE+LEN_KEY+key.getBytes().length+LEN_VALUE+lenOfvalue];//1字节表示value的数据类型，1字节表示key的长度，最后4字节表示固定的int值

        System.arraycopy(heads,0,newArray,0,srcLen);
        //数据类型
        int index=srcLen;
        newArray[index]=typeOfvalue;
        //key的长度
        newArray[++index]=(byte) key.getBytes().length;
        //key值
        System.arraycopy(key.getBytes(), 0, newArray, ++index, key.getBytes().length);
        index+=key.getBytes().length;
        //value长度
        newArray[index]=(byte) value.length;
        //value值
        System.arraycopy(value, 0, newArray, ++index, lenOfvalue);
        heads=newArray;
    }

    private void appendProps(String key,byte[] value,byte typeOfvalue,int lenOfvalue){
        if (props.length==0)
            props=new byte[]{PROP_FLAG};
        //拼接header的byte数组
        int srcLen=props.length;
        byte[] newArray = new byte[srcLen+LEN_TYPE+LEN_KEY+key.getBytes().length+LEN_VALUE+lenOfvalue];//1字节表示value的数据类型，1字节表示key的长度，最后4字节表示固定的int值

        System.arraycopy(props,0,newArray,0,srcLen);
        //数据类型
        int index=srcLen;
        newArray[index]=typeOfvalue;
        //key的长度
        newArray[++index]=(byte) key.getBytes().length;
        //key值
        System.arraycopy(key.getBytes(), 0, newArray, ++index, key.getBytes().length);
        index+=key.getBytes().length;
        //value长度
        newArray[index]=(byte) value.length;
        //value值
        System.arraycopy(value, 0, newArray, ++index, lenOfvalue);
        props=newArray;
    }
    public static void main(String[] args) {
        DefaultBytesMessage message=new DefaultBytesMessage("good luck to you !".getBytes());
        for(int i=0;i<10;i++){
            message.putHeaders("test_header_"+i,10+i);
            message.putProperties("test_prop_"+i,100+i);
        }
        System.out.println(message.heads);
    }
}
