package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.cruzdb.zlog.Log;
import org.cruzdb.zlog.LogException;

import java.io.*;
import java.util.*;

public class ZlogClient extends DB {

    private static Log instance = null;
    private LinkedHashMap<String, Long> keyMap = new LinkedHashMap<String, Long>();

    private Log getLog() throws LogException {
        HashMap<String, String> opts = new HashMap<String, String>();
        opts.put("path", "/home/jorge/Desktop/tmp/db");
    
        Random rand = new Random();
        String logname = "" + rand.nextInt();
        if (instance == null)         
            instance = Log.open("lmdb", opts, logname); 

        return instance;
    }

    @Override
    public Status read(String table, String key, Set<String> fields,
        Map<String, ByteIterator> result) {
            Long position = keyMap.get(key);
            byte[] output;
            try{
               output = getLog().read(position);  
            }catch(LogException e){
                return Status.ERROR;
            }
        try {
            StringByteIterator.putAllAsByteIterators(result,
                (Map<String, String>)toObject(output));
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return result.isEmpty() ? Status.ERROR : Status.OK;
    }
  
    @Override
    public Status insert(String table, String key,
        Map<String, ByteIterator> values) {
            Long position;
        try{
            position = getLog().append(
                toByteArray(StringByteIterator.getStringMap(values)));
            keyMap.put(key, position);
            return Status.OK;
        }catch(LogException | IOException e){
            e.printStackTrace();
        }
        return Status.ERROR;
    }
  
    @Override
    public Status delete(String table, String key) {
        Long position = keyMap.get(key);
        try{
            getLog().trim(position);
            keyMap.remove(key);  
        }catch(LogException e){
            return Status.ERROR;
        } 
        return Status.OK;
    }
  
    @Override
    public Status update(String table, String key,
        Map<String, ByteIterator> values) {
      return Status.ERROR;
    }
  
    @Override
    public Status scan(String table, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
            Set<String> keys = keyMap.keySet();
    
        HashMap<String, ByteIterator> values;

        ArrayList<String> keyArray = new ArrayList<>(keys);
        keyArray.subList(keyArray.indexOf(startkey), keyArray.size());
                
        for (String key : keyArray) {
            values = new HashMap<>();
            read(table, key, fields, values);
            result.add(values);             
        }    
        return Status.OK;
    }

    private byte[] toByteArray(Object obj) throws IOException {
        byte[] bytes = null;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
        } finally {
            if (oos != null) {
                oos.close();
            }
            if (bos != null) {
                bos.close();
            }
        }
        return bytes;
    }

    private Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
        Object obj = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            obj = ois.readObject();
        } finally {
            if (bis != null) {
                bis.close();
            }
            if (ois != null) {
                ois.close();
            }
        }
        return obj;
    }

}