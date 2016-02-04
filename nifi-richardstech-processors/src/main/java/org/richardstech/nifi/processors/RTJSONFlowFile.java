//  Copyright 2015-2016 richards-tech, LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package org.richardstech.nifi.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.json.JSONObject;

public class RTJSONFlowFile
{
    //  JSON field defs
    
    public static final String ATTRIBUTES = "attributes";
    public static final String FLOWFILE = "flowfile";
    public static final String FINAL_FLAG = "final";
    
    private boolean finalFlowFile = false;
       
    public JSONObject read(final ProcessSession session, FlowFile flowfile) throws IOException {
        
        final AtomicReference<String> filedata = new AtomicReference<>();
        JSONObject jsonFlowFile = new JSONObject();
        JSONObject jsonAttrs = new JSONObject();
        
        // do the read
        
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                filedata.set(IOUtils.toString(in));
            }
        });        
 
        Map<String,String> attrs = flowfile.getAttributes();
        
        jsonFlowFile.put(FLOWFILE, filedata.get());
        
        for (Map.Entry<String,String> entry : attrs.entrySet()) {
            jsonAttrs.put(entry.getKey(), entry.getValue());
        }
        jsonFlowFile.put(ATTRIBUTES, jsonAttrs);
        
        return jsonFlowFile;
    }
    
    public FlowFile write(final ProcessSession session, FlowFile flowfile, String jsonString) throws Exception {
        final JSONObject jsonFlowFile = new JSONObject(jsonString);
        
        JSONObject attrs = jsonFlowFile.getJSONObject(ATTRIBUTES);
        final String filedata = jsonFlowFile.getJSONObject(FLOWFILE).toString();
        
        Iterator<?> keys = attrs.keys();

        while(keys.hasNext()) {
            String key = (String)keys.next();
            if (key == "uuid")
                continue;
            flowfile = session.putAttribute(flowfile, key, attrs.getString(key));
        }

        try {
            finalFlowFile = jsonFlowFile.getBoolean(FINAL_FLAG);
        } catch (Exception e) {
            finalFlowFile = true;
        }
        
        flowfile = session.write(flowfile, new OutputStreamCallback() {

            @Override
            public void process(final OutputStream out) throws IOException {
                out.write(filedata.getBytes());
            }
        });
        return flowfile;
    }
    
    public boolean isFinal() {
        return finalFlowFile;
    }
}
