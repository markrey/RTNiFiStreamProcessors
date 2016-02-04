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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.util.*;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.json.JSONArray;

@Tags({"ReactToInsteonProcessor"})
@CapabilityDescription("Processes Insteon messages and takes any needed actions")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ReactToInsteonProcessor extends AbstractProcessor {
    
    String triggerOn;
    String[] triggerOnArray;
    String triggerOff;
    String[] triggerOffArray;
    String controlledDevice;
    String destination;
    JSONObject statusObject;
   
    public static final PropertyDescriptor PROPERTY_CONTROLLED_DEVICE = new PropertyDescriptor
            .Builder().name("Insteon address of controlled device")
            .description("The Insteon address (in decimal) of the device to be controlled")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROPERTY_CONTROL_TRIGGERON = new PropertyDescriptor
            .Builder().name("Trigger on equation")
            .description("Insteon addresses (in decimal) and logic separated with spaces (preceed with ! to invert)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor PROPERTY_CONTROL_TRIGGEROFF = new PropertyDescriptor
            .Builder().name("Trigger off equation")
            .description("Insteon addresses (in decimal) and logic separated with spaces (preceed with ! to invert)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROPERTY_CONTROL_DESTINATION = new PropertyDescriptor
            .Builder().name("Control destination")
            .description("Where to send the control messages (e.g. an MQTT topic)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship RELATIONSHIP_CONTROL = new Relationship.Builder()
            .name("Control")
            .description("Insteon control messages")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROPERTY_CONTROLLED_DEVICE);
        descriptors.add(PROPERTY_CONTROL_TRIGGERON);
        descriptors.add(PROPERTY_CONTROL_TRIGGEROFF);
        descriptors.add(PROPERTY_CONTROL_DESTINATION);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(RELATIONSHIP_CONTROL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            triggerOn = context.getProperty(PROPERTY_CONTROL_TRIGGERON).getValue();
            triggerOff = context.getProperty(PROPERTY_CONTROL_TRIGGEROFF).getValue();
            controlledDevice = context.getProperty(PROPERTY_CONTROLLED_DEVICE).getValue();
            destination = context.getProperty(PROPERTY_CONTROL_DESTINATION).getValue();
            
            triggerOnArray = triggerOn.split(" ");
            triggerOffArray = triggerOff.split(" ");
        } catch(Exception e) {
            getLogger().error("onScheduled failed " + e.getMessage());
        }
    }
 
    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
    }
    

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
 
        final AtomicReference<String> message = new AtomicReference<>();
 
        FlowFile flowfile = session.get();
         
        // do the read
        
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                    message.set(IOUtils.toString(in));
                }catch(Exception e){
                    getLogger().error("Failed to read flowfile " + e.getMessage());
                }
            }
        });
        
        String output = message.get();
        
        if ((output == null) || output.isEmpty()) {
            session.remove(flowfile);
            return;
        }
    
        Map deviceLevels = new HashMap();
        JSONArray statusArray;

        try {
            statusObject = new JSONObject(output);
            try {
                statusArray = statusObject.getJSONArray("updateList");
            } catch (Exception e) {
                // not status update
                session.remove(flowfile);
                return;
            }
            
            //  locate all the device levels
            
            for (int i = 0; i < statusArray.length(); i++) {
                JSONObject statusEntry = statusArray.getJSONObject(i);
                deviceLevels.put(statusEntry.get("deviceID").toString(), statusEntry.get("currentLevel").toString());
            }
            
        } catch(Exception e) {
            getLogger().error("Failed to decode JSON " + e.getMessage());
            session.remove(flowfile);
            return;
        }

        boolean onTriggered = false;
        boolean offTriggered = false;
        
        try {
            onTriggered = parseControlTrigger(deviceLevels, triggerOnArray);
            offTriggered = parseControlTrigger(deviceLevels, triggerOffArray);
        } catch (Exception e) {
            getLogger().error("Failed to process triggers " + e.getMessage());
            session.remove(flowfile);
            return;
        }
          
        if (!onTriggered && !offTriggered) {
            // nothing happened
            session.remove(flowfile);
            return;           
        }
        
        //  build the control message
        
        JSONArray jsa = new JSONArray();
        JSONObject jso = new JSONObject();
        final JSONObject controlObject = new JSONObject();
       
        jso.put("deviceID", Integer.parseInt(controlledDevice));
        if (onTriggered)
            jso.put("newLevel", 255);
        else
            jso.put("newLevel", 0);
        jsa = jsa.put(jso);
        controlObject.put("setDeviceLevel", jsa);
        
        flowfile = session.putAttribute(flowfile, "destination", destination);
        flowfile = session.write(flowfile, new OutputStreamCallback() {
 
        @Override
            public void process(final OutputStream out) throws IOException {
                out.write(controlObject.toString().getBytes());
            }
        });

        session.transfer(flowfile, RELATIONSHIP_CONTROL);
    }

    private boolean parseControlTrigger(Map deviceLevels, String[] triggerArray) throws Exception {
        if (triggerArray.length == 0)
            return false;
        
        if ((triggerArray.length % 2) == 0) 
            throw new Exception("Incorrect trigger format with even number of tokens");
        
        
        boolean state = false;
        boolean deviceState;
        String op;
        
        state = getDeviceState(deviceLevels, triggerArray[0]);
        
        for (int pos = 1; pos < triggerArray.length; pos += 2) {
            op = triggerArray[pos];
            deviceState = getDeviceState(deviceLevels, triggerArray[pos + 1]);
            
            switch (op) {
                case "+":
                    state |= deviceState;
                    break;
                    
                case "*":
                    state &= deviceState;
                    break;
                    
                default:
                    throw new Exception("Illegal logical operator " + op);
            }
        }
        return state;
    }
     
    private boolean getDeviceState(Map deviceLevels, String deviceID) {
        if (deviceID.startsWith("!")) {
            deviceID = deviceID.replaceFirst("!", "");
            return Integer.parseInt(deviceLevels.get(deviceID).toString()) <= 0;
        } else {
            return Integer.parseInt(deviceLevels.get(deviceID).toString()) > 0;                
        }
    }
}
