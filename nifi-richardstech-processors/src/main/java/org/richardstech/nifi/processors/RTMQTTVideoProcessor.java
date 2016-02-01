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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import java.util.*;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import org.json.JSONObject;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;

@Tags({"RTMQTTVideoProcessor"})
@CapabilityDescription("Processes RTMQTT video messages")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class RTMQTTVideoProcessor extends AbstractProcessor {

    public static final String CYCLE_SECOND = "every second";
    public static final String CYCLE_MINUTE = "every minute";
    public static final String CYCLE_HOUR = "every hour";
    public static final String CYCLE_DAY = "every day";

    double lastTime;
    boolean firstTime = true;
    
    public static final PropertyDescriptor PROPERTY_CYCLE_INTERVAL = new PropertyDescriptor.Builder()
            .name("New file cycle interval")
            .description("Period covered by each individual video file")
            .required(true)
            .defaultValue(CYCLE_HOUR)
            .allowableValues(CYCLE_SECOND, CYCLE_MINUTE, CYCLE_HOUR, CYCLE_DAY)
            .build();

    public static final Relationship RELATIONSHIP_METADATA = new Relationship.Builder()
            .name("Metadata")
            .description("Video metadata output")
            .build();

    public static final Relationship RELATIONSHIP_VIDEO = new Relationship.Builder()
            .name("Video")
            .description("Video output")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROPERTY_CYCLE_INTERVAL);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(RELATIONSHIP_METADATA);
        relationships.add(RELATIONSHIP_VIDEO);
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
    }
 
    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AtomicReference<String> message = new AtomicReference<>();
        
        final String cycleInterval = context.getProperty(PROPERTY_CYCLE_INTERVAL).getValue();

        FlowFile messageFlowfile = session.get();
        
        if (messageFlowfile == null)
            return;
        
        message.set("");
        
         // do the read
        
        session.read(messageFlowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                    message.set(IOUtils.toString(in));
                }catch(Exception e){
                    getLogger().error("Failed to read flowfile " + e.getMessage());
                }
            }
        });
        try {
            session.remove(messageFlowfile);
        } catch (Exception e) {
             getLogger().error("Failed to remove flowfile " + e.getMessage());
             return;
        }       
       
        JSONObject obj;
        String originalDeviceID;
        String originalTimestamp;
        
        try {
            obj = new JSONObject(message.get());
            originalDeviceID = obj.get("deviceID").toString();
            originalTimestamp = obj.get("timestamp").toString();
                           
        } catch(Exception e) {
            getLogger().error("Failed to process message");
            return;
        }
        byte[] bytes = new byte[0];
        try {
            bytes = Base64.getDecoder().decode(obj.remove("video").toString());
        } catch(Exception e) {
            getLogger().error("Video was not in correct base64 format");
            return;
        }
        final byte[] video = bytes;
                         
        // work out the video filename
            
        Calendar originalCalendar = new GregorianCalendar();
        double originalTimeMs = Double.valueOf(originalTimestamp) * 1000.0;
            
        if (firstTime) {
            firstTime = false;
            lastTime = originalTimeMs;
        }
        if (lastTime > originalTimeMs) {
            getLogger().error("Timestamp error");              
        }
        lastTime = originalTimeMs;
        originalCalendar.setTimeInMillis((long)originalTimeMs);
        String videoFilename = new String();
            
        switch (cycleInterval) {
            case CYCLE_SECOND:
                videoFilename = String.format("%s_%4d_%02d_%02d_%02d_%02d_%02d", 
                        originalDeviceID, originalCalendar.get(Calendar.YEAR), originalCalendar.get(Calendar.MONTH) + 1,
                        originalCalendar.get(Calendar.DAY_OF_MONTH), originalCalendar.get(Calendar.HOUR_OF_DAY),
                        originalCalendar.get(Calendar.MINUTE), originalCalendar.get(Calendar.SECOND));
                break;
                    
            case CYCLE_MINUTE:
                videoFilename = String.format("%s_%4d_%02d_%02d_%02d_%02d", 
                        originalDeviceID, originalCalendar.get(Calendar.YEAR), originalCalendar.get(Calendar.MONTH) + 1,
                        originalCalendar.get(Calendar.DAY_OF_MONTH), originalCalendar.get(Calendar.HOUR_OF_DAY),
                        originalCalendar.get(Calendar.MINUTE));
                break;
                    
            case CYCLE_HOUR:
                videoFilename = String.format("%s_%4d_%02d_%02d_%02d", 
                        originalDeviceID, originalCalendar.get(Calendar.YEAR), originalCalendar.get(Calendar.MONTH) + 1,
                        originalCalendar.get(Calendar.DAY_OF_MONTH), originalCalendar.get(Calendar.HOUR_OF_DAY));
                break;
                    
            case CYCLE_DAY:
                videoFilename = String.format("%s_%4d_%02d_%02d", 
                        originalDeviceID, originalCalendar.get(Calendar.YEAR), originalCalendar.get(Calendar.MONTH) + 1,
                        originalCalendar.get(Calendar.DAY_OF_MONTH));
                break;
        }
            
        // obj now just contains the metadata but no video data

        obj.put("filename", videoFilename);
        obj.put("length", video.length);
        final JSONObject metadata = obj;

        FlowFile metadataFlowfile = session.create();
        FlowFile videoFlowfile = session.create();

        metadataFlowfile = session.putAttribute(metadataFlowfile, CoreAttributes.FILENAME.key(), originalDeviceID);
        metadataFlowfile = session.append(metadataFlowfile, new OutputStreamCallback() {

            @Override
            public void process(final OutputStream out) throws IOException {
                 out.write(metadata.toString().getBytes());
            }
        });
        videoFlowfile = session.putAttribute(videoFlowfile, CoreAttributes.FILENAME.key(), videoFilename);
        videoFlowfile = session.append(videoFlowfile, new OutputStreamCallback() {
            
            @Override
            public void process(final OutputStream out) throws IOException {
                 out.write(video);
            }
        });
        session.transfer(metadataFlowfile, RELATIONSHIP_METADATA);
        session.transfer(videoFlowfile, RELATIONSHIP_VIDEO);
    }
}
