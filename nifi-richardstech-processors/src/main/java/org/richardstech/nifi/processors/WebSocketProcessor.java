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
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
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
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.concurrent.LinkedBlockingQueue;

@Tags({"WebSocketProcessor"})
@TriggerWhenEmpty
@CapabilityDescription("Use WebSocket external service to process FlowFile")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class WebSocketProcessor extends AbstractProcessor {
    
    private static final long RESPONSE_TIMEOUT = 2000;
    
    private String serverURL;
    private WebSocketClient wsClient;
     
    private boolean waitingForResponse;
    
    private long responseTimer;

    private LinkedBlockingQueue<String> receivedMessages = new LinkedBlockingQueue<>();

    public static final PropertyDescriptor PROPERTY_SERVER_ADDRESS = new PropertyDescriptor
            .Builder().name("WebSocket service URL")
            .description("The URL for the WebSocket server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("FlowFile was processed by the external service")
            .build();

    public static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("FlowFile was not processed by the external service")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROPERTY_SERVER_ADDRESS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(RELATIONSHIP_SUCCESS);
        relationships.add(RELATIONSHIP_FAILURE);
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
            serverURL = context.getProperty(PROPERTY_SERVER_ADDRESS).getValue();
        } catch(Exception e) {
            getLogger().error("onScheduled failed " + e.getMessage());
        }
        
        wsClient = new WebSocketClient(this);
        wsClient.connect("ws://localhost:9000");
        waitingForResponse = false;
    }
 
    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        wsClient.close();
    }
    

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
 
        RTJSONFlowFile jsonFlowFile = new RTJSONFlowFile();
 
        processReceivedMessages(session);
        
        FlowFile flowfile = session.get();
        if (flowfile == null)
            return;
         
        if (!wsClient.isConnected()) {
            getLogger().error("FlowFile but no websocket connection"); 
            waitingForResponse = false;
            session.remove(flowfile);
            return;           
        }
        
        if (waitingForResponse) {
            // waiting for response - just discard flowfile
            session.remove(flowfile);
            return;
        }
        
        final JSONObject jsonMessage;
        
        try {
            jsonMessage = jsonFlowFile.read(session, flowfile);
        } catch(Exception e) {
            getLogger().error("Failed to read flowfile " + e.getMessage()); 
            return;
        } finally {
            session.remove(flowfile);
        }
        
        try {
            wsClient.sendMessage(jsonMessage.toString());
            waitingForResponse = true;
            responseTimer = System.currentTimeMillis();
        } catch (Exception e) {
            getLogger().error("Failed to send frame to service " + e.getMessage());            
        }
    }
    
    public void processReceivedMessages(final ProcessSession session) {
        final List messageList = new LinkedList();
        
        if (waitingForResponse && ((System.currentTimeMillis() - responseTimer) >= RESPONSE_TIMEOUT)) {
            waitingForResponse = false;
            getLogger().error("Timed out response from server");
        }
        
        receivedMessages.drainTo(messageList);
        if (messageList.isEmpty())
            return;
                
        Iterator iterator = messageList.iterator();
        while (iterator.hasNext()) {
            RTJSONFlowFile jsonFlowFile = new RTJSONFlowFile();
            FlowFile flowfile = session.create();
            final String m = (String)iterator.next();
            
            try {
                flowfile = jsonFlowFile.write(session, flowfile, m);
                session.transfer(flowfile, RELATIONSHIP_SUCCESS);
             } catch (Exception e) {
                session.transfer(flowfile, RELATIONSHIP_FAILURE);
                getLogger().error("Failed to forward FlowFile from server " + e.getMessage());  
            }
            session.commit();
            
            waitingForResponse = !jsonFlowFile.isFinal();
            if (!waitingForResponse)
                // discard any other queued messages if final flag has been seen
                break;
        }
    }
    
    public void newWSMessage(String message) {
        receivedMessages.add(message);
    }
}
