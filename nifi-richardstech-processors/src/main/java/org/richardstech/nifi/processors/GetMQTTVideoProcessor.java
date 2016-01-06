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
import org.apache.nifi.components.PropertyValue;
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
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.OutputStream;
import java.io.IOException;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import org.json.JSONObject;
import java.util.Date;

@Tags({"GetMQTTVideoProcessor"})
@CapabilityDescription("Gets video messages from an MQTT broker")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetMQTTVideoProcessor extends AbstractProcessor implements MqttCallback {

    public static final String CYCLE_SECOND = "every second";
    public static final String CYCLE_MINUTE = "every minute";
    public static final String CYCLE_HOUR = "every hour";
    public static final String CYCLE_DAY = "every day";

    String topic;
    String broker;
    String clientID;
    
    MemoryPersistence persistence = new MemoryPersistence();
    MqttClient mqttClient;
    
    LinkedBlockingQueue<MQTTQueueMessage> mqttQueue = new LinkedBlockingQueue<MQTTQueueMessage>();

    public static final PropertyDescriptor PROPERTY_BROKER_ADDRESS = new PropertyDescriptor
            .Builder().name("Broker address")
            .description("MQTT broker address (tcp://<host>:<port>")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROPERTY_MQTT_TOPIC = new PropertyDescriptor
            .Builder().name("MQTT topic")
            .description("MQTT topic to subscribe to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROPERTY_MQTT_CLIENTID = new PropertyDescriptor
            .Builder().name("MQTT client ID")
            .description("MQTT client ID to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

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
    public void connectionLost(Throwable t) {
	getLogger().info("Connection to " + broker + " lost");
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
         mqttQueue.add(new MQTTQueueMessage(topic, message.getPayload()));
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROPERTY_BROKER_ADDRESS);
        descriptors.add(PROPERTY_MQTT_TOPIC);
        descriptors.add(PROPERTY_MQTT_CLIENTID);
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
        try {
            broker = context.getProperty(PROPERTY_BROKER_ADDRESS).getValue();
            topic = context.getProperty(PROPERTY_MQTT_TOPIC).getValue();
            clientID = context.getProperty(PROPERTY_MQTT_CLIENTID).getValue();
            mqttClient = new MqttClient(broker, clientID, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            mqttClient.setCallback(this);
            connOpts.setCleanSession(true);
            getLogger().info("Connecting to broker: " + broker);
            mqttClient.connect(connOpts);
            mqttClient.subscribe(topic, 0);
        } catch(MqttException me) {
            getLogger().error("msg "+me.getMessage());
        }
    }
 
    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        try {
            mqttClient.disconnect();
        } catch(MqttException me) {
            
        }
        getLogger().error("Disconnected");
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List messageList = new LinkedList();
        
        final String cycleInterval = context.getProperty(PROPERTY_CYCLE_INTERVAL).getValue();

        mqttQueue.drainTo(messageList);
        if (messageList.isEmpty())
            return;
        
        FlowFile metadataFlowfile = session.create();
        FlowFile videoFlowfile = session.create();
        Iterator iterator = messageList.iterator();
        while (iterator.hasNext()) {
            final MQTTQueueMessage m = (MQTTQueueMessage)iterator.next();
            JSONObject obj;
            String originalDeviceID;
            String originalTimestamp;
            try {
                obj = new JSONObject(new String(m.message, "UTF-8"));
                originalDeviceID = obj.get("deviceID").toString();
                originalTimestamp = obj.get("timestamp").toString();
            } catch(Exception e) {
                getLogger().error("Failed to process message");
                continue;
            }
            byte[] bytes = new byte[0];
            HexBinaryAdapter adapter = new HexBinaryAdapter();
            try {
                bytes = adapter.unmarshal(obj.remove("video").toString());
            } catch(Exception e) {
                getLogger().error("Video was not in correct hex string format");
                continue;
            }
            final byte[] video = bytes;
            
            // obj now just contains the metadata but no video data
            
            final JSONObject metadata = obj;
             
            // work out the video filename
            
            Calendar originalCalendar = new GregorianCalendar();
            double originalTimeMs = Double.valueOf(originalTimestamp) * 1000.0;
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
       }

       session.transfer(metadataFlowfile, RELATIONSHIP_METADATA);
       session.transfer(videoFlowfile, RELATIONSHIP_VIDEO);
    }

}
