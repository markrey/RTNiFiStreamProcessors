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

import java.io.IOException;
import java.net.URI;

import org.glassfish.tyrus.client.ClientManager;
import org.glassfish.tyrus.client.ClientProperties;

import javax.websocket.ClientEndpointConfig;
import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

public class WebSocketClient {
    private Session session;
    private final WebSocketProcessor wsProc;
    private ClientManager client;
    
    public WebSocketClient(WebSocketProcessor proc) {
        wsProc = proc;
    }

    public void connect(String url) {
        final ClientEndpointConfig configuration = ClientEndpointConfig.Builder.create().build();
        client = ClientManager.createClient();

        ClientManager.ReconnectHandler reconnectHandler = new ClientManager.ReconnectHandler() {
            
            @Override
            public boolean onDisconnect(CloseReason closeReason) {
                return true;
            }

            @Override
            public boolean onConnectFailure(Exception exception) {
                return true;
            }
                
            @Override
            public long getDelay() {
                return 2;
            }
        };
 
        try {
            client.getProperties().put(ClientProperties.RECONNECT_HANDLER, reconnectHandler);
            client.connectToServer(
                new Endpoint() {
                    @Override
                    public void onOpen(Session session,EndpointConfig config) {
                        WebSocketClient.this.session = session;
                        session.addMessageHandler(new MessageHandler.Whole<String>() {
                            @Override
                            public void onMessage(String message) {
                                wsProc.newWSMessage(message);
                            }
                        });
                    }
                }, configuration,new URI(url));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void close() {
        try {
            if (session != null)
                session.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            session = null;
            client.shutdown();
        }
    }

    public void sendMessage(String message) throws IOException, InterruptedException{
        session.getBasicRemote().sendText(message);
    }
}
