package com.ivy.transportclient;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class EsStartStop {
    public TransportClient getClient() {
        /////////////////////////// client start
        Settings settings = Settings.builder()
                // you have to set the cluster name if you use one different than "elasticsearch"
                .put("cluster.name", "ivy-es")
                // The Transport client comes with a cluster sniffing feature which allows it to dynamically add new hosts and remove old ones.
                .put("client.transport.sniff", true)
                .build();

        TransportClient client = null;
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.83"), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return client;
    }

    public void close(TransportClient client){
        client.close();
    }

}
