package kafka.etl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class KafkaETLRequest {
    public static long DEFAULT_OFFSET = -1;
    public static String DELIM = "\t";
    
    String _topic;
    URI _uri;
    int _partition;
    long _offset = DEFAULT_OFFSET;
    
    public KafkaETLRequest() {
        
    }
    
    public KafkaETLRequest(String input) throws IOException {
        //System.out.println("Init request from " + input);
        String[] pieces = input.trim().split(DELIM);
        if (pieces.length != 4)
            throw new IOException( input + 
                                            " : input must be in the form 'url" + DELIM +
                                            "topic" + DELIM +"partition" + DELIM +"offset'");

        try {
            _uri = new URI (pieces[0]); 
        }catch (java.net.URISyntaxException e) {
            throw new IOException (e);
        }
        _topic = pieces[1];
        _partition = Integer.valueOf(pieces[2]);
        _offset = Long.valueOf(pieces[3]);
    }
    
    public KafkaETLRequest(String node, String topic, String partition, String offset, 
                                    Map<String, String> nodes) throws IOException {

        Integer nodeId = Integer.parseInt(node);
        String uri = nodes.get(nodeId.toString());
        if (uri == null) throw new IOException ("Cannot form node for id " + nodeId);
        
        try {
            _uri = new URI (uri); 
        }catch (java.net.URISyntaxException e) {
            throw new IOException (e);
        }
        _topic = topic;
        _partition = Integer.valueOf(partition);
        _offset = Long.valueOf(offset);
    }
    
    public KafkaETLRequest(String topic, String uri, int partition) throws URISyntaxException {
        _topic = topic;
        _uri = new URI(uri);
        _partition = partition;
    }
    
    public void setDefaultOffset() {
        _offset = DEFAULT_OFFSET;
    }
    
    public void setOffset(long offset) {
        _offset = offset;
    }
    
    public String getTopic() { return _topic;}
    public URI getURI () { return _uri;}
    public int getPartition() { return _partition;}
    
    public long getOffset() { return _offset;}

    public boolean isValidOffset() {
        return _offset >= 0;
    }
    
    @Override
    public boolean equals(Object o) {
        if (! (o instanceof KafkaETLRequest))
            return false;
        
        KafkaETLRequest r = (KafkaETLRequest) o;
        return this._topic.equals(r._topic) ||
                    this._uri.equals(r._uri) ||
                    this._partition == r._partition;
    }

    @Override
    public int hashCode() {
        return toString(0).hashCode();
    }

    @Override
    public String toString() {
        return toString(_offset);
    }
    

    public String toString (long offset) {
    
        return 
        _uri + DELIM +
        _topic + DELIM +
        _partition + DELIM +
       offset;
    }
    

}
