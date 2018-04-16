package org.apache.kafka.connect.mixpanel;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Math.toIntExact;

/**
 * Implementation of the task interface for pulling event data from the Mixpanel API.
 * Created by Kostas.
 */
public class MixPanelTask extends SourceTask {

    private static final String TOPIC_NAME = "topic";
    private static final String API_KEY = "api_key";
    private static final String API_SECRET = "api_secret";
    private static final String FROM_DATE = "from_date";
    private static final String POLL_FREQUENCY = "poll_frequency";
    private static final String UPDATE_WINDOW = "update_window";
    private static final String ENDPOINT = "https://data.mixpanel.com/api/2.0/export/";
    private static final String SERVICE_FIELD = "service";
    private static final String POSITION_FIELD = "position";

    private String topic;
    private String api_key;
    private String api_secret;
    private String from_date;
    private String to_date;
    private int poll_frequency;
    private int update_window;
    private String latestDate;
    final AtomicBoolean done = new AtomicBoolean(false);

    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private BlockingQueue<String> msgs;
    private ExecutorService executor;

    public String version() {
        return new MixPanelConnector().version();
    }

    /**
     * Initialization of the task, takes care of the configuration plus creates the structured needed for communicating
     * with the client thread.
     */
    @Override
    public void start(Map<String, String> configMap) {
        topic = configMap.get(TOPIC_NAME);
        api_key = configMap.get(API_KEY);
        api_secret = configMap.get(API_SECRET);
        poll_frequency = Integer.parseInt(configMap.get(POLL_FREQUENCY));
        update_window = Integer.parseInt(configMap.get(UPDATE_WINDOW));
        to_date = DateUtils.getCurrentDate();
        latestDate = null;
        msgs = new LinkedBlockingQueue<>(1000);
        executor = Executors.newSingleThreadExecutor();
    }

    /**
     * Creates the call signature that is required for security reasons by the Mixpanel API. It should be moved to the client.
     */
    private  String calculateSig(int expire){
        ArrayList<String> vals = new ArrayList<>();

        vals.add("api_key=" + this.api_key);
        vals.add("from_date=" + this.from_date);
        vals.add("to_date=" + this.to_date);
        vals.add("expire=" + expire);
        Collections.sort(vals);

        String conc = "";
        for(String str : vals){
          conc += str;
        }

        conc += this.api_secret;
        return DigestUtils.md5Hex(conc);
    }
    /**
     * Polls the task for new data.
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // don't sleep on the first run
        if (latestDate != null) {
            TimeUnit.HOURS.sleep(poll_frequency);
        }

        this.to_date = DateUtils.getCurrentDate();
        this.from_date = DateUtils.subtractDays(this.to_date, update_window);
        latestDate = this.to_date;

        try {
            System.out.println("\n\n\nsending mixpanel request\n\n\n");
            System.out.println("Date range: " + this.from_date + " - " + this.to_date);

            final ArrayList<SourceRecord> records = new ArrayList<>();
            int s = toIntExact(new Date().getTime() / 1000 + 3600);
            String sig = calculateSig(s);

            MixPanelClient client = new MixPanelClient(msgs, done,ENDPOINT, api_key, from_date, to_date, sig, s);
            executor.submit(client);

            while(msgs.isEmpty()){
                TimeUnit.SECONDS.sleep(1);
            }
            while(!done.get() || !msgs.isEmpty()){
                String v = msgs.poll();
                if(v != null ){
                   records.add(new SourceRecord(offsetKey("mixpanel"), offsetValue(to_date), topic, VALUE_SCHEMA, v));
                }
            }
            latestDate = to_date;
            return records;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void stop() {
        executor.shutdown();

    }

    private String getStoredDate(){
        Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(SERVICE_FIELD, "mixpanel"));
        if( offset == null){
            return null;
        }else{
            Object off = offset.get(POSITION_FIELD);
            if(off == null || !(off instanceof String)){
                return null;
            }else{
               return (String) off;
            }
        }
    }

    private Map<String, String> offsetKey(String service) {
        return Collections.singletonMap(SERVICE_FIELD, service);
    }

    private Map<String, String> offsetValue(String pos) {return Collections.singletonMap(POSITION_FIELD, pos);}
}
