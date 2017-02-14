package com.zendesk.maxwell.producer;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;


public class MaxwellFirehoseProducer extends AbstractProducer {
    private static final Logger logger = LoggerFactory.getLogger(MaxwellFirehoseProducer.class);

    private final String firehoseStream;
    private final AmazonKinesisFirehoseClient firehoseClient;

    public MaxwellFirehoseProducer(MaxwellContext context, String firehoseStream){
        super(context);
        this.firehoseStream = firehoseStream;
        this.firehoseClient = new AmazonKinesisFirehoseClient();
    }

    @Override
    public void push(RowMap r) throws Exception {
        String value = r.toJSON();

        if(value == null){
            return;
        }

        Record record = new Record().withData(ByteBuffer.wrap(value.getBytes("UTF-8")));

        PutRecordRequest putRecordRequest = new PutRecordRequest()
                .withDeliveryStreamName(firehoseStream)
                .withRecord(record);

        // Put record into the DeliveryStream
        PutRecordResult putRecordResult = firehoseClient.putRecord(putRecordRequest);

        if(logger.isDebugEnabled()) {
            logger.debug("-> " + ", record id:" + putRecordResult.getRecordId());
            logger.debug("   " + value);
            logger.debug("   " + r.getPosition());
            logger.debug("");
        }

    }
}