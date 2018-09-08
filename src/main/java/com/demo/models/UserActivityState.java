

package com.demo.models;

import com.demo.constants.KafkaConstants;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;


public class UserActivityState {

    public int visitNo=0;

    public String dateStart= KafkaConstants.DEFAULT_DATE;// this  variable is for creation sessionId.
    public String dateLast=KafkaConstants.DEFAULT_DATE; //this variable is for debugging purposes
    public String ts;
    public String userId;
   // Logger logger = Logger.getLogger(UserActivityState.class);

    private static SimpleDateFormat format =  new SimpleDateFormat(KafkaConstants.DATE_FORMAT);

    public UserActivityState add(UserActivity transaction,Logger logger) {

        try {
            this.ts = transaction.getTs();
            this.userId = transaction.getUserId();
            Date dStart = format.parse(dateStart);
            Date dLast = format.parse(dateLast);

            if (((format.parse(transaction.getTs()).getTime() - dStart.getTime()) / 1000 > 43200) || ((format.parse(transaction.getTs()).getTime() - dLast.getTime()) / 1000 > 1800)) {

                this.visitNo = 0;
                this.dateStart = transaction.getTs();
                this.dateLast = transaction.getTs();
            }

            this.visitNo = this.visitNo + 1;
            this.dateLast = transaction.getTs();
        }catch (Exception e){e.printStackTrace();}
       logger.info(String.format("returning  from UserActivityState with payload %s",this.toString()));
        return this;
    }

    @Override
    public String toString() {
        return "{" +
                "userId=" + userId +
                ", ts='" + ts + '\'' +
                ", sessionId=" + dateStart +
                ", visitNo=" + visitNo +
                '}';
    }
}
