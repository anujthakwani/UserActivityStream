
package com.demo.models;


import java.util.StringTokenizer;

public class UserActivity {

    public String userId;
    public String ts;



    public void parseString(String csvStr){
        StringTokenizer st = new StringTokenizer(csvStr,",");
        ts =st.nextToken();
        userId = st.nextToken();

    }


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }


    @Override
    public String toString() {
        return "{" +
                "userId='" + userId + '\'' +
                ", ts='" + ts + '\'' +
                '}';
    }
}
