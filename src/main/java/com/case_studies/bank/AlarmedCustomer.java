package com.case_studies.bank;

public class AlarmedCustomer {

    public final String id;
    public final String account;

    public AlarmedCustomer(String data) {
        String[] words = data.split(",");
        id = words[0];
        account = words[1];
    }

}
