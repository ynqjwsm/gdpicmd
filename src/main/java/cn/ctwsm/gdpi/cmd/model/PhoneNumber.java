package cn.ctwsm.gdpi.cmd.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@AllArgsConstructor
@EqualsAndHashCode(exclude = {"visitTime", "phoneModel"})
public class PhoneNumber {

    private String account;

    private String visitTime;

    private String msisdn;

    private String phoneModel;

}
