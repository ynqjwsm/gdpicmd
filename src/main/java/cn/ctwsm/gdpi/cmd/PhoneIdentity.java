package cn.ctwsm.gdpi.cmd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@AllArgsConstructor
@EqualsAndHashCode(exclude = {"visitTime"})
public class PhoneIdentity {

    private String account;
    private String visitTime;
    private String imei;

}
