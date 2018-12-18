package cn.ctwsm.gdpi.cmd.toolkit;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 工具类
 */
public class Tools {

    /**
     * Luhn算法, IMEI最后一位使用的校验算法
     * @param ccNumber
     * @return 通过:True 不通过:False
     */
    public static boolean isLuhnCheckPassed(String ccNumber) {
        int sum = 0;
        boolean alternate = false;
        for (int i = ccNumber.length() - 1; i >= 0; i--) {
            int n = Integer.parseInt(ccNumber.substring(i, i + 1));
            if (alternate) {
                n *= 2;
                if (n > 9) {
                    n = (n % 10) + 1;
                }
            }
            sum += n;
            alternate = !alternate;
        }
        return (sum % 10 == 0);
    }

    /**
     * 判断一个字符串是否IP的形式
     * @param source 输入字符串
     * @return 是:True 不是:False
     */
    public static boolean isIp(String source){
        if (StringUtils.isEmpty(source)) return false;
        char[] chars = source.trim().toCharArray();
        if (chars.length < 7 || chars.length > 15) return false;
        int dCount = 0, nCount = 0;
        for(int index = 0 ; index < chars.length; index++){
            if ('.' == chars[index] || dCount == 0){
                if( ++dCount > 3 ){
                    return false;
                }
                nCount = 0;
                continue;
            }else if( CharUtils.isAsciiNumeric(chars[index]) ){
                if (++nCount > 3){
                    return false;
                }
            }else {
                return false;
            }
        }
        return dCount == 3;
    }

}
