package cn.ctwsm.gdpi.cmd;

/**
 * IMEI是国际移动通讯设备识别号(International Mobile Equipment Identity)的缩写，用于GSM系统。
 * 由15位数字组成，前6位(TAC)是型号核准号码，代表手机类型。接着2位(FAC)是最后装配号，代表产地。后6位(SNR)是串号，代表生产顺序号。最后1位(SP)是检验码。
 * MEID是移动通讯设备识别号(Mobile Equipment IDentifier)的缩写，用于CDMA系统。
 * 由15位16进制数字组成，前8位是生产商编号，后6位是串号，最后1位是检验码。
 */
public class DeviceUtil {
    /**
     * 格式化MEID
     * 因为MEID格式不统一，长度有14位和16位的，所以，为了统一，将14位和16位的MEID，统一设置为15位的 设置格式：
     * 如果MEID长度为14位，那么直接得到第15位，如果MEID长度为16位，那么直接在根据后14位得到第15位
     * 如果MEID长度为其他长度，那么直接返回原值
     * @param meid
     * @return
     */
    public static String formatMeid(String meid) {
        int dxml = meid.length();
        if (dxml != 14 && dxml != 16) {
            return meid;
        }
        String meidRes = "";
        if (dxml == 14) {
            meidRes =  meid + getmeid15(meid);
        }
        if (dxml == 16) {
            meidRes = meid.substring(2) + getmeid15(meid.substring(2));
        }
        return meidRes;
    }

    /**
     * 格式化IMEI
     * 因为IMEI格式不统一，长度有14位和16位的，所以，为了统一，将14位和16位的MEID，统一设置为15位的 设置格式：
     * 如果IMEI长度为14位，那么直接得到第15位，如果MEID长度为16位，那么直接在根据前14位得到第15位
     * 如果IMEI长度为其他长度，那么直接返回原值
     * @param imei
     * @return
     */
    public static String formatImei(String imei) {
        int dxml = imei.length();
        if (dxml != 14 && dxml != 16) {
            return imei;
        }
        String imeiRes = "";
        if (dxml == 14) {
            imeiRes =  imei + getimei15(imei);
        }
        if (dxml == 16) {
            imeiRes =  imei + getimei15(imei.substring(0,14));
        }
        return imeiRes;
    }
    /**
     * 根据MEID的前14位，得到第15位的校验位
     * MEID校验码算法：
     * (1).将偶数位数字分别乘以2，分别计算个位数和十位数之和，注意是16进制数
     * (2).将奇数位数字相加，再加上上一步算得的值
     * (3).如果得出的数个位是0则校验位为0，否则为10(这里的10是16进制)减去个位数
     * 如：AF 01 23 45 0A BC DE 偶数位乘以2得到F*2=1E 1*2=02 3*2=06 5*2=0A A*2=14 C*2=18 E*2=1C,
     * 	计算奇数位数字之和和偶数位个位十位之和，得到 A+(1+E)+0+2+2+6+4+A+0+(1+4)+B+(1+8)+D+(1+C)=64
     *  校验位 10-4 = C
     * @param meid
     * @return
     */
    private static String getmeid15(String meid) {
        if (meid.length() == 14) {
            String myStr[] = { "a", "b", "c", "d", "e", "f" };
            int sum = 0;
            for (int i = 0; i < meid.length(); i++) {
                String param = meid.substring(i, i + 1);
                for (int j = 0; j < myStr.length; j++) {
                    if (param.equalsIgnoreCase(myStr[j])) {
                        param = "1" + String.valueOf(j);
                    }
                }
                if (i % 2 == 0) {
                    sum = sum + Integer.parseInt(param);
                } else {
                    sum = sum + 2 * Integer.parseInt(param) % 16;
                    sum = sum + 2 * Integer.parseInt(param) / 16;
                }
            }
            if (sum % 16 == 0) {
                return "0";
            } else {
                int result = 16 - sum % 16;
                if (result > 9) {
                    result += 65 - 10;
                }
                return result + "";
            }
        } else {
            return "";
        }
    }
    /**
     * 根据IMEI的前14位，得到第15位的校验位
     * IMEI校验码算法：
     * (1).将偶数位数字分别乘以2，分别计算个位数和十位数之和
     * (2).将奇数位数字相加，再加上上一步算得的值
     * (3).如果得出的数个位是0则校验位为0，否则为10减去个位数
     * 如：35 89 01 80 69 72 41 偶数位乘以2得到5*2=10 9*2=18 1*2=02 0*2=00 9*2=18 2*2=04 1*2=02,计算奇数位数字之和和偶数位个位十位之和，
     * 得到 3+(1+0)+8+(1+8)+0+(0+2)+8+(0+0)+6+(1+8)+7+(0+4)+4+(0+2)=63
     * 校验位 10-3 = 7
     * @param imei
     * @return
     */
    private static String getimei15(String imei){
        if (imei.length() == 14) {
            char[] imeiChar=imei.toCharArray();
            int resultInt=0;
            for (int i = 0; i < imeiChar.length; i++) {
                int a=Integer.parseInt(String.valueOf(imeiChar[i]));
                i++;
                final int temp=Integer.parseInt(String.valueOf(imeiChar[i]))*2;
                final int b=temp<10?temp:temp-9;
                resultInt+=a+b;
            }
            resultInt%=10;
            resultInt=resultInt==0?0:10-resultInt;
            return resultInt + "";
        }else{
            return "";
        }
    }

    /**
     * 将15位IMEI传入，根据算法校验IMEI是否合规
     * @param imei
     * @return
     */
    public static boolean valiedImei(String imei){
        boolean flag = false;
        do{
            if (null == imei || imei.length() != 15) continue;
            String prefix = imei.substring(0, 14);
            String suffix = imei.substring(14, 15);
            flag = getimei15(prefix).equals(suffix);
        }while (false);
        return flag;
    }

}