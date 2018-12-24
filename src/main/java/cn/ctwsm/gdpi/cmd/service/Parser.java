package cn.ctwsm.gdpi.cmd.service;

import java.util.List;

public interface Parser<T> {

    List<T> getData();

    boolean parse(String sourceLine);

}
