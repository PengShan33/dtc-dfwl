package com.dtc.analytic.online.common.modle;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created on 2020-02-15
 *
 * @author :hao.li
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlterStruct {
    //device_id
    private String device_id;
    //设备名称
    private String device_name;
    //策略名称
    private String strategy_name;
    //system_time告警时间
    private String system_time;
    //具体告警值
    private String value;
    //告警等级
    private String level;
    //资产唯一值，asset_id拼接
    private String unique_id;
    //阈值
    private String yuzhi;
    //ip-code
    private String gaojing;
}
