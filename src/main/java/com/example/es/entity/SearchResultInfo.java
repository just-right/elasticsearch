package com.example.es.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @className: SearchResultInfo
 * @description
 * @author: luffy
 * @date: 2020/7/21 19:20
 * @version:V1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class SearchResultInfo {
    private String shopID;
    private String shopName;
    private Double len;

}
