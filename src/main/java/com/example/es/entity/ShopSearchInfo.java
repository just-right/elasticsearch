package com.example.es.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ShopSearchInfo {
    private String shopName;
    private Double longitude;
    private Double latitude;
    private Double len;  //距离我几KM

}
