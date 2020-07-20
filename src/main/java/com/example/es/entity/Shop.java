package com.example.es.entity;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.common.geo.GeoPoint;
import org.springframework.data.elasticsearch.annotations.CompletionField;
import org.springframework.data.elasticsearch.annotations.GeoPointField;
import org.springframework.data.elasticsearch.core.completion.Completion;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Shop {
    private String shopName;
    private String address;
    private String[] tags;
    @CompletionField
    private Completion suggest;
    @GeoPointField
    private String location;
    public static final String SHOPNAME = "shopName";
    public static final String SUGGEST = "suggest";
    public static final String LOCATION = "location";

}
