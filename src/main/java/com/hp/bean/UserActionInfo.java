package com.hp.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class UserActionInfo {
    private int userId;
    private long itemId;
    private int category;
    private String behavior;
    private Long timestamp;
}
