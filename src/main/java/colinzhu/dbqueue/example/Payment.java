package colinzhu.dbqueue.example;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Payment {
    private Long id;
    private String status;
    private String instance;
    private Long createTime;
}