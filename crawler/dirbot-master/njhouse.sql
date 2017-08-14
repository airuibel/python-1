CREATE TABLE `njhouse` (
  `program_name` varchar(200) DEFAULT NULL,
  `license_no` varchar(200) DEFAULT NULL,
  `area` varchar(200) DEFAULT NULL,
  `open_time` varchar(200) DEFAULT NULL,
  `program_type` varchar(200) DEFAULT NULL,
  `sale_phone_no` varchar(200) DEFAULT NULL,
  `program_addr` varchar(200) DEFAULT NULL,
  `etl_date` varchar(200) DEFAULT NULL,
  `turn` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `query_times` (
  `q_times` int DEFAULT NULL

) ENGINE=InnoDB DEFAULT CHARSET=utf8;