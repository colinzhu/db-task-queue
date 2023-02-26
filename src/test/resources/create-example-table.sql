drop table payment;
CREATE TABLE PAYMENT (
    ID bigint,
    STATUS varchar(20),
    CREATE_TIME bigint
);
truncate table payment;
insert into PAYMENT (ID, STATUS, CREATE_TIME) values (1, 'CREATED', 1677337372696);
insert into PAYMENT (ID, STATUS, CREATE_TIME) values (2, 'CREATED', 1677337372697);
insert into PAYMENT (ID, STATUS, CREATE_TIME) values (3, 'CREATED', 1677337372698);
insert into PAYMENT (ID, STATUS, CREATE_TIME) values (4, 'CREATED', 1677337372699);
COMMIT;

SELECT * FROM PAYMENT where status = 'CREATED';


select count(*) from payment;
SELECT * FROM PAYMENT;