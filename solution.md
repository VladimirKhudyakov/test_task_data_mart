> [!NOTE]
> Все запросы выполнялись на PostgreSQL [ PostgreSQL 16.4 (Ubuntu 24.04) ].
> 
> Где используется путь к файлам на диске, вместо троеточия, необходимо указать настоящий путь к файлу/файлам, иначе, будет ошибка выполнения запроса. 


# ***Решение задания 1.***

**1. Создание таблицы 'test_data_contract':**

```sql
CREATE TABLE test_data_contract (
    customer_id VARCHAR(36) NOT NULL,
    contract_id VARCHAR(36) PRIMARY KEY,
    contract_code VARCHAR(11) NOT NULL,
    marker_delete BOOL NOT NULL,
    issue_dt DATE NOT NULL,
    subdivision_id VARCHAR(36) NOT NULL,
    renewal_contract_id VARCHAR(36)
);
```

**2. Загрузка данных из файла: test_data_contract.csv в таблицу test_data_contract:**

```sql
\copy test_data_contract FROM '.../data/test_data_contract.csv'
DELIMITER ','
CSV HEADER;
```

**3. Создание таблицы 'test_data_contract_conditions':**

```sql
CREATE TABLE test_data_contract_conditions (
    condition_id VARCHAR(36) PRIMARY KEY, 
    condition_dt TIMESTAMPTZ NOT NULL,
    contract_id VARCHAR(36) NOT NULL,
    conducted BOOL NOT NULL,
    marker_delete BOOL NOT NULL,
    operation_type VARCHAR(18) NOT NULL,
    condition_type VARCHAR(30) NOT NULL,
    condition_start_dt DATE NOT NULL,
    condition_end_dt DATE NOT NULL,
    days INT NOT NULL
);
```

**4. Загрузка данных из файла: test_data_contract_conditions.csv в таблицу test_data_contract_conditions:**

```sql
\copy test_data_contract_conditions FROM '.../data/test_data_contract_conditions.csv'
DELIMITER ','
CSV HEADER;
```

**5. Создание таблицы 'test_data_contract_conditions_payment_plan.csv':**

```sql
CREATE TABLE test_data_contract_conditions_payment_plan (
    condition_id VARCHAR(36),
    payment_dt DATE NOT NULL,
    loan_amount FLOAT8 NOT NULL,
    PRIMARY KEY(condition_id, payment_dt)
);
```

**6. Загрузка данных из файла: test_data_contract_conditions_payment_plan.csv в таблицу test_data_contract_conditions_payment_plan:**

```sql
\copy test_data_contract_conditions_payment_plan FROM '.../data/test_data_contract_conditions_payment_plan.csv'
DELIMITER ','
CSV HEADER;
```

**7. Создание таблицы 'test_data_contract_status.csv':**

```sql
CREATE TABLE test_data_contract_status (
    contract_id VARCHAR(36),
    status_dt TIMESTAMPTZ NOT NULL,
    status_type VARCHAR(30) NOT NULL
);
```

**8. Загрузка данных из файла: test_data_contract_status.csv в таблицу test_data_contract_status:**

```sql
\copy test_data_contract_status FROM '.../data/test_data_contract_status.csv'
DELIMITER ','
CSV HEADER;
```

**9. Создание таблицы 'v_contracts':**

```sql
CREATE TABLE v_contracts (
    contract_id VARCHAR(36) PRIMARY KEY,
    contract_code VARCHAR(11) NOT NULL,
    customer_id VARCHAR(36) NOT NULL,
    condition_id VARCHAR(36) NOT NULL,
    subdivision_id VARCHAR(36) NOT NULL,
    contract_serial_number INT NOT NULL,
    contract_renewal_serial_number INT NOT NULL,
    is_renewal BOOL NOT NULL,
    is_installment BOOL NOT NULL,
    prolong_count int,
    first_issue_dt DATE NOT NULL, 
    issue_dt DATE NOT NULL,
    plan_dt DATE NOT NULL,
    last_plan_dt DATE NOT NULL,
    close_dt DATE,
    loan_amount FLOAT8 NOT NULL,
    total_loan_amount FLOAT8 NOT NULL,
    min_loan_amount FLOAT8 NOT NULL,
    max_loan_amount FLOAT8 NOT NULL,
    loan_term INT NOT NULL,
    min_loan_term INT NOT NULL,
    max_loan_term INT NOT NULL,
    is_closed BOOL NOT NULL,
    usage_days INT,
    dev_days INT,
    delay_days INT,
    has_next BOOL NOT NULL
);
```

**10. Создание вспомогательной промежуточной таблицы:**

```sql
CREATE TABLE v_contracts_temp (
    contract_id VARCHAR(36) PRIMARY KEY, 
    contract_code VARCHAR(11) NOT NULL,
    customer_id VARCHAR(36) NOT NULL, 
    condition_id VARCHAR(36) NOT NULL,
    subdivision_id VARCHAR(36) NOT NULL, 
    issue_dt DATE NOT NULL,
    renewal_contract_id VARCHAR(36),
    condition_end_dt DATE NOT NULL,
    days INT NOT NULL
);
```

**11. Заполнение вспомогательной промежуточной временной таблицы:**

```sql
INSERT INTO v_contracts_temp 
SELECT dc.contract_id,
       dc.contract_code,
       dc.customer_id, 
       dcc.condition_id,
       dc.subdivision_id,
       dc.issue_dt,
       dc.renewal_contract_id,
       dcc.condition_end_dt,
       dcc.days
FROM test_data_contract AS dc
INNER JOIN test_data_contract_conditions AS dcc ON dc.contract_id = dcc.contract_id
WHERE dc.marker_delete = false AND
      dcc.marker_delete = false AND
      dcc.conducted = true AND
      dcc.operation_type = 'ЗаключениеДоговора';
```

**12. Создание временных таблиц (CTE).**

```sql
WITH contracts_serial_numbers_is_renewal_has_next AS (
    SELECT vct.contract_id,
           vct.contract_code,
           vct.customer_id,
           vct.condition_id,
           vct.subdivision_id,
           ROW_NUMBER() OVER(PARTITION BY vct.customer_id ORDER BY vct.issue_dt) AS contract_serial_number,
           CASE 
               WHEN vct.renewal_contract_id IS NOT NULL AND vct.renewal_contract_id <> '' THEN True
               ELSE False
           END AS is_renewal,
           FIRST_VALUE(vct.issue_dt) OVER(PARTITION BY vct.customer_id ORDER BY vct.issue_dt) AS first_issue_dt,
           issue_dt,
           vct.condition_end_dt AS plan_dt,
           vct.days AS loan_term,
           CASE 
               WHEN LEAD(vct.issue_dt) OVER(PARTITION BY vct.customer_id ORDER BY vct.issue_dt) IS NOT NULL
               THEN True
               ELSE False
           END AS has_next
    FROM v_contracts_temp AS vct),


contracts_serial_numbers_excluding_reissues AS (
    SELECT vct.customer_id,
           vct.contract_id,
           MAX(t.contract_is_not_renewal_serial_number) OVER(PARTITION BY vct.customer_id ORDER BY vct.issue_dt) AS contract_renewal_serial_number
    FROM v_contracts_temp AS vct
    LEFT JOIN (SELECT *,
                      ROW_NUMBER () OVER(PARTITION BY csnrhn.customer_id ORDER BY csnrhn.issue_dt) AS contract_is_not_renewal_serial_number
               FROM contracts_serial_numbers_is_renewal_has_next AS csnrhn
               WHERE csnrhn.is_renewal = False) AS t
    ON vct.customer_id = t.customer_id AND vct.contract_id = t.contract_id),
    

contracts_is_installment_loan_amount AS (
    SELECT DISTINCT ON (vct.contract_id) vct.contract_id,
       vct.customer_id,
       vct.issue_dt,
       CASE 
           WHEN COUNT(dccpp.payment_dt) OVER(PARTITION BY vct.contract_id) > 1 THEN True
           ELSE False
       END AS is_installment,
       SUM(dccpp.loan_amount) OVER(PARTITION BY vct.contract_id) AS loan_amount
FROM v_contracts_temp AS vct
INNER JOIN test_data_contract_conditions_payment_plan AS dccpp ON vct.condition_id = dccpp.condition_id),


contracts_prolong_count_and_last_condition_id AS (
    SELECT DISTINCT ON (vct.contract_id) vct.contract_id,
           COUNT(dcc.condition_type) OVER(PARTITION BY vct.contract_id) AS prolong_count,
           LAST_VALUE(dcc.condition_id) OVER(PARTITION BY vct.contract_id 
                                             ORDER BY dcc.condition_dt
                                             RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS last_condition_id
    FROM v_contracts_temp AS vct
    INNER JOIN test_data_contract_conditions AS dcc 
    ON vct.contract_id = dcc.contract_id 
    WHERE dcc.condition_type = 'Продление'),
    

contracts_with_extension_last_plan_dt AS (
    SELECT DISTINCT cpclc.last_condition_id,
           cpclc.contract_id,
           LAST_VALUE(dccpp.payment_dt) OVER(PARTITION BY cpclc.last_condition_id 
                                             ORDER BY dccpp.payment_dt
                                             RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS last_plan_dt
    FROM contracts_prolong_count_and_last_condition_id AS cpclc
    INNER JOIN test_data_contract_conditions_payment_plan AS dccpp 
    ON cpclc.last_condition_id = dccpp.condition_id),
    
    
contracts_last_plan_dt AS (
    SELECT DISTINCT ON (vct.contract_id) vct.contract_id,
           CASE
               WHEN vct.contract_id = celpd.contract_id
               THEN celpd.last_plan_dt
               ELSE vct.condition_end_dt
           END AS last_plan_dt
    FROM v_contracts_temp AS vct
    LEFT JOIN contracts_with_extension_last_plan_dt AS celpd 
    ON vct.contract_id = celpd.contract_id),
    
contracts_close_dt_is_closed_usage_days AS (
    SELECT DISTINCT ON (vct.contract_id) vct.contract_id,
           vct.customer_id,
           vct.issue_dt,
           CASE
               WHEN FIRST_VALUE(dcs.status_type) OVER(PARTITION BY vct.contract_id
                                                      ORDER BY dcs.status_dt DESC) IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')
               THEN dcs.status_dt::date
           END AS close_dt,
           CASE
               WHEN FIRST_VALUE(dcs.status_type) OVER(PARTITION BY vct.contract_id
                                                  ORDER BY dcs.status_dt DESC) IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')
               THEN True
               ELSE False
           END AS is_closed,
           CASE
               WHEN FIRST_VALUE(dcs.status_type) OVER(PARTITION BY vct.contract_id
                                                  ORDER BY dcs.status_dt DESC) IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')
               THEN dcs.status_dt::date - vct.issue_dt::date
           END AS usage_days
    FROM v_contracts_temp AS vct
    INNER JOIN test_data_contract_status AS dcs ON vct.contract_id = dcs.contract_id),
    
    
contracts_loans_amount AS (
    SELECT DISTINCT cila.customer_id,
                    cila.contract_id,
                    cila.issue_dt,
                    SUM(cila.loan_amount) OVER(PARTITION BY cila.customer_id ORDER BY cila.issue_dt) AS loans_amount
    FROM contracts_is_installment_loan_amount AS cila
    ORDER BY cila.customer_id, cila.issue_dt),
    
    
contracts_total_loan_amount AS (
    SELECT cla.customer_id,
           cla.contract_id,
           cla.issue_dt,
           LAG(cla.loans_amount, 1, 0) OVER(PARTITION BY cla.customer_id ORDER BY cla.issue_dt) AS total_loan_amount
    FROM contracts_loans_amount AS cla),
    
    
contracts_min_max_amount AS (
    SELECT DISTINCT cila.customer_id,
           cila.contract_id,
           cila.issue_dt,
           MIN(cila.loan_amount) OVER(PARTITION BY cila.customer_id ORDER BY cila.issue_dt) AS min_amount,
           MAX(cila.loan_amount) OVER(PARTITION BY cila.customer_id ORDER BY cila.issue_dt) AS max_amount
    FROM contracts_is_installment_loan_amount AS cila),
    
    
contracts_min_max_loan_amount AS (
    SELECT cmma.customer_id,
           cmma.contract_id,
           LAG(cmma.min_amount, 1, 0) OVER(PARTITION BY cmma.customer_id 
                                           ORDER BY cmma.issue_dt) AS min_loan_amount,
           LAG(cmma.max_amount, 1, 0) OVER(PARTITION BY cmma.customer_id 
                                           ORDER BY cmma.issue_dt) AS max_loan_amount
    FROM contracts_min_max_amount AS cmma),
    

contracts_min_max_term AS (
    SELECT DISTINCT vct.customer_id,
           vct.contract_id,
           vct.issue_dt,
           MIN(vct.days) OVER(PARTITION BY vct.customer_id ORDER BY vct.issue_dt) as min_term,
           MAX(vct.days) OVER(PARTITION BY vct.customer_id ORDER BY vct.issue_dt) as max_term
    FROM v_contracts_temp AS vct
    ORDER BY vct.customer_id, vct.issue_dt),
    

contracts_min_max_loan_term AS (
    SELECT cmmt.customer_id,
           cmmt.contract_id,
           cmmt.issue_dt,
           LAG(cmmt.min_term, 1, 0) OVER(PARTITION BY cmmt.customer_id ORDER BY cmmt.issue_dt) AS min_loan_term,
           LAG(cmmt.max_term, 1, 0) OVER(PARTITION BY cmmt.customer_id ORDER BY cmmt.issue_dt) AS max_loan_term
    FROM contracts_min_max_term AS cmmt),
    
    
contracts_dev_days AS (
    SELECT DISTINCT ON (ccdcud.contract_id) ccdcud.contract_id,
           CASE
               WHEN ccdcud.is_closed
               THEN ccdcud.close_dt::date - clpd.last_plan_dt::date
           END AS dev_days
    FROM contracts_close_dt_is_closed_usage_days AS ccdcud
    INNER JOIN contracts_last_plan_dt AS clpd
    ON ccdcud.contract_id = clpd.contract_id),
    

contracts_delay_days AS (
    SELECT ccdcud.customer_id,
           ccdcud.contract_id,
           ccdcud.issue_dt::date - (LAG(ccdcud.close_dt) OVER(PARTITION BY ccdcud.customer_id ORDER BY ccdcud.close_dt))::date AS delay_days
    FROM contracts_close_dt_is_closed_usage_days AS ccdcud)
```

**13. Заполнение основной таблицы v_contracts.**

```sql
INSERT INTO v_contracts
SELECT csnrn.contract_id,
       csnrn.contract_code,
       csnrn.customer_id,
       csnrn.condition_id,
       csnrn.subdivision_id,
       csnrn.contract_serial_number,
       csner.contract_renewal_serial_number,
       csnrn.is_renewal,
       cila.is_installment,
       cpclc.prolong_count,
       csnrn.first_issue_dt,
       csnrn.issue_dt,
       csnrn.plan_dt,
       clpd.last_plan_dt,
       ccdcud.close_dt,
       cila.loan_amount,
       ctla.total_loan_amount,
       cmmla.min_loan_amount,
       cmmla.max_loan_amount,
       csnrn.loan_term,
       cmmlt.min_loan_term,
       cmmlt.max_loan_term,
       ccdcud.is_closed,
       ccdcud.usage_days,
       cdd.dev_days,
       cdds.delay_days,
       csnrn.has_next
FROM contracts_serial_numbers_is_renewal_has_next AS csnrn
LEFT JOIN contracts_serial_numbers_excluding_reissues AS csner ON csnrn.contract_id = csner.contract_id
LEFT JOIN contracts_is_installment_loan_amount AS cila ON csnrn.contract_id = cila.contract_id
LEFT JOIN contracts_prolong_count_and_last_condition_id AS cpclc ON csnrn.contract_id = cpclc.contract_id
LEFT JOIN contracts_last_plan_dt AS clpd ON csnrn.contract_id = clpd.contract_id
LEFT JOIN contracts_close_dt_is_closed_usage_days AS ccdcud ON csnrn.contract_id = ccdcud.contract_id
LEFT JOIN contracts_total_loan_amount AS ctla ON csnrn.contract_id = ctla.contract_id
LEFT JOIN contracts_min_max_loan_amount AS cmmla ON csnrn.contract_id = cmmla.contract_id
LEFT JOIN contracts_min_max_loan_term AS cmmlt ON csnrn.contract_id = cmmlt.contract_id
LEFT JOIN contracts_dev_days AS cdd ON csnrn.contract_id = cdd.contract_id
LEFT JOIN contracts_delay_days AS cdds ON csnrn.contract_id = cdds.contract_id;
```

**14. Создание индексов для полей 'customer_id', 'condition_id', в таблице 'v_contracts'.**
contract_id - является первичным ключем, следовательно, PostgreSQL автоматически создает уникальный индекс (https://www-postgresql-org.translate.goog/docs/current/indexes-unique.html?_x_tr_sl=en&_x_tr_tl=ru&_x_tr_hl=ru&_x_tr_pto=rq).

```sql
CREATE INDEX customer_id_idx ON v_contracts (customer_id);
CREATE INDEX condition_id_idx ON v_contracts (condition_id);
```

**15. Cоздание комментариев.**

```sql
COMMENT ON TABLE v_contracts IS 'Итоговая таблица (витрина с данными по контрактам).';
COMMENT ON COLUMN v_contracts.contract_id IS 'ID контракта';
COMMENT ON COLUMN v_contracts.contract_code IS 'Код контракта';
COMMENT ON COLUMN v_contracts.customer_id IS 'ID клиента';
COMMENT ON COLUMN v_contracts.condition_id IS 'ID документа о заключении контракта';
COMMENT ON COLUMN v_contracts.subdivision_id IS 'ID подразделения, заключившего контракт';
COMMENT ON COLUMN v_contracts.contract_serial_number IS 'Порядковый номер контракта у клиента';
COMMENT ON COLUMN v_contracts.contract_renewal_serial_number IS 'Порядковый номер контракта у клиента без учёта переоформлений. Если контракт является переоформлением, порядковый номер не должен увеличиваться';
COMMENT ON COLUMN v_contracts.is_renewal IS 'Является ли данный контракт переоформлением (наличие ID в поле renewal_contract_id)';
COMMENT ON COLUMN v_contracts.is_installment IS 'Является ли данный контракт долгосрочным (наличие нескольких платежей в плане погашений)';
COMMENT ON COLUMN v_contracts.prolong_count IS 'Количество продлений. Контракт может быть продлён неограниченное количество раз (см. test_data_contract_conditions.condition_type)';
COMMENT ON COLUMN v_contracts.first_issue_dt IS 'Дата первого контракта у клиента';
COMMENT ON COLUMN v_contracts.issue_dt IS 'Дата выдачи займа';
COMMENT ON COLUMN v_contracts.plan_dt IS 'Дата планового погашения займа';
COMMENT ON COLUMN v_contracts.last_plan_dt IS 'Дата планового погашения займа с учётом продлений';
COMMENT ON COLUMN v_contracts.close_dt IS 'Дата фактического погашения займа (дата закрытия). Учитывается только последний статус по контракту';
COMMENT ON COLUMN v_contracts.loan_amount IS 'Сумма займа. Суммируются все платежи по графику';
COMMENT ON COLUMN v_contracts.total_loan_amount IS 'Сумма всех предыдущих займов';
COMMENT ON COLUMN v_contracts.min_loan_amount IS 'Минимальная сумма предыдущих займов';
COMMENT ON COLUMN v_contracts.max_loan_amount IS 'Максимальная сумма предыдущих займов';
COMMENT ON COLUMN v_contracts.loan_term IS 'Срок займа в днях';
COMMENT ON COLUMN v_contracts.min_loan_term IS 'Минимальный срок предыдущих займов';
COMMENT ON COLUMN v_contracts.max_loan_term IS 'Максимальный срок предыдущих займов';
COMMENT ON COLUMN v_contracts.is_closed IS 'Является ли контракт закрытым на текущий момент. Закрытым считаем контракт со следующими статусами: «Закрыт», «Договор закрыт с переплатой», «Переоформлен»';
COMMENT ON COLUMN v_contracts.usage_days IS 'Количество дней фактического использования займа (для закрытых). Разница между датой закрытия и открытия займа';
COMMENT ON COLUMN v_contracts.dev_days IS 'Разница между датой закрытия и последней датой планового погашения (для закрытых)';
COMMENT ON COLUMN v_contracts.delay_days IS 'Количество дней с даты закрытия предыдущего займа у клиента и датой открытия текущего';
COMMENT ON COLUMN v_contracts.has_next IS 'Признак наличия следующего контракта у клиента';
```

**16. Экспорт таблицы v_contracts в файл v_contracts.csv:**
   
```sql
\copy v_contracts TO '.../data/v_contracts.csv'
DELIMITER ','
CSV HEADER;
```       



# Решение задания 2.


**17. Построение запроса.**


```sql
\copy (
WITH clients_first_contract_2019 AS (
    SELECT vc.customer_id,
           EXTRACT(YEAR FROM vc.first_issue_dt) AS first_issue_year,
           EXTRACT(MONTH FROM vc.first_issue_dt) AS first_issue_month,
           vc.contract_serial_number,
           MAX(vc.contract_serial_number) OVER (PARTITION BY EXTRACT(YEAR FROM vc.first_issue_dt), EXTRACT(MONTH FROM vc.first_issue_dt) ORDER BY vc.contract_serial_number) AS current_max_contract_serial_number,
           MAX(vc.contract_serial_number) OVER (PARTITION BY EXTRACT(YEAR FROM vc.first_issue_dt), EXTRACT(MONTH FROM vc.first_issue_dt)) AS max_contract_serial_number,
           vc.loan_amount
    FROM v_contracts AS vc
    WHERE EXTRACT(YEAR FROM vc.first_issue_dt) = 2019)

 
SELECT cfc.customer_id,
       cfc.first_issue_year,
       cfc.first_issue_month,
       ROUND((cfc.loan_amount / clfa.loan_amount)::numeric, 3) AS loan_amount_to_loan_first_amount_ratio
FROM clients_first_contract_2019 AS cfc
INNER JOIN (SELECT DISTINCT ON (vc.customer_id, vc.contract_id) vc.customer_id, 
                   vc.contract_id,
                   vc.loan_amount
            FROM v_contracts AS vc
            WHERE vc.first_issue_dt = vc.issue_dt) AS clfa /* contracts_loan_first_amount */
ON cfc.customer_id = clfa.customer_id
WHERE cfc.current_max_contract_serial_number = cfc.max_contract_serial_number
ORDER BY cfc.first_issue_year, cfc.first_issue_month

)TO '.../data/solution_task_2.csv'
DELIMITER ','
CSV HEADER; 
```


