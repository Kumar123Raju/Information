CREATE OR REPLACE PROCEDURE cs1.accseriesallocatestatusupdate(
    IN bankentityid CHARACTER VARYING,
    IN vaproductid CHARACTER VARYING,
    IN subaccountprefix CHARACTER VARYING,
    INOUT p_err_msg TEXT,
    INOUT p_succ_flag CHARACTER
)
LANGUAGE plpgsql
AS $$
DECLARE
    batchSize                     CONSTANT INT := 1000;
    seriesPrefixLength            INT;
    runningSeriesLength           INT;
    openRangeFromAccountId        VARCHAR;
    seriesToAccountId             VARCHAR;
    seriesNoOfVaAllowed           INT;
    lastFromAccountId             NUMERIC;
    accountId                     VARCHAR;
    seriesPrefix                  VARCHAR;
    vaAccNumberWithOrWithoutCheckDigit VARCHAR;
    checkDigitRequired            VARCHAR;
    checkDigitLogic               VARCHAR;
    lastFromAccountIdWithOrWithoutCheckDigit VARCHAR;
    subQuery                      VARCHAR;
    query1                        VARCHAR;
    query2                        VARCHAR;
    query3                        VARCHAR;
    query                         VARCHAR;
    vaIssueStatus                 CHARACTER VARYING(1) := '2';
    recordStatus                  CHARACTER VARYING(1) := 'O';
    v_add_comma                   CHARACTER VARYING(1) := ',';
    counter                       INT;
    newFromAccountId              VARCHAR;
    rec                           RECORD;
    all_success                   BOOLEAN := TRUE;
BEGIN
    -- Default flag
    p_succ_flag := '1';
    p_err_msg := NULL;

    FOR rec IN
        SELECT *
        FROM VA_SUBACC_MULTISERIES
        WHERE BANK_ENTITY_ID = bankentityid
          AND SUB_ACC_PREFIX_VAL = subaccountprefix
        ORDER BY SEQUENCE_NUMBER
    LOOP
        BEGIN
            -- Fetch series configuration
            SELECT SERIES_PREFIX, RUNNING_SERIES_LENGTH, CHECK_DIGIT_REQUIRED, CHECK_DIGIT_LOGIC
            INTO STRICT seriesPrefix, runningSeriesLength, checkDigitRequired, checkDigitLogic
            FROM VA_ACCOUNT_SERIES_MASTER
            WHERE BANK_ENTITY_ID = bankentityid AND SERIES_ID = rec.series_id AND STATUS = recordStatus;

            seriesPrefixLength := LENGTH(seriesPrefix);

            -- Lock and fetch open account range
            SELECT FROM_ACCOUNT_ID, TO_ACCOUNT_ID, NO_OF_VA_ALLOWED
            INTO STRICT openRangeFromAccountId, seriesToAccountId, seriesNoOfVaAllowed
            FROM VA_OPEN_ACCOUNT_RANGES
            WHERE BANK_ENTITY_ID = bankentityid
              AND SERIES_ID = rec.series_id
              AND NO_OF_VA_ALLOWED >= rec.VA_NUMBER_ALLOWED
            ORDER BY FROM_ACCOUNT_ID
            LIMIT 1
            FOR UPDATE;

            -- Initialize allocation
            lastFromAccountId := TO_NUMBER(substring(openRangeFromAccountId, seriesPrefixLength + 1), REPEAT('9', runningSeriesLength));
            counter := 0;
            subQuery := NULL;

            WHILE counter < rec.VA_NUMBER_ALLOWED LOOP
                counter := counter + 1;
                accountId := LPAD(lastFromAccountId::varchar, runningSeriesLength, '0');

                IF checkDigitRequired = 'Y' AND checkDigitLogic = '0' THEN
                    SELECT mod10_vaNumber_generate(accountId) INTO STRICT lastFromAccountIdWithOrWithoutCheckDigit;
                ELSIF checkDigitRequired = 'Y' AND checkDigitLogic = '1' THEN
                    SELECT custom_vaNumber_generate(accountId) INTO STRICT lastFromAccountIdWithOrWithoutCheckDigit;
                ELSE
                    lastFromAccountIdWithOrWithoutCheckDigit := accountId;
                END IF;

                vaAccNumberWithOrWithoutCheckDigit := COALESCE(seriesPrefix, '') || lastFromAccountIdWithOrWithoutCheckDigit;

                subQuery := COALESCE(subQuery, '') || '(' || quote_literal(bankentityid) || ',' || quote_literal(rec.series_id) || ',' || quote_literal(vaproductid) || ',' || quote_literal(subaccountprefix) || ',' || quote_literal(vaAccNumberWithOrWithoutCheckDigit) || ',' || quote_literal(vaIssueStatus) || ')';

                IF counter % batchSize = 0 OR counter = rec.VA_NUMBER_ALLOWED THEN
                    query1 := 'UPDATE VA_SERIES_INVENTORY_MASTER SET VA_ISSUE_STATUS = BATCH_INSERT_VALUES.VA_ISSUE_STATUS, VA_PRODUCT_ID = BATCH_INSERT_VALUES.VA_PRODUCT_ID, SUB_ACC_PREFIX_VAL = BATCH_INSERT_VALUES.SUB_ACC_PREFIX_VAL FROM (VALUES ';
                    query2 := subQuery;
                    query3 := ') AS BATCH_INSERT_VALUES(BANK_ENTITY_ID, SERIES_ID, VA_PRODUCT_ID, SUB_ACC_PREFIX_VAL, VA_ACC_NUMBER, VA_ISSUE_STATUS) WHERE VA_SERIES_INVENTORY_MASTER.BANK_ENTITY_ID = BATCH_INSERT_VALUES.BANK_ENTITY_ID AND VA_SERIES_INVENTORY_MASTER.SERIES_ID = BATCH_INSERT_VALUES.SERIES_ID AND VA_SERIES_INVENTORY_MASTER.VA_ACC_NUMBER = BATCH_INSERT_VALUES.VA_ACC_NUMBER';
                    query := query1 || query2 || query3;
                    EXECUTE query;
                    subQuery := NULL;
                ELSE
                    subQuery := subQuery || v_add_comma;
                END IF;

                lastFromAccountId := lastFromAccountId + 1;
            END LOOP;

            -- Final update to VA_OPEN_ACCOUNT_RANGES
            accountId := LPAD(lastFromAccountId::varchar, runningSeriesLength, '0');

            IF checkDigitRequired = 'Y' AND checkDigitLogic = '0' THEN
                SELECT mod10_vaNumber_generate(accountId) INTO STRICT lastFromAccountIdWithOrWithoutCheckDigit;
            ELSIF checkDigitRequired = 'Y' AND checkDigitLogic = '1' THEN
                SELECT custom_vaNumber_generate(accountId) INTO STRICT lastFromAccountIdWithOrWithoutCheckDigit;
            ELSE
                lastFromAccountIdWithOrWithoutCheckDigit := accountId;
            END IF;

            newFromAccountId := COALESCE(seriesPrefix, '') || lastFromAccountIdWithOrWithoutCheckDigit;

            UPDATE VA_OPEN_ACCOUNT_RANGES
            SET FROM_ACCOUNT_ID = newFromAccountId,
                NO_OF_VA_ALLOWED = NO_OF_VA_ALLOWED - rec.VA_NUMBER_ALLOWED
            WHERE BANK_ENTITY_ID = bankentityid
              AND SERIES_ID = rec.series_id
              AND FROM_ACCOUNT_ID = openRangeFromAccountId;

            -- Store post-process values
            UPDATE VA_SUBACC_MULTISERIES
            SET FROM_ACCOUNT_ID = openRangeFromAccountId,
                TO_ACCOUNT_ID = seriesToAccountId
            WHERE BANK_ENTITY_ID = bankentityid AND SERIES_ID = rec.series_id AND SUB_ACC_PREFIX_VAL = subaccountprefix;

        EXCEPTION WHEN OTHERS THEN
            all_success := FALSE;
            p_err_msg := 'Error processing series ' || rec.series_id || ': ' || SQLERRM;
            p_succ_flag := '0';
            RAISE NOTICE '%', p_err_msg;
            EXIT;
        END;
    END LOOP;

    IF all_success THEN
        -- Final update to VA_SUBACC_STRUCTURE only if all series succeeded
        UPDATE VA_SUBACC_STRUCTURE
        SET SERIES_ALLOCATION_STATUS = 'P'
        WHERE BANK_ENTITY_ID = bankentityid AND VA_PRODUCT_ID = vaproductid AND SUB_ACC_PREFIX_VAL = subaccountprefix;
        p_succ_flag := '1';
        p_err_msg := NULL;
        COMMIT;
    ELSE
        -- Rollback everything if any failure occurred
        ROLLBACK;
    END IF;
END;
$$;
