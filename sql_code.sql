SELECT DISTINCT
    DATE_TRUNC(
        DATE_ADD(s.date, INTERVAL es.sent_date DAY),
        MONTH
    ) AS sent_month,

    es.id_account AS id_account,

    COUNT(es.id_message) OVER (
        PARTITION BY
            DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH),
            es.id_account
    )
    /
    COUNT(es.id_message) OVER (
        PARTITION BY
            DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)
    ) * 100 AS sent_msg_percent_from_this_month,

    MIN(DATE_ADD(s.date, INTERVAL es.sent_date DAY)) OVER (
        PARTITION BY
            es.id_account,
            DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)
    ) AS first_sent_date,

    MAX(DATE_ADD(s.date, INTERVAL es.sent_date DAY)) OVER (
        PARTITION BY
            es.id_account,
            DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)
    ) AS last_sent_date

FROM `DA.email_sent` es

JOIN `DA.account` acc
    ON es.id_account = acc.id

JOIN `DA.account_session` acs
    ON acc.id = acs.account_id

JOIN `DA.session` s
    ON acs.ga_session_id = s.ga_session_id;
