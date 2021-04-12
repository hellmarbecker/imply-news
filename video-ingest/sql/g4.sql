SELECT G.gender, G.birthdate, APPROX_COUNT_DISTINCT(account_id)
FROM user_account G
GROUP BY G.gender, G.birthdate
ORDER BY gender, birthdate DESC
