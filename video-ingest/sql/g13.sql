select  cast(APPROX_COUNT_DISTINCT(account_id) filter(WHERE consent_analytics='true')as decimal(10,2)) / cast(APPROX_COUNT_DISTINCT(account_id)as decimal(10,2))  AS percentage
from user_account 
