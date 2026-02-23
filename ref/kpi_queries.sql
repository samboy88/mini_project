
--#--Top 10 contributors of any events
SELECT user_id,
       user_login,
       count_of_contributions
FROM fact_team_contribution
ORDER BY count_of_contributions DESC
LIMIT 10;



--#--Top 10 contributors of PR
SELECT user_id,
       user_login,
       count_of_pull_requests_contributed_to
FROM fact_team_contribution
ORDER BY count_of_pull_requests_contributed_to DESC
LIMIT 10;

--#--Top 10 time consuming PRs
SELECT pull_request_id,title, source_author_id,user.user_login, time_to_merge_days
FROM fact_pull_request_performance fpp
LEFT JOIN dim_user user on user.source_user_id= fpp.source_author_id
order by time_to_merge_days desc
LIMIT 10;


--#--Top 10 time consuming PRs --Example https://github.com/pallets/flask/pull/1220
SELECT *
FROM fact_pull_request_performance fpp
LEFT JOIN dim_user user on user.source_user_id= fpp.source_author_id
order by time_to_merge_days desc
LIMIT 10;


--#--Avg Time taken to merge a PR
SELECT repo_name, AVG(time_to_merge_days)
FROM fact_pull_request_performance
group by 1
order by 2 desc;


--#--Avg Time taken to close a PR
SELECT repo_name, AVG(time_to_close_days)
FROM fact_pull_request_performance
group by 1
order by 2 desc;


--#--Avg lifetime of a PR
SELECT repo_name, AVG(pr_lifetime_days)
FROM fact_pull_request_performance
group by 1
order by 2 desc;