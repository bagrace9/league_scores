
select player
      ,coalesce(
                first_value(next_handicap) over (partition by player,league_id order by start_date desc)
               , 0)  Handicap
from scores
where league_id = {league_id}
  and year >= strftime('%Y', 'now', '-1 year')
  and exists (select 1
              from scores s2
              where s.player = s2.player
              group by year
              having count (1) >=3)
group by player
order by player

