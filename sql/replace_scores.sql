delete from scores as s1
where exists (
  						select 1
              from impt_raw_scores irs
              where s1.event = irs.event
             );

insert into scores(league_id,
                       start_date,
                       end_date,
                       event,
                       division,
                       player,
                       raw_score,
                       points_multiplyer,
                       handicap_excluded)
select league_id,
			 start_date,
       end_date,
       event,
       division,
       player,
       score,
       points_multiplyer,
       handicap_excluded
from impt_raw_scores
                       


