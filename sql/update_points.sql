update scores
set year = strftime('%Y', end_date)
where league_id = {league_id}
;

update scores as s1
set handicap = case when (select handicap
                          from leagues l
                          where l.id = s1.league_id
                          )
                          = 0
                     then 0
                     when (select count(1)
                           from scores s2
                           where s2.player = s1.player
                             and s2.league_id = s1.league_id
                             and s2.end_date < s1.end_date
                             and s2.year >= s1.year -1
                           	 and s2.handicap_excluded = 0
                           )
                           < 3
                     then 0
                     else (select  max(round((avg(x.raw_score) - 49) * 0.8 , 0) ,0)
                           from(select s3.raw_score,
                                row_number() over (order by end_date desc) rn
                           			from scores s3
                                where s3.player = s1.player
                                  and s3.league_id = s1.league_id
                                  and s3.end_date < s1.end_date
                                  and s3.year >= s1.year -1
                                  and s3.handicap_excluded = 0
                                ) x
                           where x.rn <=3
                           )
                      end
where s1."year" = strftime('%Y', datetime('now'))
  and league_id = {league_id}
;
  
                           
update scores as s1
set adjusted_score = raw_score - handicap
where s1.year = strftime('%Y', datetime('now'))
  and league_id = {league_id}

;
                           
update scores as s1
set place = (select count(1) + 1
             from scores s2
             where s2.event = s1.event
               and s2.league_id = s1.league_id
             	 and s2.adjusted_score < s1.adjusted_score
               and s2.division = s1.division
             )
   	,points = (select count(1)
             from scores s2
             where s2.event = s1.event
               and s2.league_id = s1.league_id
             	 and s2.adjusted_score >= s1.adjusted_score
               and s2.division = s1.division
             )
where s1.year = strftime('%Y', datetime('now'))     
  and league_id = {league_id}
       
;



update scores as s1
set next_handicap = case when (select handicap
                          from leagues l
                          where l.id = s1.league_id
                          )
                          = 0
                     then 0
                     when (select count(1)
                           from scores s2
                           where s2.player = s1.player
                             and s2.league_id = s1.league_id
                             and s2.end_date <= s1.end_date
                             and s2.year >= s1.year -1
                             and s2.handicap_excluded = 0
                           )
                           < 3
                     then 0
                     else (select  max(round((avg(x.raw_score) - 49) * 0.8 , 0) ,0)
                           from(select s3.raw_score,
                                row_number() over (order by end_date desc) rn
                           			from scores s3
                                where s3.player = s1.player
                                  and s3.league_id = s1.league_id
                                  and s3.end_date <= s1.end_date
                                  and s3.year >= s1.year -1
                                  and s3.handicap_excluded = 0
                                ) x
                           where x.rn <=3
                           )
                      end
where s1.year = strftime('%Y', datetime('now'))                    
  and league_id = {league_id}

;


update scores as s1
set season_points = (select sum(points)
                     from scores s2
                     where s2.player = s1.player
                       and s2.league_id = s1.league_id
                       and s2.end_date <= s1.end_date
                       and s2.year >= s1.year
                     )
where s1."year" = strftime('%Y', datetime('now'))
  and league_id = {league_id}             
  
;


update scores as s1
set season_place = (select count(distinct player) + 1
                     from scores s2
                     where s2.event = s1.event
                       and s2.league_id = s1.league_id
                       and s2.season_points > s1.season_points
                       and s2.division = s1.division
                     )
where s1.year = strftime('%Y', datetime('now'))     
  and league_id = {league_id}
;
  
  