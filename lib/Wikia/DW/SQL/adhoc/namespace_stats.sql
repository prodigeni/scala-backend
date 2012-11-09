select count(page_id) as cnt
  from (select p1.page_id,
               p1.page_ns,
               p1.is_redirect,
               p1.rev_timestamp,
               p1.event_type
          from events p1
    inner join (
                select p2.page_id,
                       p2.wiki_id,
                       max(p2.rev_id) as max_rev
                  from events p2
                 where p2.wiki_id = 403068
                   and p2.rev_timestamp <= '2012-10-31 23:59:59'
                 GROUP BY p2.wiki_id,
                          p2.page_id
                HAVING (select count(page_id)
                          from events p3
                         where p3.wiki_id = p2.wiki_id
                           and p3.page_id = p2.page_id
                           and log_id > 0) = 0
                 ORDER BY p2.wiki_id desc,
                          p2.page_id desc,
                          p2.rev_id desc,
                          p2.event_type desc
               ) as c
            on c.page_id  = p1.page_id
           and p1.rev_id  = c.max_rev
           and p1.wiki_id = c.wiki_id
         where p1.wiki_id = 403068
        ) as d
  where d.page_ns = '14'
    and d.is_redirect = 'N'
