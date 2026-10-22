select
    p.arxiv_id,
    p.title,
    p.primary_category,
    count(l.hn_id)                  as nb_hn_posts,
    coalesce(sum(l.hn_score), 0)    as total_hn_score,
    coalesce(sum(l.hn_comments_count), 0) as total_hn_comments
from {{ ref('stg_arxiv_papers') }} p
join {{ ref('stg_arxiv_hn_links') }} l using (arxiv_id)
group by 1, 2, 3
