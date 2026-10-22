select
    a.author_name,
    p.primary_category,
    count(distinct a.arxiv_id) as nb_papers,
    min(p.date_published)      as first_publication,
    max(p.date_published)      as last_publication
from {{ ref('stg_arxiv_authors') }} a
join {{ ref('stg_arxiv_papers') }} p using (arxiv_id)
group by 1, 2
having count(distinct a.arxiv_id) >= 1
