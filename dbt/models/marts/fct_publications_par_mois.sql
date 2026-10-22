select
    date_trunc('month', date_published)::date as month,
    primary_category,
    count(*)                                   as nb_publications,
    round(avg(abstract_length))::int           as avg_abstract_length
from {{ ref('stg_arxiv_papers') }}
where date_published is not null
group by 1, 2
