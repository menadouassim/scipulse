select
    c.category,
    c.label,
    c.domain,
    c.subdomain,
    count(p.arxiv_id) as nb_papers
from {{ ref('category_labels') }} c
left join {{ ref('stg_arxiv_papers') }} p on p.primary_category = c.category
group by 1, 2, 3, 4
