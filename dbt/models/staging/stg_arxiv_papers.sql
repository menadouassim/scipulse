select
    arxiv_id,
    trim(title)                                   as title,
    trim(abstract)                                as abstract,
    categories,
    coalesce(primary_category, split_part(categories, ' ', 1)) as primary_category,
    split_part(coalesce(primary_category, split_part(categories, ' ', 1)), '.', 1) as domain,
    date_published,
    date_updated,
    doi,
    length(abstract)                              as abstract_length
from {{ source('raw', 'raw_arxiv_papers') }}
where arxiv_id is not null
