select
    arxiv_id,
    position,
    regexp_replace(trim(author_name), '\s+', ' ', 'g') as author_name
from {{ source('raw', 'raw_arxiv_authors') }}
where author_name is not null and trim(author_name) <> ''
