select
    arxiv_id,
    hn_id,
    coalesce(hn_score, 0)          as hn_score,
    coalesce(hn_comments_count, 0) as hn_comments_count
from {{ source('raw', 'raw_arxiv_hn_links') }}
