from dagster import asset

from .utils import (
    execute_query_in_warehouse
)

@asset()
def aggregation_gists(**kwargs):

    dim_gists_query = """
     INSERT INTO aggregation.dim_gists
        SELECT distinct id, url, public, created_at, updated_at, description, comments
        FROM consolidate.consolidate_gists as cg
    ON CONFLICT (id) DO UPDATE
    SET url = EXCLUDED.url,
   		public = EXCLUDED.public,
   		updated_at = EXCLUDED.updated_at,
   		description = EXCLUDED.description,
   		comments = EXCLUDED.comments;
    """

    execute_query_in_warehouse(dim_gists_query)


@asset()
def aggregation_gist_commits(**kwargs):

    dim_gist_commits_query = """
     INSERT INTO aggregation.dim_gist_commits
        SELECT distinct version, id_gist, committed_at, url, "change_status.deletions", "change_status.additions", "change_status.total"
        FROM consolidate.consolidate_gist_commits as cgc
    ON CONFLICT (id_gist, version) DO UPDATE
    SET url = EXCLUDED.url,
   		"change_status.deletions" = EXCLUDED."change_status.deletions",
   		"change_status.additions" = EXCLUDED."change_status.additions",
   		"change_status.total" = EXCLUDED."change_status.total";
    """

    execute_query_in_warehouse(dim_gist_commits_query)


@asset()
def aggregation_gists_files(**kwargs):

    dim_gist_files_query = """
     INSERT INTO aggregation.dim_gist_files
        SELECT filename, id_gist, type, language, size
        FROM consolidate.consolidate_gist_files as cgf
    ON CONFLICT (id_gist, filename) DO UPDATE
    SET type = EXCLUDED.type,
   		language = EXCLUDED.language,
   		size = EXCLUDED.size;
    """

    execute_query_in_warehouse(dim_gist_files_query)


@asset(deps=[aggregation_gists, aggregation_gist_commits, aggregation_gists_files])
def aggregation_fact_gist_metrics(**kwargs):

    fact_gist_metrics_query = """
    insert into aggregation.fact_gist_metrics 
    select 
        id,
        public,
        description,
        created_at,
        updated_at,
        comments,
        cgc.nb_commit,
        l.languages
    from consolidate.consolidate_gists cg
    left join (
        select distinct cgf.id_gist, string_agg(language, ',') over (partition by cgf.id_gist) as languages
        from consolidate.consolidate_gist_files cgf
    ) as l on l.id_gist = cg.id
    left join (
        select id_gist, count(*) as nb_commit 
        from consolidate.consolidate_gist_commits
        group by 1
    ) as cgc on cg.id = cgc.id_gist
    ON CONFLICT (id) DO NOTHING;
    """

    execute_query_in_warehouse(fact_gist_metrics_query)
