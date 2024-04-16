Notebook-like project for exploring ZFIN-9142
===

### Description:
We lost some data from NCBI load, then regained most of it due to a URL fix.  However, some of the GenBank RNA and DNA
sequences didn't come back. We are exploring that.

### Starting point:

The data was extracted from DB archives at 2 points:
- 2024-03-06 (the last run of NCBI before failures started happening)
- 2024-04-08 (after we regained most of the lost data)

#### Data restore

Example restore commands:
```
pg_restore -h db -U rtaylor -d arc -n public -t record_attribution /.../2024.03.06.1.bak
echo "alter table record_attribution rename to record_attribution_2024_03_06"| psql -h db arc
... similar commands for db_link and also for 2024.04.08
```

#### Data massaging

In order to filter to just the dblinks we may be concerned with, I ran the following sql to get a single table per timestamp that combines
db_link and record_attributions:

```
drop table if exists genbank_0306;
select 
	d.dblink_linked_recid, 
	d.dblink_acc_num, 
	d.dblink_info, 
	d.dblink_zdb_id, 
	d.dblink_length, 
	d.dblink_fdbcont_zdb_id,
	case when dblink_fdbcont_zdb_id = 'ZDB-FDBCONT-040412-37' then 'RNA' else 'DNA' end as acc_type,
	r.recattrib_source_zdb_id 
into genbank_0306
from db_link_2024_03_06 d
left join record_attribution_2024_03_06 r
on r.recattrib_data_zdb_id = d.dblink_zdb_id
where dblink_fdbcont_zdb_id in ('ZDB-FDBCONT-040412-37', 'ZDB-FDBCONT-040412-36') --genbank rna or dna
 and (dblink_linked_recid like 'ZDB-GENE%' or dblink_linked_recid like '%RNAG%');
 
... ditto for genbank_0408
```

#### Exporting to CSV
 
```
echo "\copy (select * from genbank_0306) to 'genbank0306.csv' with csv header" | psql -h db arc
echo "\copy (select * from genbank_0408) to 'genbank0408.csv' with csv header" | psql -h db arc
```

### Next steps (this repo)


#### Run app
Run the app.py command to perform analysis on these exports.
Some of the queries may take a long time to run (~4 minutes).

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

#### Check output
Check the `out` directory for generated reports.

Multiple csv files are generated with information about db links that were lost or not between runs:

- gene_accession_pairs_lost.csv : all the cases where a gene used to have an accession, but no longer does (regardless of attribution)
- gene_accession_attribs_lost.csv : all the cases where an attribution was lost, though the gene/genbank sequence association still exists with another attribution
- gene_accession_attribs_kept.csv : all the gene/genbank links that were preserved between runs.

out/all.xlsx contains all the csv files as xlsx sheets