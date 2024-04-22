import csv_to_sqlite
import sqlite3
import os
import pandas as pd

SQLITE_DB_PATH = "genbanks.db"
OUTPUT_EXCEL_PATH = "out/genbanks_accessions_change_report.xlsx"

class ReportRunner:
    report_files = []
    query_descriptions = []

    def main(self):
        print("Starting time: " + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
        start_time = pd.Timestamp.now()
        if not os.path.exists("out"):
            os.mkdir("out")
        self.create_sqlite_from_csvs()
        self.initialize_query_descriptions()
        self.build_intermediate_tables()
        self.run_queries()
        self.combine_all_csvs_into_one_xlsx()
        time_passed = pd.Timestamp.now() - start_time
        nicely_formatted = self.nicely_formatted_time_interval(time_passed.total_seconds())
        print("Done after: " + nicely_formatted)

    def _create_sqlite_from_csvs(self):
        pass
    def create_sqlite_from_csvs(self):
        #drop sqlite db if exists
        if os.path.exists(SQLITE_DB_PATH):
            os.remove(SQLITE_DB_PATH)

        input_files = ["genbank0306.csv", "genbank0408.csv", "gene2abbr.csv"]
        options = csv_to_sqlite.CsvOptions(typing_style="quick", drop_tables=True)
        csv_to_sqlite.write_csv(input_files, SQLITE_DB_PATH, options)

    def initialize_query_descriptions(self):
        self.query_descriptions = [
            {'name': 'old_vs_new', 'description': 'For every gene and accession pair, show all old attributions and all new attributions (3/6/24 vs 4/8/24).', 'definition': self.old_vs_new_query()},
            {'name': 'changed_counts', 'description': 'How many accessions had their attribution changed from oldpub to newpub.', 'definition': self.changed_attribs_count_query()},
            {'name': 'proposed_fixes', 'description': 'SQL queries that we can use to revert any changed attributions', 'definition': self.proposed_fixes_query()},
            {'name': 'gene_accession_pairs_lost', 'description': 'all the cases where a gene used to have an accession, but no longer does (regardless of attribution)', 'definition': self.gene_accession_pairs_lost_query()},
            {'name': 'gene_accession_attribs_lost', 'description': 'all the cases where an attribution was lost, though the gene/genbank sequence association still exists with another attribution', 'definition': self.gene_accession_attribs_lost_query()},
            {'name': 'gene_accession_attribs_kept', 'description': 'all the gene/genbank links that were preserved between runs.', 'definition': self.gene_accession_attribs_kept_query()}
        ]

    def build_intermediate_tables(self):
        # Create "old" table as a simplified version of genbank0306
        sql = """
        select dblink_linked_recid as gene, 
        dblink_acc_num as acc, 
        recattrib_source_zdb_id as pub, 
        dblink_linked_recid || ':' || dblink_acc_num as gene_acc_hash  
        from genbank0306;
        """
        self.query_as_df(sql,"old")

        # Create "new" table as a simplified version of genbank0408
        sql = """
        select dblink_linked_recid as gene,
        dblink_acc_num as acc,
        recattrib_source_zdb_id as pub,
        dblink_linked_recid || ':' || dblink_acc_num as gene_acc_hash
        from genbank0408;
        """
        self.query_as_df(sql,"new")

        # Create common attributions:
        sql = """
        select old.gene_acc_hash, old.pub
        from old
        inner join new on old.gene_acc_hash = new.gene_acc_hash and old.pub = new.pub
        """
        self.query_as_df(sql,"common_attribs")

        # Create lost attributions:
        sql = """
        select gene_acc_hash, group_concat(pub) as pub from
        (select old.gene_acc_hash, old.pub 
        from old
        left join new on old.gene_acc_hash = new.gene_acc_hash and old.pub = new.pub
        where new.pub is null)
        group by gene_acc_hash
        """
        self.query_as_df(sql,"lost_attribs")

        # Create gained attributions:
        sql = """
        select gene_acc_hash, group_concat(pub) as pub from
        (select new.gene_acc_hash, new.pub
        from new
        left join old on new.gene_acc_hash = old.gene_acc_hash and new.pub = old.pub
        where old.pub is null)
        group by gene_acc_hash
        """
        self.query_as_df(sql,"gained_attribs")

        # All hashes old and new
        sql = """
        select distinct gene, acc, gene_acc_hash from (
        select gene, acc, gene_acc_hash from old
        union
        select gene, acc, gene_acc_hash from new )
        """
        self.query_as_df(sql, "all_hashes")

        # Combined final report:
        sql = """
        select * from (
        select all_hashes.gene, all_hashes.acc, old.pub as oldpub, new.pub as newpub
        from all_hashes
        left join lost_attribs as old on all_hashes.gene_acc_hash = old.gene_acc_hash 
        left join gained_attribs as new on all_hashes.gene_acc_hash = new.gene_acc_hash
        )
        where oldpub is not null or newpub is not null 
        order by gene, acc        
        """
        self.query_as_df(sql, "old_vs_new")


    def run_queries(self):
        for q in self.query_descriptions:
            self.run_query_to_csv(q['name'], q['definition'])

    def run_query_to_csv(self, report, query):
        print(f"Creating csv: out/{report}.csv")
        df = self.query_as_df(query, report)
        df.to_csv(f"out/{report}.csv", index=False)
        self.report_files.append(report)

    def combine_all_csvs_into_one_xlsx(self):
        print("Combining all csvs into one xlsx: " + OUTPUT_EXCEL_PATH)

        if os.path.exists(OUTPUT_EXCEL_PATH):
            os.remove(OUTPUT_EXCEL_PATH)

        #First create the xlsx file
        with pd.ExcelWriter(OUTPUT_EXCEL_PATH) as writer:
            #add worksheet called descriptions for describing the other sheets
            descriptions = [];
            for q in self.query_descriptions:
                descriptions.append((q['name'], q['description']))
            descriptions.append(('',''))
            descriptions.append(('report source: https://github.com/rtaylorzfin/zfin-9142', ''))

            df = pd.DataFrame(descriptions, columns=['sheet name', 'description'])
            df.to_excel(writer, sheet_name='descriptions', index=False)
            self.auto_adjust_column_widths(df, 'descriptions', writer)


        #append all reports
        for report in self.report_files:
            with pd.ExcelWriter(OUTPUT_EXCEL_PATH, mode='a') as writer:
                df = pd.read_csv(f"out/{report}.csv")

                # Add hyperlink column if df['gene'] exists
                if 'gene' in df.columns:
                    df['link'] = df['gene'].apply(lambda x: f'=HYPERLINK("https://zfin.org/{x}#sequences", "link")')
                df.to_excel(writer, sheet_name=report, index=False)

                # Adjust columns to fit content
                self.auto_adjust_column_widths(df, report, writer)

    def gene_accession_pairs_lost_query(self):
        query = """
                select distinct dblink_linked_recid as gene, dblink_acc_num as acc, recattrib_source_zdb_id as pub, acc_type, abbr from genbank0306
                left join gene2abbr on dblink_linked_recid = gene
                where (dblink_linked_recid, dblink_acc_num) not in
                (select distinct dblink_linked_recid, dblink_acc_num from genbank0408)
                order by dblink_acc_num, dblink_linked_recid
        """
        return query

    def gene_accession_attribs_lost_query(self):
        query = """
                select distinct dblink_linked_recid as gene, dblink_acc_num as acc, recattrib_source_zdb_id as pub, acc_type, abbr from genbank0306
                left join gene2abbr on dblink_linked_recid = gene
                where (dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id) not in
                (select distinct dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id from genbank0408)
                order by dblink_acc_num, dblink_linked_recid
        """
        return query

    def gene_accession_attribs_kept_query(self):
        query = """
                select distinct dblink_linked_recid as gene, dblink_acc_num as acc, recattrib_source_zdb_id as pub, acc_type, abbr from genbank0306
                left join gene2abbr on dblink_linked_recid = gene
                where recattrib_source_zdb_id in ('ZDB-PUB-020723-3', 'ZDB-PUB-130725-2')
                AND 
                (dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id) in
                (select distinct dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id from genbank0408)
                order by dblink_acc_num, dblink_linked_recid                
        """
        return query

    def old_vs_new_query(self):
        query = """
        select * from old_vs_new order by gene, acc        
        """
        return query

    def changed_attribs_count_query(self):
        sql = """
        select oldpub, newpub, count(*) from old_vs_new group by oldpub, newpub        
        """
        return sql

    def proposed_fixes_query(self):
        sql = """
        select 'update record_attribution set recattrib_source_zdb_id = ''' || oldpub || ''' where recattrib_source_zdb_id = ''' || newpub || '''' || 
        ' and exists (select 1 from db_link where dblink_zdb_id = recattrib_data_zdb_id ' || 
        ' and dblink_linked_recid = ''' || gene || ''' and dblink_acc_num = ''' || acc || ''' );' as fix, gene, acc, oldpub, newpub from old_vs_new where oldpub is not null and oldpub <> '' and newpub is not null and newpub not like '%,%'  ;        
        """
        return sql

    def query_as_df(self, query, tablename):
        conn = sqlite3.connect(SQLITE_DB_PATH)

        #create a table for analysis afterwards
        sql = f"create table if not exists {tablename} as {query}"
        conn.execute(sql)

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def auto_adjust_column_widths(self, df, report, writer):
        # Auto-adjust columns' width
        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer.sheets[report].column_dimensions[
                writer.sheets[report].cell(row=1, column=col_idx + 1).column_letter].width = column_length

    def nicely_formatted_time_interval(self, number_seconds):
        hours = int(number_seconds // 3600)
        minutes = int((number_seconds % 3600) // 60)
        seconds = int(number_seconds % 60)
        return f"{str(hours).zfill(2)}:{str(minutes).zfill(2)}:{str(seconds).zfill(2)}"


if __name__ == '__main__':
    ReportRunner().main()
