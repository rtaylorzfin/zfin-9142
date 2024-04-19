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
        self.run_queries()
        self.combine_all_csvs_into_one_xlsx()
        time_passed = pd.Timestamp.now() - start_time
        nicely_formatted = self.nicely_formatted_time_interval(time_passed.total_seconds())
        print("Done after: " + nicely_formatted)

    def create_sqlite_from_csvs(self):
        pass
    def _create_sqlite_from_csvs(self):
        #drop sqlite db if exists
        if os.path.exists(SQLITE_DB_PATH):
            os.remove(SQLITE_DB_PATH)

        input_files = ["genbank0306.csv", "genbank0408.csv", "gene2abbr.csv"]
        options = csv_to_sqlite.CsvOptions(typing_style="quick", drop_tables=True)
        csv_to_sqlite.write_csv(input_files, SQLITE_DB_PATH, options)

    def initialize_query_descriptions(self):
        self.query_descriptions = [
            # {'name': 'gene_accession_pairs_lost', 'description': 'all the cases where a gene used to have an accession, but no longer does (regardless of attribution)', 'definition': self.gene_accession_pairs_lost_query()},
            # {'name': 'gene_accession_attribs_lost', 'description': 'all the cases where an attribution was lost, though the gene/genbank sequence association still exists with another attribution', 'definition': self.gene_accession_attribs_lost_query()},
            # {'name': 'gene_accession_attribs_kept', 'description': 'all the gene/genbank links that were preserved between runs.', 'definition': self.gene_accession_attribs_kept_query()},
            # {'name': 'old_gene_acc_attrib_vs_new', 'description': 'all the cases where an attribution was lost, and the gene/genbank sequence association was preserved with a different attribution', 'definition': self.compare_old_gene_acc_attrib_to_new_attrib_query()},
            {'name': 'attributions_to_fix', 'description': 'fix these attributions by reverting them from ZDB-PUB-230516-87 to ZDB-PUB-130725-2 (as they used to be)', 'definition': self.attributions_to_fix_query()},
            {'name': 'attributions_replaced_counts', 'description': 'counts of how many instances of attribution 1 was swapped for attribution 2', 'definition': self.attributions_replaced_counts_query()}
        ]

    def run_queries(self):
        for q in self.query_descriptions:
            self.run_query(q['name'], q['definition'])

    def run_query(self, report, query):
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

    def compare_old_gene_acc_attrib_to_new_attrib_query(self):
        query = """
                select gaal.gene, gaal.acc, gaal.pub as oldpub, gaal.acc_type, gaal.abbr, group_concat(recattrib_source_zdb_id, ',') as newpubs
                from gene_accession_attribs_lost gaal left join genbank0408 
                on gene=dblink_linked_recid and acc=dblink_acc_num and recattrib_source_zdb_id <> pub
                group by gaal.gene, gaal.acc, gaal.pub, gaal.acc_type, gaal.abbr 
        """
        return query

    def attributions_to_fix_query(self):
        query = """
        select gene, abbr, acc, acc_type, newpubs as from_pub, oldpub as to_pub from old_gene_acc_attrib_vs_new where (oldpub, newpubs) in (
            ('ZDB-PUB-130725-2', 'ZDB-PUB-230516-87'),
            ('ZDB-PUB-130725-2', 'ZDB-PUB-020723-3'),
            ('ZDB-PUB-130725-2', 'ZDB-PUB-030703-1'),
            ('ZDB-PUB-130725-2', 'ZDB-PUB-030905-2'),
            ('ZDB-PUB-020723-3', 'ZDB-PUB-020723-5')
        ) order by oldpub, newpubs, gene, acc
        """
        return query

    def attributions_replaced_counts_query(self):
        query = """
        select oldpub, newpubs, count(*) from old_gene_acc_attrib_vs_new where newpubs <> 'ZDB-PUB-230516-87' or oldpub <> 'ZDB-PUB-130725-2' group by oldpub, newpubs
        """
        return query

    def query_as_df(self, query, tablename):
        conn = sqlite3.connect("genbanks.db")

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
