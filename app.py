import csv_to_sqlite
import sqlite3
import os
import pandas as pd

SQLITE_DB_PATH = "genbanks.db"


class ReportRunner:
    report_files = []
    def main(self):
        print("Starting time: " + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
        start_time = pd.Timestamp.now()
        if not os.path.exists("out"):
            os.mkdir("out")
        self.create_sqlite_from_csvs()
        self.run_queries()
        self.combine_all_csvs_into_one_xlsx()
        time_passed = pd.Timestamp.now() - start_time
        nicely_formatted = self.nicely_formatted_time_interval(time_passed.total_seconds())
        print("Done after: " + nicely_formatted)

    def create_sqlite_from_csvs(self):
        #drop sqlite db if exists
        if os.path.exists(SQLITE_DB_PATH):
            os.remove(SQLITE_DB_PATH)

        input_files = ["genbank0306.csv", "genbank0408.csv", "gene2abbr.csv"]
        options = csv_to_sqlite.CsvOptions(typing_style="quick", drop_tables=True)
        csv_to_sqlite.write_csv(input_files, SQLITE_DB_PATH, options)

    def run_queries(self):
        self.run_query('gene_accession_pairs_lost', self.gene_accession_pairs_lost_query())
        self.run_query('gene_accession_attribs_lost', self.gene_accession_attribs_lost_query())
        self.run_query('gene_accession_attribs_kept', self.gene_accession_attribs_kept_query())


    def run_query(self, report, query):
        print(f"Creating csv: out/{report}.csv")
        df = self.query_as_df(query, report)
        df.to_csv(f"out/{report}.csv", index=False)
        self.report_files.append(report)

    def combine_all_csvs_into_one_xlsx(self):
        print("Combining all csvs into one xlsx: out/all.xlsx")

        if os.path.exists("out/all.xlsx"):
            os.remove("out/all.xlsx")

        #First create the xlsx file
        with pd.ExcelWriter("out/all.xlsx") as writer:
            #add worksheet called default
            pd.DataFrame().to_excel(writer, sheet_name='default', index=False)

        #append all reports
        for report in self.report_files:
            with pd.ExcelWriter("out/all.xlsx", mode='a') as writer:
                df = pd.read_csv(f"out/{report}.csv")

                # Add hyperlink column
                df['link'] = df['gene'].apply(lambda x: f'=HYPERLINK("https://zfin.org/{x}#sequences", "link")')

                df.to_excel(writer, sheet_name=report, index=False)

                # Auto-adjust columns' width
                for column in df:
                    column_length = max(df[column].astype(str).map(len).max(), len(column))
                    col_idx = df.columns.get_loc(column)
                    writer.sheets[report].column_dimensions[writer.sheets[report].cell(row=1, column=col_idx + 1).column_letter].width = column_length

        #remove 'default' sheet
        with pd.ExcelWriter("out/all.xlsx", mode='a') as writer:
            writer.book.remove(writer.book['default'])

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

    def query_as_df(self, query, tablename):
        conn = sqlite3.connect("genbanks.db")

        #create a table for analysis afterwards
        conn.execute(f"create table {tablename} as {query}")

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def nicely_formatted_time_interval(self, number_seconds):
        hours = int(number_seconds // 3600)
        minutes = int((number_seconds % 3600) // 60)
        seconds = int(number_seconds % 60)
        return f"{str(hours).zfill(2)}:{str(minutes).zfill(2)}:{str(seconds).zfill(2)}"


if __name__ == '__main__':
    ReportRunner().main()
