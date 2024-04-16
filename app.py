import csv_to_sqlite
import sqlite3
import os
import pandas as pd

class ReportRunner:
    report_files = []
    def main(self):  # put application's code here
        if not os.path.exists("out"):
            os.mkdir("out")
        self.create_sqlite_from_csvs()
        self.run_queries()
        self.combine_all_csvs_into_one_xlsx()
        print("Done")

    def create_sqlite_from_csvs(self):
        #drop sqlite db if exists
        if os.path.exists("output.sqlite"):
            os.remove("output.sqlite")

        input_files = ["genbank0306.csv", "genbank0408.csv"]
        options = csv_to_sqlite.CsvOptions(typing_style="quick", drop_tables=True)
        csv_to_sqlite.write_csv(input_files, "output.sqlite", options)

    def run_queries(self):
        self.run_query('gene_accession_pairs_lost', self.gene_accession_pairs_lost_query())
        self.run_query('gene_accession_attribs_lost', self.gene_accession_attribs_lost_query())
        self.run_query('gene_accession_attribs_kept', self.gene_accession_attribs_kept_query())


    def run_query(self, report, query):
        print(f"Creating csv: out/{report}.csv")
        df = self.query_as_df(query)
        df.to_csv(f"out/{report}.csv", index=False)
        self.report_files.append(report)

    def combine_all_csvs_into_one_xlsx(self):
        print("Combining all csvs into one xlsx: out/all.xlsx")
        if os.path.exists("out/all.xlsx"):
            os.remove("out/all.xlsx")

        for report in self.report_files:
            with pd.ExcelWriter("out/all.xlsx", mode='a') as writer:
                df = pd.read_csv(f"out/{report}.csv")
                df.to_excel(writer, sheet_name=report, index=False)

    def gene_accession_pairs_lost_query(self):
        query = """
                select distinct dblink_linked_recid as gene, dblink_acc_num as acc, recattrib_source_zdb_id as pub, acc_type from genbank0306
                where (dblink_linked_recid, dblink_acc_num) not in
                (select distinct dblink_linked_recid, dblink_acc_num from genbank0408);
        """
        return query

    def gene_accession_attribs_lost_query(self):
        query = """
                select distinct dblink_linked_recid as gene, dblink_acc_num as acc, recattrib_source_zdb_id as pub, acc_type from genbank0306
                where (dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id) not in
                (select distinct dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id from genbank0408);
        """
        return query

    def gene_accession_attribs_kept_query(self):
        query = """
                select distinct dblink_linked_recid as gene, dblink_acc_num as acc, recattrib_source_zdb_id as pub, acc_type from genbank0306
                where (dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id) in
                (select distinct dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id from genbank0408);
        """
        return query

    def query_as_df(self, query):
        conn = sqlite3.connect("output.sqlite")
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

if __name__ == '__main__':
    ReportRunner().main()
