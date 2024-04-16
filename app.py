import csv_to_sqlite
import sqlite3
import os
import pandas as pd

def main():  # put application's code here
    if not os.path.exists("out"):
        os.mkdir("out")
    create_sqlite_from_csvs()
    run_queries()
    combine_all_csvs_into_one_xlsx()
    print("Done")


def create_sqlite_from_csvs():
    #drop sqlite db if exists
    if os.path.exists("output.sqlite"):
        os.remove("output.sqlite")

    input_files = ["genbank0306.csv", "genbank0408.csv"]
    options = csv_to_sqlite.CsvOptions(typing_style="quick", drop_tables=True)
    csv_to_sqlite.write_csv(input_files, "output.sqlite", options)

def run_queries():
    rows = execute_query(gene_accession_pairs_lost_query())
    print("Creating csv of all gene accession pairs lost: out/gene_accession_pairs_lost.csv")

    df = pd.DataFrame(rows, columns=["gene", "accession", "pub", "acc_type"])
    df.to_csv("out/gene_accession_pairs_lost.csv", index=False)

    rows = execute_query(gene_accession_attribs_lost_query())
    print("Creating csv of all gene accession pairs with attribs lost: out/gene_accession_attribs_lost.csv")

    df = pd.DataFrame(rows, columns=["gene", "accession", "pub", "acc_type"])
    df.to_csv("out/gene_accession_attribs_lost.csv", index=False)

def combine_all_csvs_into_one_xlsx():
    print("Combining all csvs into one xlsx: out/all.csv")
    df = pd.read_csv("out/gene_accession_pairs_lost.csv")
    df.to_excel("out/all.xlsx", index=False)

    with pd.ExcelWriter("out/all.xlsx", mode="a") as writer:
        df = pd.read_csv("out/gene_accession_attribs_lost.csv")
        df.to_excel(writer, sheet_name="attribs_lost", index=False)

def gene_accession_pairs_lost_query():
    query = """
            select distinct dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id, acc_type from genbank0306
            where (dblink_linked_recid, dblink_acc_num) not in
            (select distinct dblink_linked_recid, dblink_acc_num from genbank0408);
    """
    return query

def gene_accession_attribs_lost_query():
    query = """
            select distinct dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id, acc_type from genbank0306
            where (dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id) not in
            (select distinct dblink_linked_recid, dblink_acc_num, recattrib_source_zdb_id from genbank0408);
    """
    return query

def execute_query(query):
    conn = sqlite3.connect("output.sqlite")
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    return rows

if __name__ == '__main__':
    main()
