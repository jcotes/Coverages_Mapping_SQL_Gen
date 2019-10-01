import csv

from utilities.CoveragesUtil import CoveragesUtil
from utilities.QueryBuilder import QueryBuilder

csv_filename = "CMS-11869.csv"
audit_id = "CMS-11869"
migrate_script_name = "{}_BOP_COV_REMAP.sql".format(audit_id)


def main():
    coverages_datastruct, causes = CoveragesUtil().load_coverages_datastruct(csv_filename)
    sql_txt_out = QueryBuilder().build_sql_script(coverages_datastruct, causes, audit_id)
    write_sql_script_file(sql_txt_out)


def write_sql_script_file(outfile_content):
    fout = open(migrate_script_name, 'w')
    fout.write(outfile_content)
    fout.close()


if __name__ == "__main__":
    main()
