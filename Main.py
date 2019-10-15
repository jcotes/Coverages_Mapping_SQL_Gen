import csv

from utilities.CoveragesUtil import CoveragesUtil
from utilities.QueryBuilder import QueryBuilder

csv_filename = "dwellingfire.csv"
migrate_script_name = "CMS-11525-New_Cov_Maps.sql"
rollback_script_name = "CMS-11525-New_Cov_Maps_RB.sql"
audit_id = "CMS-11525"

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