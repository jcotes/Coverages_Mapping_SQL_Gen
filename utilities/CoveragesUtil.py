import csv

from objects.CauseOfLoss import CauseOfLoss
from objects.ChildCoverage import ChildCoverage
from objects.ParentCoverage import ParentCoverage


class CoveragesUtil:

    @staticmethod
    def get_coverages_datastruct(filename):
        csv_file = open(filename)
        return CoveragesUtil().load_coverages_datastruct(csv_file)

    @staticmethod
    def load_coverages_datastruct(filename):
        file_content = open(filename)
        csv_reader = csv.DictReader(file_content)
        coverages_object = {}
        causes = []

        for csv_row in csv_reader:

            pc_coverage_code = csv_row["PC_COVERAGE_CODE"]
            parent_coverage_desc = csv_row["OLD_COVERAGE_NAME"].replace('&', '\' || CHR(38) || \'')
            child_coverage_desc = csv_row["NEW_COVERAGE_NAME"].replace('&', '\' || CHR(38) || \'')
            cause = CauseOfLoss(csv_row["CAUSE"])
            child_coverage = ChildCoverage(child_coverage_desc, [cause])
            parent_coverage = ParentCoverage(pc_coverage_code, parent_coverage_desc, [child_coverage])

            # Add this cause to causes list if not exists
            if not any(c.cause_name == cause.cause_name for c in causes):
                causes.append(cause)

            # Add this pc_coverage_code to coverages_object if not exists.
            if pc_coverage_code not in coverages_object:
                coverages_object[pc_coverage_code] = [parent_coverage]
                continue

            # Add this parent_coverage to coverages_struct pc_coverage_code if not exists.
            if not any(pc.coverage_desc == parent_coverage_desc for pc in coverages_object[pc_coverage_code]):
                coverages_object[pc_coverage_code].append(parent_coverage)
                continue

            # Get list of child_coverages for parent_coverage in coverages_object
            # pc_coverage_code : parent_coverage.
            child_coverages = next(pc for pc in coverages_object[pc_coverage_code]
                                   if pc.coverage_desc == parent_coverage_desc).child_coverages

            # Add the child_coverage to the parent_coverage.child_coverages if not exists.
            if not any(cc.coverage_desc == child_coverage_desc for cc in child_coverages):
                next(pc for pc in coverages_object[pc_coverage_code]
                     if pc.coverage_desc == parent_coverage_desc).child_coverages.append(child_coverage)
                continue

            # Get list of causes for child_coverage in coverages_object
            # line_of_business : parent_coverage.child_coverages.
            child_coverage_causes = next(ccc for ccc in child_coverages if ccc.coverage_desc == child_coverage_desc).causes

            # Add the cause to the child_coverage.causes if not exists.
            if not any(ccc == cause for ccc in child_coverage_causes):
                child_coverage_causes.append(cause)

        file_content.close()
        return coverages_object, causes

    @staticmethod
    def write_sql_script_file(outfile_content, outfile_name):
        fout = open(outfile_name, 'w')
        fout.write(outfile_content)
        fout.close()
