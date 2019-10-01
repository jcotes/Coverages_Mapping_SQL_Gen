class ParentCoverage:

    def __init__(self, pc_coverage_code, coverage_desc, child_coverages):
        self.pc_coverage_code = pc_coverage_code
        self.coverage_desc = coverage_desc.replace('&', '\' || CHR(38) || \'')
        self.child_coverages = child_coverages
