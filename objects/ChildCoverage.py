class ChildCoverage:

    def __init__(self, coverage_desc, causes):
        self.coverage_desc = coverage_desc.replace('&', '\' || CHR(38) || \'')
        self.causes = causes

