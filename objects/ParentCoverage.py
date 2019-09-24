from objects.Coverage import Coverage


class ParentCoverage(Coverage):

    def __init__(self, clazz, line, desc, child_coverages):
        super().__init__(clazz, line, desc)
        self.child_coverages = child_coverages
