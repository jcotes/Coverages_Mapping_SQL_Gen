from objects.Coverage import Coverage


class ChildCoverage(Coverage):

    def __init__(self, clazz, line, desc, causes):
        super().__init__(clazz, line, desc)
        self.causes = causes
